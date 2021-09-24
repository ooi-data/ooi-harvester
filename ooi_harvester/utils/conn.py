import datetime
from loguru import logger
import os
import re

import zarr
import fsspec
import requests
import xarray as xr
import pandas as pd
from lxml import html
import progressbar

from ..config import BASE_URL, M2M_PATH
from ..utils.parser import (
    seconds_to_date,
    parse_global_range_dataframe,
    parse_param_dict,
)
from ooi_harvester.settings.main import harvest_settings

SESSION = requests.Session()
a = requests.adapters.HTTPAdapter(
    max_retries=1000, pool_connections=1000, pool_maxsize=1000
)
SESSION.mount("https://", a)


# OOI FOCUSED =====================================================
def check_zarr(dest_fold, storage_options={}):
    fsmap = fsspec.get_mapper(dest_fold, check=True, **storage_options)
    if fsmap.get('.zmetadata') is not None:
        zgroup = zarr.open_consolidated(fsmap)
        if 'time' not in zgroup:
            raise ValueError(
                f"Dimension time is missing from the dataset {dest_fold}!"
            )

        time_array = zgroup['time']
        calendar = time_array.attrs.get('calendar', 'gregorian')
        units = time_array.attrs.get('units', 'seconds since 1900-01-01 0:0:0')
        last_time = xr.coding.times.decode_cf_datetime(
            [time_array[-1]], units=units, calendar=calendar
        )
        return True, last_time[0]
    else:
        return False, None


def check_data_status(args: dict) -> bool:
    """Check if data is ready or not by looking for status.txt"""
    complete = False
    check_complete = args["status_url"]

    req = requests.get(check_complete)
    status_code = req.status_code
    if status_code == requests.codes.ok:
        text = f'Request ({args["thredds_url"]}) completed.'
        logger.info(text)  # noqa
        complete = True
    else:
        text = f'Your data ({args["thredds_url"]}) is still compiling... Please wait.'
        logger.info(text)  # noqa
    return complete


def get_toc():
    url = f"{BASE_URL}/{M2M_PATH}/12576/sensor/inv/toc"
    return send_request(url)


def get_vocab():
    url = f"{BASE_URL}/{M2M_PATH}/12586/vocab"
    return send_request(url)


def get_global_ranges():
    logger.info("Fetching global ranges ...")
    url = "https://raw.githubusercontent.com/ooi-integration/qc-lookup/master/data_qc_global_range_values.csv"  # noqa
    return parse_global_range_dataframe(pd.read_csv(url))


def get_stream(stream):
    url = f"{BASE_URL}/{M2M_PATH}/12575/stream/byname/{stream}"
    stream_dict = send_request(url)
    return {
        "stream_id": stream_dict["id"],
        "stream_rd": stream,
        "stream_type": stream_dict["stream_type"]["value"],
        "stream_content": stream_dict["stream_content"]["value"],
        "parameters": [parse_param_dict(p) for p in stream_dict["parameters"]],
        "last_updated": datetime.datetime.utcnow().isoformat(),
    }


def get_param_by_id(param_id):
    url = f"{BASE_URL}/{M2M_PATH}/12575/parameter/{param_id}"
    param_dict = send_request(url)
    return param_dict


def split_refdes(refdes):
    rd_list = refdes.split("-")
    return (rd_list[0], rd_list[1], "-".join(rd_list[2:]))


def retrieve_deployments(refdes):
    dep_port = 12587
    reflist = list(split_refdes(refdes))
    base_url_list = [
        BASE_URL,
        M2M_PATH,
        str(dep_port),
        "events/deployment/inv",
    ]
    dep_list = send_request("/".join(base_url_list + reflist))
    deployments = []
    if isinstance(dep_list, list):
        if len(dep_list) > 0:
            for d in dep_list:
                print("/".join(base_url_list + reflist + [str(d)]))
                dep = send_request(
                    "/".join(base_url_list + reflist + [str(d)])
                )
                if len(dep) > 0:
                    deployment = dep[0]
                    dep_dct = {
                        "reference_designator": deployment[
                            "referenceDesignator"
                        ],
                        "uid": deployment["sensor"]["uid"],
                        "description": deployment["sensor"]["description"],
                        "owner": deployment["sensor"]["owner"],
                        "manufacturer": deployment["sensor"]["manufacturer"],
                        "deployment_number": deployment["deploymentNumber"],
                        "lat": deployment["location"]["latitude"],
                        "lon": deployment["location"]["longitude"],
                        "deployment_start": None,
                        "deployment_end": None,
                    }
                    dep_start = deployment["eventStartTime"]
                    dep_end = deployment["eventStopTime"]

                    if dep_start:
                        dep_dct[
                            "deployment_start"
                        ] = datetime.datetime.fromtimestamp(
                            dep_start / 1000
                        ).isoformat()

                    if dep_end:
                        dep_dct[
                            "deployment_end"
                        ] = datetime.datetime.fromtimestamp(
                            dep_end / 1000
                        ).isoformat()

                    deployments.append(dep_dct)
    return deployments


def fetch_streams(inst):
    logger.debug(inst["reference_designator"])
    streams_list = []
    for stream in inst["streams"]:
        newst = stream.copy()
        newst.update(get_stream(stream["stream"]))
        streams_list.append(
            dict(
                reference_designator=inst["reference_designator"],
                platform_code=inst["platform_code"],
                mooring_code=inst["mooring_code"],
                instrument_code=inst["instrument_code"],
                **newst,
            )
        )
    return streams_list


def fetch_url(
    prepped_request, session=None, timeout=120, stream=False, **kwargs
):

    session = session or requests.Session()
    r = session.send(prepped_request, timeout=timeout, stream=stream, **kwargs)

    if r.status_code == 200:
        logger.debug(f"URL fetch {prepped_request.url} successful.")
        return r
    elif r.status_code == 500:
        message = "Server is currently down."
        if "ooinet.oceanobservatories.org/api" in prepped_request.url:
            message = "UFrame M2M is currently down."
        logger.warning(message)
        return r
    else:
        message = f"Request {prepped_request.url} failed: {r.status_code}, {r.reason}"
        logger.warning(message)  # noqa
        return r


def send_request(url, params=None, username=None, token=None):
    """Send request to OOI. Username and Token already included."""
    if username is None:
        # When not provided, grab username from settings
        username = harvest_settings.ooi_config.username

    if token is None:
        # When not provided, grab token from settings
        token = harvest_settings.ooi_config.token

    if username is None and token is None:
        raise ValueError("Please provide ooi username and token!")
    try:
        prepped_request = requests.Request(
            "GET", url, params=params, auth=(username, token)
        ).prepare()
        r = fetch_url(prepped_request, session=SESSION)
        if isinstance(r, requests.Response):
            return r.json()
    except Exception as e:
        logger.warning(e)


def request_data(
    platform,
    mooring,
    instrument,
    stream_method,
    stream,
    start_dt,
    end_dt,
    output_format="application/netcdf",
    limit=-1,
    exec_dpa=True,
    provenance=False,
    email=None,
    estimate=False,
):
    url = f"{BASE_URL}/{M2M_PATH}/12576/sensor/inv/{platform}/{mooring}/{instrument}/{stream_method}/{stream}"
    params = {
        "beginDT": start_dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "endDT": end_dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "format": output_format,
        "limit": limit,
        "execDPA": str(exec_dpa).lower(),
        "include_provenance": str(provenance).lower(),
        "estimate_only": str(estimate).lower(),
        "email": str(email),
    }
    return send_request(url, params), {"url": url, "params": params}


def get_download_urls(url):
    r = requests.get(url)
    tree = html.fromstring(r.content)
    # Only get netCDF
    nc_list = list(
        filter(
            lambda x: re.match(r"(.*?.nc$)", x.attrib["href"]) is not None,
            tree.xpath('//*[contains(@href, ".nc")]'),
        )
    )
    return ["/".join([url, nc.attrib["href"]]) for nc in nc_list]


def open_zarr_group(zarr_group, decode_times=False):
    try:
        return xr.open_zarr(
            store=zarr_group, consolidated=True, decode_times=decode_times
        )
    except Exception:
        return None


def url_download(url, data_fold, session=SESSION):
    """Method to help download files."""
    outpath = os.path.join(data_fold, os.path.basename(url))
    logger.info(f"{datetime.datetime.now().isoformat()}:    Downloading {url}")
    request = session.get(url, stream=True)  # `prefetch=False` for older
    # versions of requests
    bar = progressbar.ProgressBar(max_value=progressbar.UnknownLength)
    with open(outpath, "wb") as code:
        for i, chunk in enumerate(request.iter_content(100 * (1024 ** 2))):
            if chunk:
                code.write(chunk)
                bar.update(i)

    if os.path.exists(outpath):
        logger.info(
            f"{datetime.datetime.now().isoformat()}:    Downloaded {outpath}"
        )
    return outpath


def write_to_s3(outpath, fname, content, FS):
    s3out = os.path.join(outpath, fname)
    with open(fname, "w") as f:
        f.write(content)

    FS.put(fname, s3out)
    if FS.exists(s3out):
        os.unlink(fname)
