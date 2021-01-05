import os
import datetime
import math

import requests
from loguru import logger
from lxml import etree
from siphon.catalog import TDSCatalog

from ..config import STORAGE_OPTIONS, HARVEST_CACHE_BUCKET


def estimate_size_and_time(raw):
    m = ""
    if "requestUUID" in raw:
        est_size = raw["sizeCalculation"] / 1024 ** 2
        size_txt = "MB"
        if (est_size / 1024) >= 1.0:
            est_size = est_size / 1024
            size_txt = "GB"

        est_time = raw["timeCalculation"]
        time_txt = "Seconds"
        if (est_time / 60) >= 1.0 and (est_time / 60) < 60.0:
            est_time = math.floor(est_time / 60)
            time_txt = "Minutes"
            if est_time == 1:
                time_txt = "Minute"
        elif (est_time / 60) >= 60.0:
            est_time = math.floor(est_time / 60 ** 2)
            time_txt = "Hours"
            if est_time == 1:
                time_txt = "Hour"
        m = f"""
        Estimated File size: {est_size:.4} {size_txt}
        Estimated Time: {est_time} {time_txt}
        """
    elif "message" in raw:
        m = f"""
        No estimate calculated.
        {raw['message']['status']}
        """
    return m


def parse_uframe_response(resp):
    if "allURLs" in resp:
        return {
            "request_id": resp["requestUUID"],
            "thredds_catalog": resp["allURLs"][0],
            "download_catalog": resp["allURLs"][1],
            "status_url": resp["allURLs"][1] + "/status.txt",
            "data_size": resp["sizeCalculation"],
            "estimated_time": resp["timeCalculation"],
            "units": {
                "data_size": "bytes",
                "estimated_time": "seconds",
                "request_dt": "UTC",
            },
            "request_dt": datetime.datetime.utcnow().isoformat(),
        }
    logger.warning(resp)
    return None


def parse_dataset_element(d, namespace):
    dataset_dict = {}
    for i in d.getiterator():
        clean_tag = i.tag.replace('{' + namespace + '}', '')
        if clean_tag == 'dataset':
            dataset_dict = dict(**i.attrib)

        if clean_tag == 'dataSize':
            dataset_dict = dict(
                data_size=float(i.text), **i.attrib, **dataset_dict
            )

        if clean_tag == 'date':
            dataset_dict = dict(date_modified=i.text, **dataset_dict)
    return dataset_dict


def parse_response_thredds(response):
    stream_name = response['stream']['table_name']
    catalog = TDSCatalog(
        response['result']['thredds_catalog'].replace('.html', '.xml')
    )
    catalog_dict = {
        'stream_name': stream_name,
        'catalog_url': catalog.catalog_url,
        'base_tds_url': catalog.base_tds_url,
        'async_url': response['result']['download_catalog'],
    }
    req = requests.get(catalog.catalog_url)
    catalog_root = etree.fromstring(req.content)

    namespaces = {}
    for k, v in catalog_root.nsmap.items():
        if k is None:
            namespaces['cat'] = v
        else:
            namespaces[k] = v
    dataset_elements = catalog_root.xpath(
        '/cat:catalog/cat:dataset/cat:dataset', namespaces=namespaces
    )
    datasets = [
        parse_dataset_element(i, namespaces['cat']) for i in dataset_elements
    ]
    catalog_dict['datasets'] = datasets

    return catalog_dict


def filter_and_parse_datasets(cat):
    import re

    stream_cat = cat.copy()
    name = stream_cat['stream_name']
    filtered_datasets = []
    for d in stream_cat['datasets']:
        m = re.search(
            r'(deployment(\d{4})_(%s)_(\d{4}\d{2}\d{2}T\d+.\d+)-(\d{4}\d{2}\d{2}T\d+.\d+).nc)'
            % (name),
            str(d['name']),
        )
        if m:
            file_name, dep_num, ref, start, end = m.groups()
            dataset = dict(
                deployment=int(dep_num), start_ts=start, end_ts=end, **d
            )
            filtered_datasets.append(dataset)

    stream_cat['datasets'] = filtered_datasets
    return stream_cat


def setup_etl(
    stream, source='ooinet', target_bucket='s3://ooi-data'
):
    name = stream['stream_name']

    harvest_location = os.path.expanduser('~/.ooi-harvester')

    # Setup Local temp folder for netcdf
    temp_fold = os.path.join(harvest_location, name)
    if not os.path.exists(os.path.dirname(temp_fold)):
        os.mkdir(os.path.dirname(temp_fold))

    if not os.path.exists(temp_fold):
        os.mkdir(temp_fold)

    # Setup S3 Bucket
    temp_s3_fold = f"s3://temp-ooi-data/{name}.zarr"
    final_s3_fold = f"{target_bucket}/{name}"

    if source == 'ooinet':
        retrieved_dt = stream['result']['request_dt']
    else:
        retrieved_dt = stream['retrieved_dt']
        del stream['retrieved_dt']
    return dict(
        temp_fold=temp_fold,
        temp_bucket=temp_s3_fold,
        final_bucket=final_s3_fold,
        retrieved_dt=retrieved_dt,
        **stream,
    )


def seconds_to_date(num):
    start_dt = datetime.datetime(1900, 1, 1)
    return start_dt + datetime.timedelta(seconds=num)


def get_storage_options(path):
    if path.startswith("s3://"):
        return STORAGE_OPTIONS["aws"]
