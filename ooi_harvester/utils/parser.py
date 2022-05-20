import os
import datetime
import math
import traceback
from typing import Dict, List, Any

import requests
from loguru import logger
from lxml import etree
from siphon.catalog import TDSCatalog
from dask.utils import memory_repr
import numpy as np
from dateutil import parser

from ooi_harvester.settings import harvest_settings


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
    if resp is not None:
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
                "request_dt": resp["request_dt"],
            }
        else:
            logger.warning(resp)
    return resp


def param_change(name):
    """
    Method to accomodate for param change.
    https://oceanobservatories.org/renaming-data-stream-parameters/
    """

    if name == 'pressure_depth':
        return 'pressure'
    else:
        return name


def parse_param_dict(param_dict):
    unit = None
    if "unit" in param_dict:
        if isinstance(param_dict["unit"], dict):
            if "value" in param_dict["unit"]:
                unit = param_dict["unit"]["value"]
    product_type = None
    if "data_product_type" in param_dict:
        if isinstance(param_dict["data_product_type"], dict):
            if "value" in param_dict["data_product_type"]:
                product_type = param_dict["data_product_type"]["value"]
    return {
        "pid": param_dict["id"],
        "reference_designator": param_change(param_dict["name"]),
        "parameter_name": param_dict["display_name"],
        "netcdf_name": param_dict["netcdf_name"],
        "standard_name": param_dict["standard_name"],
        "description": param_dict["description"],
        "unit": unit,
        "data_level": param_dict['data_level'],
        "data_product_type": product_type,
        "data_product_identifier": param_dict["data_product_identifier"],
        "last_updated": datetime.datetime.utcnow().isoformat(),
    }


def parse_global_range_dataframe(global_ranges):
    """Cleans up the global ranges dataframe"""
    global_df = global_ranges[global_ranges.columns[:-3]]
    global_df.columns = [
        "reference_designator",
        "parameter_id_r",
        "parameter_id_t",
        "global_range_min",
        "global_range_max",
        "data_level",
        "units",
    ]
    return global_df


def get_bytes(value, unit):
    bytes_map = {
        'bytes': 1,
        'Kbytes': 1024 ** 1,
        'Mbytes': 1024 ** 2,
        'Gbytes': 1024 ** 3,
    }
    return value * bytes_map[unit]


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
            dataset_dict = dict(
                size_bytes=get_bytes(
                    dataset_dict['data_size'], dataset_dict['units']
                ),
                **dataset_dict,
            )

        if clean_tag == 'date':
            dataset_dict = dict(date_modified=i.text, **dataset_dict)
    return dataset_dict


def parse_ooi_data_catalog(catalog_url) -> Dict[Any, Any]:
    catalog = TDSCatalog(
        catalog_url.replace('.html', '.xml')
    )
    catalog_dict = {
        'catalog_url': catalog.catalog_url,
        'base_tds_url': catalog.base_tds_url,
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


def parse_response_thredds(response) -> Dict[Any, Any]:
    stream_name = response['stream']['table_name']
    catalog_dict = parse_ooi_data_catalog(response['result']['thredds_catalog'])
    catalog_dict.update({
        'stream_name': stream_name,
        'async_url': response['result']['download_catalog'],
    })
    return catalog_dict


def filter_ooi_datasets(datasets, stream_name):
    import re
    provenance_files = []
    filtered_datasets = []
    for d in datasets:
        m = re.search(
            r'(deployment(\d{4})_(%s)_(\d{4}\d{2}\d{2}T\d+.\d+)-(\d{4}\d{2}\d{2}T\d+.\d+).nc)'  # noqa
            % (stream_name),
            str(d['name']),
        )
        prov = re.search(
            r'(deployment(\d{4})_(%s)_aggregate_provenance.json)' % (stream_name),
            str(d['name']),
        )
        if m:
            _, dep_num, _, start, end = m.groups()
            dataset = dict(
                deployment=int(dep_num), start_ts=start, end_ts=end, **d
            )
            filtered_datasets.append(dataset)
        elif prov:
            _, dep_num, _ = prov.groups()
            provenance = dict(deployment=int(dep_num), **d)
            provenance_files.append(provenance)
    
    return provenance_files, filtered_datasets


def filter_and_parse_datasets(cat):
    stream_cat = cat.copy()
    name = stream_cat['stream_name']
    provenance_files, filtered_datasets = filter_ooi_datasets(stream_cat['datasets'], name)

    total_bytes = np.sum([d['size_bytes'] for d in filtered_datasets])
    stream_cat['datasets'] = filtered_datasets
    stream_cat['provenance'] = provenance_files
    stream_cat['total_data_size'] = memory_repr(total_bytes)
    stream_cat['total_data_bytes'] = total_bytes
    return stream_cat


def filter_datasets_by_time(
    datasets: List[dict], start_dt: np.datetime64, end_dt: np.datetime64
) -> List[dict]:
    """
    Filters datasets collection based on the given start and end datetime.

    Each dataset dictionary in the collection MUST have
    `start_ts` and `end_ts`key in it.

    Parameters
    ----------
    datasets : list
        The datasets collection to be filtered.
    start_dt : np.datetime64
        The start datetime desired.
    end_dt : np.datetime64
        The end datetime desired.

    Returns
    -------
    list
        The filtered datasets collection

    """
    filtered_datasets = []
    for d in datasets:
        start_d = np.datetime64(parser.parse(d['start_ts']))
        end_d = np.datetime64(parser.parse(d['end_ts']))
        if start_d >= start_dt.astype(
            start_d.dtype
        ) and end_d <= end_dt.astype(start_d.dtype):
            filtered_datasets.append(d)
    return filtered_datasets


def setup_etl(stream, source='ooinet', target_bucket='s3://ooi-data'):
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
        return harvest_settings.storage_options.aws.dict()


def get_items(keys, orig_dict):
    new_dict = {}
    for k, v in orig_dict.items():
        if k in keys:
            new_dict[k] = v
    return new_dict


def rename_item(old_key, new_key, orig_dict):
    new_dict = orig_dict.copy()
    if old_key in new_dict:
        new_dict.update({new_key: new_dict[old_key]})
        del new_dict[old_key]
    return new_dict


def parse_exception(exception):
    exc_dict = {
        'type': type(exception).__name__,
        'value': str(exception),
        'traceback': "".join(
            traceback.format_exception(
                type(exception), exception, exception.__traceback__
            )
        ),
    }
    return exc_dict
