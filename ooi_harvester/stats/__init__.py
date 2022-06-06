import requests
import re
from collections import OrderedDict
from itertools import groupby
import json
import datetime

import fsspec
import zarr
from dask.utils import memory_repr
import numpy as np
from loguru import logger
from xarray.coding.times import decode_cf_datetime
from ooi_harvester.utils.compute import map_concurrency
from ooi_harvester.settings.main import harvest_settings


FS = fsspec.filesystem('s3', **harvest_settings.storage_options.aws.dict())


def parse_inst_ref(name):
    m = re.findall(r'ooi-data/(\w+-\w+-\w+-\w+)-\w+-\w+', name)
    if len(m) == 1:
        inst_ref = m[0]
        return inst_ref


def is_valid(inst, instrument_refs=[]):
    name = inst['name']
    meta_exists = FS.exists(f"{name}/.zmetadata")
    if meta_exists:
        inst_ref = parse_inst_ref(name)
        if inst_ref:
            if inst_ref in instrument_refs:
                return True
            else:
                return False
    return


def create_stream_dct(data_stream):
    try:
        fmap = FS.get_mapper(data_stream['zarr_file'])
        zg = zarr.open_consolidated(fmap)
        stream_id = data_stream['zarr_file'].split('/')[-1]

        # Get variables, data products, and sizes
        variables = [
            dict(
                name=k,
                size=memory_repr(arr.nbytes),
                bytes_size=arr.nbytes,
                product_id=arr.attrs.get('data_product_identifier'),
            )
            for k, arr in zg.arrays()
        ]
        data_products = [
            v['name'] for v in variables if v['product_id'] is not None
        ]
        total_size = sum(v['bytes_size'] for v in variables)
        tsize_string = memory_repr(total_size)

        # Get date range
        time_arr = zg['time']
        units = time_arr.attrs.get('units', 'seconds since 1900-01-01')
        calendar = time_arr.attrs.get('calendar', 'gregorian')
        date_range = decode_cf_datetime(
            np.array([time_arr[0], time_arr[-1]]),
            units=units,
            calendar=calendar,
        )
        date_range_str = [str(d) for d in date_range]

        # Setup dict
        logger.info(f"Parsing {stream_id}")
        stream_dct = OrderedDict()
        stream_dct['id'] = stream_id
        stream_dct['data_location'] = f"s3://{data_stream['zarr_file']}"
        stream_dct['instrument_rd'] = data_stream['inst_ref']
        stream_dct['size'] = tsize_string
        stream_dct['bytes_size'] = total_size
        stream_dct['start_dt'], stream_dct['end_dt'] = date_range_str
        stream_dct['num_variables'] = len(variables)
        stream_dct['num_data_products'] = len(data_products)
        stream_dct['data_products'] = data_products
        stream_dct['variables'] = variables

        return stream_dct
    except Exception as e:
        logger.warning(f"Issues found for {data_stream['zarr_file']}: {e}")


def create_stats(s3_bucket: str = 'ooi-data'):
    resp = requests.get(
        'https://api.ooica.net/metadata/instruments'
    )
    instruments = resp.json()
    instrument_refs = [i['reference_designator'] for i in instruments]
    all_files = [
        i
        for i in FS.listdir(s3_bucket, detail=True)
        if i['type'] == 'directory'
    ]
    valid_data_stream = []
    for inst in all_files:
        valid_zarr = is_valid(inst, instrument_refs)
        if any(valid_zarr is t for t in [None, False]):
            logger.info(inst['name'], "Not valid zarr... skipping.")
        elif valid_zarr is True:
            valid_data_stream.append(
                {
                    'inst_ref': parse_inst_ref(inst['name']),
                    'zarr_file': inst['name'],
                }
            )

    all_stream_dct = map_concurrency(create_stream_dct, valid_data_stream)
    final_streams = [s for s in all_stream_dct if s is not None]
    instrument_groups = {
        k: list(v)
        for k, v in groupby(final_streams, key=lambda s: s['instrument_rd'])
    }
    all_instruments = []
    for k, v in instrument_groups.items():
        logger.info(f"Indexing {k}")
        stream_total = sum(map(lambda s: s['bytes_size'], v))
        inst_dct = next(
            filter(lambda i: i['reference_designator'] == k, instruments)
        )
        new_inst = OrderedDict()
        new_inst['id'] = inst_dct['reference_designator']
        new_inst['name'] = inst_dct['instrument_name']
        new_inst['formal_name'] = inst_dct['formal_name']
        new_inst['url'] = (
            inst_dct['instrument_url']
            if 'https://' in inst_dct['instrument_url']
            else f"https://interactiveoceans.washington.edu{inst_dct['instrument_url']}"
        )
        new_inst['total_bytes'] = stream_total
        new_inst['group'] = inst_dct['group_code']
        new_inst['size'] = memory_repr(stream_total)
        new_inst['num_streams'] = len(v)
        new_inst['streams'] = v
        all_instruments.append(new_inst)

    total_data_bytes = sum(i['total_bytes'] for i in all_instruments)
    index_dct = OrderedDict()
    index_dct['version'] = 'v1'
    index_dct['generated'] = datetime.datetime.utcnow().isoformat()
    index_dct['stats'] = OrderedDict()
    index_dct['stats']['num_instruments'] = len(all_instruments)
    index_dct['stats']['bytes'] = total_data_bytes
    index_dct['stats']['size'] = memory_repr(total_data_bytes)
    index_dct['instruments'] = all_instruments

    with FS.open(f'{s3_bucket}/index.json', 'w') as f:
        f.write(json.dumps(index_dct))

    with FS.open(f'{s3_bucket}/stats.json', 'w') as f:
        f.write(json.dumps(index_dct['stats']))
