import os
import json
import datetime
from typing import List

import fsspec
from loguru import logger
import pandas as pd
import dask.dataframe as dd

from ..config import METADATA_BUCKET, PRODUCER_BUCKET
from ..utils.conn import request_data, check_zarr, send_request
from ..utils.parser import (
    estimate_size_and_time,
    get_storage_options,
    parse_uframe_response,
)
from ..utils.compute import map_concurrency


def fetch_instrument_streams_list(refdes_list=[]) -> List[dict]:
    """
    Fetch streams metadata from instrument(s).
    Note: This function currently only get science streams.

    Args:
        refdes_list (list, str): List of reference designators

    Returns:
        list: List of streams metadata
    """
    streams_list = []
    if isinstance(refdes_list, str):
        refdes_list = refdes_list.split(',')

    if len(refdes_list) > 0:
        streamsddf = dd.read_parquet(
            f"{METADATA_BUCKET}/ooi_streams",
            storage_options=get_storage_options(METADATA_BUCKET),
        )
        # Science streams only!
        filtered_df = streamsddf[
            streamsddf.reference_designator.isin(refdes_list)
            & (streamsddf.stream_type.str.match('Science'))
            & ~(streamsddf.method.str.contains('bad'))
        ].compute()
        streams_json = filtered_df.to_json(orient='records')
        streams_list = json.loads(streams_json)
    return streams_list


def create_request_estimate(
    stream_dct,
    start_dt=None,
    end_dt=None,
    refresh=False,
    existing_data_path=None,
):
    beginTime = pd.to_datetime(stream_dct['beginTime'])
    endTime = pd.to_datetime(stream_dct['endTime'])

    zarr_exists = False
    if not refresh:
        if existing_data_path is not None:
            zarr_exists, last_time = check_zarr(
                os.path.join(existing_data_path, stream_dct['table_name']),
                storage_options=get_storage_options(existing_data_path),
            )
        else:
            raise ValueError(
                "Please provide existing data path when not refreshing."
            )

    if zarr_exists:
        start_dt = last_time + datetime.timedelta(seconds=1)
        end_dt = datetime.datetime.utcnow()

    if start_dt:
        if not isinstance(start_dt, datetime.datetime):
            raise TypeError(f"{start_dt} is not datetime.")
        beginTime = start_dt
    if end_dt:
        if not isinstance(end_dt, datetime.datetime):
            raise TypeError(f"{end_dt} is not datetime.")
        endTime = end_dt

    response, request_dict = request_data(
        stream_dct['platform_code'],
        stream_dct['mooring_code'],
        stream_dct['instrument_code'],
        stream_dct['method'],
        stream_dct['stream'],
        beginTime,
        endTime,
        estimate=True,
    )
    if response:
        table_name = f"{stream_dct['reference_designator']}-{stream_dct['method']}-{stream_dct['stream']}"
        text = """
        *************************************
        {0}
        -------------------------------------
        {1}
        *************************************
        """.format
        if "requestUUID" in response:
            m = estimate_size_and_time(response)
            request_dict["params"].update({"estimate_only": "false"})
            request_dict.update(
                {
                    "stream": stream_dct,
                    "estimated": response,
                    "zarr_exists": zarr_exists,
                }
            )
            logger.debug(text(table_name, m))
        else:
            m = f"""
            Skipping... Data not available.
            """
            request_dict.update({"stream": stream_dct, "estimated": response})
            logger.debug(text(table_name, m))

        return request_dict


def _sort_and_filter_estimated_requests(estimated_requests):
    """Internal sorting function"""

    success_requests = sorted(
        filter(
            lambda req: "requestUUID" in req['estimated'], estimated_requests
        ),
        key=lambda i: i['stream']['count'],
    )
    failed_requests = list(
        filter(
            lambda req: "requestUUID" not in req['estimated'],
            estimated_requests,
        )
    )

    return {
        'success_requests': success_requests,
        'failed_requests': failed_requests,
    }


def perform_request(req, refresh=False):
    TODAY_DATE = datetime.datetime.utcnow()
    name = req['stream']['table_name']

    refresh_text = 'refresh' if refresh else 'daily'
    datestr = f"{TODAY_DATE:%Y%m}"
    fname = f"{name}__{datestr}__{refresh_text}"
    fs = fsspec.filesystem(
        PRODUCER_BUCKET.split(":")[0], **get_storage_options(PRODUCER_BUCKET)
    )
    fpath = os.path.join(PRODUCER_BUCKET, 'ooinet-requests', fname)

    if fs.exists(fpath):
        logger.info(
            f"Already requested {name} on "
            f"{datestr} for {refresh_text} ({fpath})"
        )
        with fs.open(fpath, mode='r') as f:
            response = json.load(f)
    else:
        logger.info(f"Requesting {name}")
        response = dict(
            result=parse_uframe_response(
                send_request(req["url"], params=req["params"])
            ),
            **req,
        )
        with fs.open(fpath, mode='w') as f:
            json.dump(response, f)

    return response


def perform_estimates(instrument_rd, refresh, existing_data_path):
    streams_list = fetch_instrument_streams_list(instrument_rd)
    estimated_requests = map_concurrency(
        create_request_estimate,
        streams_list,
        func_kwargs=dict(
            refresh=refresh, existing_data_path=existing_data_path
        ),
        max_workers=50,
    )
    estimated_dict = _sort_and_filter_estimated_requests(estimated_requests)
    success_requests = estimated_dict['success_requests']
    return success_requests
