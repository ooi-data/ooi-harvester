import json
from typing import List

import dask.dataframe as dd

from ooi_harvester.settings import harvest_settings


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
            f"{harvest_settings.s3_buckets.metadata}/ooi_streams"
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
