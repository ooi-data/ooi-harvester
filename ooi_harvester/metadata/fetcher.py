import json
from typing import List

from ooi_harvester.metadata import get_toc, get_ooi_streams_and_parameters


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
        instruments = get_toc()['instruments']
        filtered_instruments = [
            i for i in instruments if i['reference_designator'] in refdes_list
        ]
        if len(filtered_instruments) > 0:
            filtered_df, _ = get_ooi_streams_and_parameters(
                filtered_instruments
            )
            streams_json = filtered_df.to_json(orient='records')
            streams_list = json.loads(streams_json)
    return streams_list
