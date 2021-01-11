import os
import pandas as pd
import dask

from .utils import (
    FS,
    read_cava_assets,
    df2parquet,
    compile_streams_parameters,
    compile_instrument_streams,
    create_ooinet_inventory,
    get_axiom_ooi_catalog,
    write_parquet,
    write_axiom_catalog,
)
from ..utils.conn import get_global_ranges, get_toc
from ..utils.compute import map_concurrency


def create_metadata(
    bucket,
    axiom_refresh=False,
    global_ranges_refresh=False,
    cava_assets_refresh=False,
    ooinet_inventory_refresh=False,
    ooi_streams_refresh=False,
):
    if cava_assets_refresh:
        cava_assets = read_cava_assets()
        for k, v in cava_assets.items():
            table_name = f"cava_{k}"
            df2parquet(v, table_name, bucket)

    # Get ooinet inventory
    if ooinet_inventory_refresh:
        ooinet_inventory = create_ooinet_inventory()
        for k, v in ooinet_inventory.items():
            table_name = f"{k}_inventory"
            df2parquet(v, table_name, bucket)

    # Get instruments streams from OOI
    if ooi_streams_refresh:
        instruments = get_toc()['instruments']
        streams_list = compile_instrument_streams(instruments)
        parameters_list = compile_streams_parameters(streams_list)
        streams = [
            dict(
                parameter_ids=','.join(
                    [str(p['pid']) for p in st['parameters']]
                ),
                **st,
            )
            for st in streams_list
        ]
        streams_df = pd.DataFrame(streams).drop('parameters', axis=1)
        df2parquet(streams_df, 'ooi_streams', bucket)
        parameters_df = pd.DataFrame(parameters_list)
        df2parquet(parameters_df, 'ooi_parameters', bucket)

    # Get global ranges
    if global_ranges_refresh:
        grdf = get_global_ranges()
        grddf = dask.dataframe.from_pandas(grdf, npartitions=len(grdf) / 1000)
        grpath = os.path.join(bucket, 'global_ranges')
        write_parquet(grddf, grpath)

    if axiom_refresh:
        # Get axiom thredds catalog
        axiom_catalog = get_axiom_ooi_catalog()
        map_concurrency(
            write_axiom_catalog,
            axiom_catalog,
            func_args=(
                bucket,
                FS,
            ),
        )
