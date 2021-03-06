import os
import json
import itertools as it
import numpy as np
import pandas as pd
import dask
import yaml
import datetime

from .utils import (
    FS,
    read_cava_assets,
    df2parquet,
    json2bucket,
    compile_streams_parameters,
    compile_instrument_streams,
    create_ooinet_inventory,
    get_axiom_ooi_catalog,
    write_parquet,
    write_axiom_catalog,
    create_catalog_item,
    create_catalog_source,
)
from ..utils.conn import get_global_ranges, get_toc
from ..utils.compute import map_concurrency
from ..config import (
    GH_DATA_ORG,
    GH_PAT,
)


def set_stream(param, stream):
    param['stream'] = '-'.join([stream['method'], stream['stream']])
    return param


def get_ooi_streams_and_parameters():
    instruments = get_toc()['instruments']
    streams_list = compile_instrument_streams(instruments)
    parameters_list = compile_streams_parameters(streams_list)
    streams = [
        dict(
            parameter_ids=','.join([str(p['pid']) for p in st['parameters']]),
            **st,
        )
        for st in streams_list
    ]
    streams_df = pd.DataFrame(streams).drop('parameters', axis=1)
    parameters_df = pd.DataFrame(parameters_list)
    return streams_df, parameters_df


def create_metadata(
    bucket,
    axiom_refresh=False,
    global_ranges_refresh=False,
    cava_assets_refresh=False,
    ooinet_inventory_refresh=False,
    ooi_streams_refresh=False,
    instrument_catalog_refresh=False,
    legacy_inst_catalog_refresh=False,
):
    cava_assets, streams_df, parameters_df = None, None, None
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
        streams_df, parameters_df = get_ooi_streams_and_parameters()
        df2parquet(streams_df, 'ooi_streams', bucket)
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
    if legacy_inst_catalog_refresh:
        if not isinstance(cava_assets, dict):
            cava_assets = read_cava_assets()

        if not isinstance(streams_df, pd.DataFrame) or not isinstance(
            parameters_df, pd.DataFrame
        ):
            streams_df, parameters_df = get_ooi_streams_and_parameters()

        cava_streams = streams_df[
            streams_df.reference_designator.isin(
                cava_assets['instruments'].reference_designator
            )
        ]
        row_list = [stream for _, stream in cava_streams.iterrows()]
        instrument_catalog = map_concurrency(
            create_catalog_item,
            row_list,
            func_args=(
                parameters_df,
                cava_assets['parameters'],
                cava_assets['infrastructures'],
                cava_assets['instruments'],
                cava_assets['sites'],
            ),
        )
        json2bucket(instrument_catalog, "legacy_catalog.json", bucket)

    if instrument_catalog_refresh:
        if not isinstance(cava_assets, dict):
            cava_assets = read_cava_assets()

        if not isinstance(streams_df, pd.DataFrame) or not isinstance(
            parameters_df, pd.DataFrame
        ):
            streams_df, parameters_df = get_ooi_streams_and_parameters()

        instrument_catalog_list = []
        instruments_df = cava_assets['instruments']
        for _, inst in instruments_df.iterrows():
            inst_dict = inst.to_dict()
            inst_streams = streams_df[
                streams_df.reference_designator.str.match(
                    inst_dict["reference_designator"]
                )
            ]
            param_list = []
            for _, row in inst_streams.iterrows():
                int_pids = np.array(row['parameter_ids'].split(',')).astype(
                    int
                )
                params = list(
                    map(
                        lambda p: set_stream(p, row),
                        json.loads(
                            parameters_df[
                                parameters_df['pid'].isin(int_pids)
                            ].to_json(orient='records')
                        ),
                    )
                )
                param_list.append(params)
            inst_params = list(it.chain.from_iterable(param_list))
            inst_dict['streams'] = json.loads(
                inst_streams.to_json(orient='records')
            )
            inst_dict['parameters'] = inst_params
            instrument_catalog_list.append(inst_dict)
        json2bucket(
            instrument_catalog_list, "instruments_catalog.json", bucket
        )


def create_data_catalog(
    bucket,
    site_branch,
):
    from github import Github

    data_list = list(
        filter(
            lambda d: os.path.basename(d)
            not in ['index.html', 'data_availability'],
            FS.listdir(bucket, detail=False),
        )
    )
    now = datetime.datetime.utcnow()
    root_cat_dict = {
        'name': 'OOI Data Streams Catalog',
        'description': "OOI Data Intake Catalog. This effort is part of the University of Washington, Regional Cabled Array Value Add Project.",
        'metadata': {
            'version': '0.1.0',
            'last_updated': now.isoformat(),
            'owner': 'University of Washington, Regional Cabled Array',
        },
        'sources': {},
    }

    sources = map_concurrency(
        create_catalog_source, data_list, func_args=(FS,)
    )
    for source in sources:
        root_cat_dict['sources'].update(source)

    gh = Github(GH_PAT)
    site_repo_path = os.path.join(GH_DATA_ORG, f"{GH_DATA_ORG}.github.io")
    repo = gh.get_repo(site_repo_path)
    catalog_file_name = 'catalog.yaml'
    file_contents = [
        c
        for c in repo.get_contents('.', ref=site_branch)
        if c.path == catalog_file_name
    ]
    if len(file_contents) == 1:
        content = file_contents[0]
        repo.update_file(
            path=content.path,
            message=f"⬆️ Data Catalog updated at {now.isoformat()}",
            content=yaml.dump(root_cat_dict),
            sha=content.sha,
            branch=site_branch,
        )
    else:
        repo.create_file(
            catalog_file_name,
            f"🪄 Data Catalog created at {now.isoformat()}",
            yaml.dump(root_cat_dict),
            branch=site_branch,
        )
