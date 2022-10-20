import os
import re
import json
import itertools as it

import dask
import dask.dataframe as dd
import fsspec
import gspread
from loguru import logger
import pandas as pd
import datetime
import requests
from lxml import etree
import numpy as np
from siphon.catalog import TDSCatalog
import zarr
from s3fs.core import S3FileSystem

from ..config import STORAGE_OPTIONS
from ..utils.compute import map_concurrency
from ..utils.encoders import NumpyEncoder
from ..utils.conn import fetch_streams, retrieve_deployments
from ..utils.parser import get_items, rename_item, parse_dataset_element

FS = fsspec.filesystem('s3', **STORAGE_OPTIONS['aws'])

INSTRUMENT_MAP = {
    'MOP': '3-Axis Motion Pack',
    'HYDGN0': 'Hydrogen Sensor',
    'VEL3D': '3-D Single Point Velocity Meter',
    'ADCP': 'ADCP',
    'FLOBN': 'Benthic Fluid Flow',
    'ZPLS': 'Bio-acoustic Sonar',
    'BOTPT': 'Bottom Pressure and Tilt',
    'HYDBB': 'Broadband Acoustic Receiver (Hydrophone)',
    'OBSBB': 'Broadband Ocean Bottom Seismometer',
    'METBK': 'Bulk Meteorology Instrument Package',
    'CTD': 'CTD',
    'TMPSF': 'Diffuse Vent Fluid 3-D Temperature Array',
    'CAMHD': 'HD Digital Video Camera',
    'CAM': 'Digital Still Camera',
    'FDCHP': 'Direct Covariance Flux',
    'DO': 'Dissolved Oxygen',
    'ENG': 'Engineering',
    'FL': 'Fluorometer',
    'HPIES': 'Horizontal Electric Field, Pressure and Inverted Echo Sounder',
    'THSPH': 'Hydrothermal Vent Fluid In-situ Chemistry',
    'RASFL': 'Hydrothermal Vent Fluid Interactive Sampler',
    'TRHPH': 'Hydrothermal Vent Fluid Temperature and Resistivity',
    'HYDLF': 'Low Frequency Acoustic Receiver (Hydrophone)',
    'MASSP': 'Mass Spectrometer',
    'NUTNR': 'Nitrate',
    'OSMOI': 'Osmosis-Based Water Sampler',
    'PPSDN': 'Particulate DNA Sampler',
    'PCO2A': 'pCO2 Air-Sea',
    'PCO2W': 'pCO2 Water',
    'PARAD': 'Photosynthetically Active Radiation',
    'PRESF': 'Seafloor Pressure',
    'PHSEN': 'Seawater pH',
    'OBSSP': 'Short-Period Ocean Bottom Seismometer',
    'VELPT': 'Single Point Velocity Meter',
    'SPKIR': 'Spectral Irradiance',
    'OPTAA': 'Spectrophotometer',
    'WAVSS': 'Surface Wave Spectra',
    'PREST': 'Tidal Seafloor Pressure',
    'COVIS': 'Vent Imaging Sonar',
    'AOA': 'A-0-A Pressure Sensor',
    'SCT': 'Self-Calibrating Triaxial Accelerometer',
    'SCP': 'Self-Calibrating Pressure Recorder',
    'D1000': 'Hydrothermal Vent Fluid Temperature Sensor',
}


def fetch_creds():
    GOOGLE_SERVICE_JSON = os.environ.get("GOOGLE_SERVICE_JSON", "")
    GSPREAD_DIR = os.path.join(os.path.expanduser("~"), ".config", "gspread")
    if not os.path.exists(GSPREAD_DIR):
        os.mkdir(GSPREAD_DIR)
    FS.get(
        GOOGLE_SERVICE_JSON,
        os.path.join(GSPREAD_DIR, "service_account.json"),
    )


def df2list(df):
    return json.loads(df.to_json(orient='records', date_format="iso"))


def set_instrument_group(rd):
    inst_code = rd.split('-')[3]
    m = re.search(('|'.join(list(INSTRUMENT_MAP.keys()))), inst_code)
    if m:
        return m.group()
    else:
        return None


def read_cava_assets():
    fetch_creds()
    gc = gspread.service_account()
    wks = gc.open("CAVA_Assets")
    dfdict = {}
    for ws in wks.worksheets():
        name = ws.title
        if name in [
            "Arrays",
            "Areas",
            "Sites",
            "Infrastructures",
            "Instruments",
            "Streams",
            "Parameters",
            "Instrument-Groups",
            "DataProduct-Groups",
            "DataProducts",
            "POE"
        ]:
            lower_name = name.lower()
            df = pd.DataFrame(ws.get_all_records())
            df = df.replace({"TRUE": True, "FALSE": False}).copy()
            if 'wp_page' in df.columns:
                df.loc[:, 'wp_page'] = df['wp_page'].astype(str)
            if 'wp_mapping' in df.columns:
                df.loc[:, 'wp_mapping'] = df['wp_mapping'].astype(str)
            dfdict[lower_name] = df
    return dfdict


def get_stream_only(stream):
    return rename_item(
        "method",
        "stream_method",
        rename_item(
            "stream_id",
            "m2m_id",
            rename_item(
                "stream",
                "reference_designator",
                get_items(
                    [
                        "stream",
                        "method",
                        "stream_id",
                        "stream_type",
                        "stream_content",
                    ],
                    stream,
                ),
            ),
        ),
    )


def compile_instrument_streams(instruments):
    logger.info("Compiling instrument streams ...")
    streams_list = map_concurrency(fetch_streams, instruments, max_workers=50)
    streams_list = list(
        dict(
            table_name="-".join(
                [st["reference_designator"], st["method"], st["stream"]]
            ),
            **st,
        )
        for st in it.chain.from_iterable(streams_list)
    )
    streamsdf = pd.DataFrame(streams_list)
    streamsdf.loc[:, "beginTime"] = streamsdf["beginTime"].apply(
        pd.to_datetime
    )
    streamsdf.loc[:, "endTime"] = streamsdf["endTime"].apply(pd.to_datetime)
    streamsdf.loc[:, 'group_code'] = streamsdf.reference_designator.apply(
        set_instrument_group
    )
    return df2list(streamsdf)


def compile_streams_parameters(streams_list):
    logger.info("Compiling parameters ...")
    all_params = [
        params
        for params in it.chain.from_iterable(
            [st['parameters'] for st in streams_list]
        )
    ]
    return list({v['pid']: v for v in all_params}.values())


def compile_instrument_deployments(dfdict):
    logger.info("Compiling instrument deployments ...")
    instrumentsdf = pd.DataFrame(dfdict["instruments"])
    inst_list = instrumentsdf.reference_designator.values
    dep_list = map_concurrency(retrieve_deployments, inst_list, max_workers=10)
    dep_list = list(it.chain.from_iterable(dep_list))
    return dep_list


def get_infrastructure(infra_rd, dfdict):
    infras = pd.DataFrame(dfdict["infrastructures"])
    infra_df = infras[infras.reference_designator.str.match(infra_rd)]
    if len(infra_df) == 1:
        return json.loads(
            infras[infras.reference_designator.str.match(infra_rd)]
            .iloc[0]
            .to_json()
        )


def get_instrument(instrument_rd, dfdict):
    insts = pd.DataFrame(dfdict["instruments"])
    return json.loads(
        insts[insts.reference_designator.str.match(instrument_rd)]
        .iloc[0]
        .to_json()
    )


def get_site(site_rd, dfdict):
    sites = pd.DataFrame(dfdict["sites"])
    return json.loads(
        sites[sites.reference_designator.str.match(site_rd)].iloc[0].to_json()
    )


def get_parameters(parameters, dfdict):
    dfdict = pd.DataFrame(dfdict)
    return json.loads(
        dfdict["parameters"][
            dfdict["parameters"].reference_designator.isin(parameters)
        ].to_json(orient="records")
    )


def create_catalog_item(
    stream,
    parameters_df,
    cava_params,
    cava_infrastructure,
    cava_instruments,
    cava_sites,
):
    item = {}
    # get parameters
    int_pids = np.array(stream['parameter_ids'].split(',')).astype(int)
    params = json.loads(
        parameters_df[parameters_df['pid'].isin(int_pids)].to_json(
            orient='records'
        )
    )
    param_rds = [p['reference_designator'] for p in params]
    item["data_table"] = stream["table_name"]
    item["instrument_rd"] = stream["reference_designator"]
    item["site_rd"] = stream["platform_code"]
    item["infra_rd"] = stream["mooring_code"]
    item["inst_rd"] = stream["instrument_code"]
    item["stream_rd"] = stream["stream"]
    item["stream_method"] = stream["method"]
    item["stream_type"] = stream["stream_type"]
    item["stream"] = get_stream_only(stream.to_dict())
    item["parameter_rd"] = ",".join(param_rds)
    item["ooi_parameter_ids"] = list(int_pids)
    item["infrastructure"] = json.loads(
        cava_infrastructure[
            cava_infrastructure.reference_designator.str.match(
                "-".join([stream["platform_code"], stream["mooring_code"]])
            )
        ].to_json(orient='records')
    )[0]
    item["instrument"] = json.loads(
        cava_instruments[
            cava_instruments.reference_designator.str.match(
                stream["reference_designator"]
            )
        ].to_json(orient='records')
    )[0]
    item["site"] = json.loads(
        cava_sites[
            cava_sites.reference_designator.str.match(stream['platform_code'])
        ].to_json(orient='records')
    )[0]
    item["parameters"] = json.loads(
        cava_params[
            cava_params['reference_designator'].isin(param_rds)
        ].to_json(orient='records')
    )
    return item


def create_instruments_catalog(dfdict, streams_list):
    logger.info("Creating instruments catalog ...")
    cava_instruments = dfdict["instruments"]
    cava_streams = list(
        filter(
            lambda st: st["reference_designator"]
            in [i['reference_designator'] for i in cava_instruments],
            streams_list,
        )
    )
    catalog_list = [
        create_catalog_item(stream, dfdict) for stream in cava_streams
    ]
    return catalog_list


def get_catalog_meta(catalog_ref):
    stream_name, ref = catalog_ref
    catalog = ref.follow()
    fetch_dt = datetime.datetime.utcnow()
    catalog_dict = {
        'stream_name': stream_name,
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
    catalog_dict['retrieved_dt'] = fetch_dt.isoformat()

    return catalog_dict


def get_axiom_ooi_catalog():
    logger.info("Fetching OOI Axiom Gold Copy THREDDS Catalog ...")
    axiom_ooi_catalog = TDSCatalog(
        'http://thredds.dataexplorer.oceanobservatories.org/thredds/catalog/ooigoldcopy/public/catalog.xml'
    )
    catalog_list = map_concurrency(
        get_catalog_meta, axiom_ooi_catalog.catalog_refs.items()
    )
    return catalog_list


def write_axiom_catalog(catalog, bucket, fs):
    axiom_catalog_dir = os.path.join(bucket, 'axiom_catalog')
    file_location = os.path.join(axiom_catalog_dir, catalog['stream_name'])
    with fs.open(file_location, 'w') as f:
        json.dump(catalog, f)
    return file_location


def write_parquet(df, s3path):
    logger.info(f"Writing to s3://{s3path}...")
    df.to_parquet(
        f"s3://{s3path}",
        write_index=False,
        storage_options=STORAGE_OPTIONS['aws'],
    )


def df2parquet(df, table_name, bucket):
    npartitions = int(len(df) / 1000)
    if npartitions <= 0:
        npartitions = 1
    ddf = dd.from_pandas(df, npartitions=npartitions)
    s3path = os.path.join(bucket, table_name)
    write_parquet(ddf, s3path)


def json2bucket(data, filepath, bucket):
    with FS.open(os.path.join(bucket, filepath), mode='w') as f:
        json.dump(data, f, cls=NumpyEncoder)


def create_ooinet_inventory():
    """ Create instruments inventory based on what's available in ooinet """

    OOINET_LIST = requests.get(
        'https://ooinet.oceanobservatories.org/api/uframe/instrument_list?refresh=false'
    ).json()['instruments']
    ooinetdf = pd.DataFrame(OOINET_LIST).copy()
    ooinetdf.loc[:, 'iris_enabled'] = ooinetdf['iris_enabled'].astype(bool)
    # GET STUFF THAT ARE IN IRIS
    irisdf = ooinetdf[
        (ooinetdf.iris_enabled == True)
        & ~ooinetdf.reference_designator.str.contains('BOTPT')
    ].copy()
    # GET STUFF THAT ARE IN RAW DATA ARCHIVE
    rawdatadf = ooinetdf[~ooinetdf.rds_link.isna()]
    # GET STUFF THAT ARE IN CI
    m2mdf = ooinetdf[
        ooinetdf.reference_designator.str.contains('BOTPT')
        | (ooinetdf.iris_enabled == False & ooinetdf.rds_link.isna())
    ]

    return {'iris': irisdf, 'rawdata': rawdatadf, 'm2m': m2mdf}


def create_catalog_source(
    data_zarr: str,
    fs: S3FileSystem,
) -> dict:
    """Create a catalog dictionary entry for a zarr dataset in s3"""
    name = os.path.basename(data_zarr)
    # print(f"=== Creating data catalog entry for {name} ===")
    zmeta = os.path.join(data_zarr, '.zmetadata')
    intake_dict = {
        'description': '',
        'metadata': {},
        'driver': 'zarr',
        'args': {
            'urlpath': '',
            'consolidated': True,
            'storage_options': {'anon': True},
        },
    }

    if fs.exists(zmeta):
        try:
            zg = zarr.open_consolidated(fs.get_mapper(data_zarr))
            # print("Parsing global attributes ...")
            # Parse global attributes
            global_attrs = zg.attrs.asdict()
            data_meta = {
                'id': name,
                'owner': global_attrs['Owner'],
                'notes': global_attrs['Notes'],
                'reference_designator': {
                    'site': global_attrs['subsite'],
                    'infrastructure': "-".join(
                        [global_attrs['subsite'], global_attrs['node']]
                    ),
                    'instrument': "-".join(
                        [
                            global_attrs['subsite'],
                            global_attrs['node'],
                            global_attrs['sensor'],
                        ]
                    ),
                    'stream_method': global_attrs['collection_method'],
                    'stream': global_attrs['stream'],
                },
            }
            intake_dict['metadata'].update(data_meta)
            intake_dict['description'] = global_attrs['title']

            # print("Parsing parameter attributes ...")
            # Parse parameter attributes
            data_product_list = []
            for k, arr in zg.arrays():
                # Make a copy of the attributes so it doesn't modify the original
                arr_attrs = arr.attrs.asdict().copy()
                # For now just filter params that are L1/L2 and time
                # Future should use the preferred_parameters
                if (
                    (k == "time")
                    or ("data_product_identifier" in arr_attrs)
                    and (
                        "L1" in arr_attrs["data_product_identifier"]
                        or "L2" in arr_attrs["data_product_identifier"]
                        or "BOTSFLU" in arr_attrs["data_product_identifier"]
                    )
                ):
                    # Remove Array Dimensions key
                    del arr_attrs['_ARRAY_DIMENSIONS']
                    data_product_list.append(
                        dict(reference_designator=k, **arr_attrs)
                    )

            intake_dict['metadata'].update(
                {
                    'data_products': sorted(
                        data_product_list,
                        key=lambda p: p['reference_designator'],
                    )
                }
            )
            intake_dict['args']['urlpath'] = f"s3://{data_zarr}"
            # print("Done.")
            return {name: intake_dict}
        except Exception:
            # print(f"Error found: {e}. Skipping. Done.")
            return {}
    else:
        # print("No data found. Skipping. Done.")
        return {}
