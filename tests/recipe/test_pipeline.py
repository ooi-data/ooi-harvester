import fsspec
import xarray as xr
from pathlib import Path
import pandas as pd
import yaml
import json


def test_minio_data_bucket(minio_service):
    fs = fsspec.filesystem(
        "s3",
        **minio_service,
    )
    assert fs.exists("ooi-data")
    assert fs.exists("temp-ooi-data")


def test_data_pipeline(minio_service, harvest_settings):
    from ooi_harvester.producer import StreamHarvest
    from ooi_harvester.processor.pipeline import OOIStreamPipeline

    HERE = Path(__file__).parent.parent.absolute()
    BASE = HERE.joinpath('data')
    CONFIG_PATH = BASE.joinpath(
        harvest_settings.github.defaults.config_path_str
    )
    RESPONSE_PATH = BASE.joinpath(
        harvest_settings.github.defaults.response_path_str
    )
    PROCESS_STATUS_PATH = BASE.joinpath(
        harvest_settings.github.defaults.process_status_path_str
    )

    config_json = yaml.load(CONFIG_PATH.open(), Loader=yaml.SafeLoader)
    config_json["harvest_options"]["path_settings"] = minio_service[
        "client_kwargs"
    ]
    stream_harvest = StreamHarvest(**config_json)

    response = json.load(RESPONSE_PATH.open())
    config_json = yaml.load(CONFIG_PATH.open(), Loader=yaml.SafeLoader)
    pipeline = OOIStreamPipeline(response, stream_harvest=stream_harvest)
    state = pipeline.flow.run()

    assert state.is_successful() is True

    # Checks if the zarr file can be read
    store = fsspec.get_mapper(
        pipeline.nc_files_dict['final_bucket'], **minio_service
    )
    ds = xr.open_dataset(
        store, engine='zarr', backend_kwargs={'consolidated': True}
    )
    assert isinstance(ds, xr.Dataset)

    # Checks if the dataset actually completed
    last_dt_response = pd.to_datetime(
        response['datasets'][-1]['end_ts']
    ).floor(freq='H')
    last_dt = pd.to_datetime(ds.time.data[-1]).floor(freq='H')

    assert last_dt == last_dt_response
