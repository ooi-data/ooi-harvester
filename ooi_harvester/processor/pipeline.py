import os
import datetime
from typing import List
import textwrap

import fsspec
from loguru import logger
from prefect import Flow, task
# from prefect.environments import DaskKubernetesEnvironment
# from prefect.environments.storage import Docker

from . import process_dataset, finalize_zarr
from ..core import AbstractPipeline
from ..utils.parser import (
    parse_response_thredds,
    filter_and_parse_datasets,
    get_storage_options,
    setup_etl
)


@task
def processing_task(dataset_list, nc_files_dict, zarr_exists, refresh):
    name = nc_files_dict['stream']['table_name']
    logger.info(f"Processing {name}.")
    start_time = datetime.datetime.utcnow()
    if not zarr_exists or refresh:
        for idx, d in enumerate(dataset_list):
            is_first = False
            if idx == 0:
                is_first = True
            process_dataset(d, nc_files_dict, is_first=is_first)
        logger.info(f"Finalizing data stream {name}.")
        final_path = finalize_zarr(
            source_zarr=nc_files_dict['temp_bucket'],
            final_zarr=nc_files_dict['final_bucket'],
        )
    else:
        final_path = nc_files_dict['final_bucket']
    time_elapsed = datetime.datetime.utcnow() - start_time
    logger.info(f"DONE. ({final_path}) Time elapsed: {str(time_elapsed)}")


class OOIStreamPipeline(AbstractPipeline):
    def __init__(
        self, response, refresh=True, existing_data_path='s3://ooi-data'
    ):
        self.response = response
        self.refresh = refresh
        self.nc_files_dict = None
        self.__existing_data_path = existing_data_path
        self.fs = None

        self._setup_pipeline()

    def __repr__(self):
        return textwrap.dedent(
            f"""\
        Pipeline: {self.name}
        Range: {self.start_dt} - {self.end_dt}
        Number of datasets: {len(self.sources)}
        """
        )

    @property
    def start_dt(self):
        return self.nc_files_dict['params']['beginDT']

    @property
    def end_dt(self):
        return self.nc_files_dict['params']['endDT']

    @property
    def zarr_exists(self):
        return self.__check_target()

    @property
    def repo(self):
        return f"https://github.com/ooi-data/{self.name}"

    @property
    def sources(self) -> List[dict]:
        """A list of source URLs containing the original data."""
        dataset_list = sorted(
            self.nc_files_dict['datasets'], key=lambda i: i['start_ts']
        )
        return dataset_list

    @property
    def targets(self) -> List[str]:
        """A list of target URLs where the transformed data is written."""
        return [self.nc_files_dict['final_bucket']]

    @property
    def environment(self):
        return None

    @property
    def storage(self):
        return None

    @property
    def flow(self):
        if not len(self.targets) == 1:
            raise ValueError(
                "Zarr target requires self.targets be a length one list"
            )

        with Flow(
            self.name, storage=self.storage, environment=self.environment
        ) as _flow:
            processing_task(
                self.sources,
                self.nc_files_dict,
                self.zarr_exists,
                self.refresh,
            )

        return _flow

    def __check_target(self):
        zpath, ext = os.path.splitext(self.nc_files_dict['final_bucket'])
        return self.fs.exists(os.path.join(zpath, '.zmetadata'))

    def _setup_pipeline(self):
        catalog_dict = parse_response_thredds(self.response)
        filtered_catalog_dict = filter_and_parse_datasets(catalog_dict)
        harvest_catalog = dict(**filtered_catalog_dict, **self.response)

        nc_files_dict = setup_etl(
            harvest_catalog, target_bucket=self.__existing_data_path
        )
        self.nc_files_dict = nc_files_dict
        self.name = nc_files_dict['stream']['table_name']

        self.fs = fsspec.get_mapper(
            self.nc_files_dict['final_bucket'],
            **get_storage_options(self.nc_files_dict['final_bucket']),
        ).fs
