import os
import datetime
from typing import List
import textwrap
import json

import fsspec

# from loguru import logger
import prefect
from prefect import Flow, task
from prefect.storage import Docker
from prefect.run_configs import KubernetesRun
from prefect.storage import Storage
from prefect.run_configs import RunConfig

from . import process_dataset, finalize_zarr
from ..core import AbstractPipeline
from ..utils.parser import (
    parse_response_thredds,
    filter_and_parse_datasets,
    get_storage_options,
    setup_etl,
)
import time


@task
def processing_task(
    dataset_list, nc_files_dict, zarr_exists, refresh, test_run=False
):
    logger = prefect.context.get("logger")
    name = nc_files_dict['stream']['table_name']
    logger.info(f"Processing {name}.")
    start_time = datetime.datetime.utcnow()
    if test_run:
        logger.info("RUNNING TEST RUN ... IDLING FOR 5 Seconds")
        logger.info(json.dumps(nc_files_dict))
        time.sleep(5)
        time_elapsed = datetime.datetime.utcnow() - start_time
        logger.info(f"DONE. Time elapsed: {str(time_elapsed)}")
    else:
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
        self,
        response,
        refresh=True,
        existing_data_path='s3://ooi-data',
        storage_options={},
        run_config_options={},
        test_run=False
    ):
        self.response = response
        self.refresh = refresh
        self.nc_files_dict = None
        self.__existing_data_path = existing_data_path
        self.fs = None

        self._flow = None
        self.__flow_so = storage_options
        self.__flow_rco = run_config_options
        self.__test_run = test_run
        # By default use Docker and Kubernetes for flows
        self._storage = Docker(**self.__flow_so)
        self._run_config = KubernetesRun(**self.__flow_rco)

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
    def storage(self):
        return self._storage
    
    @storage.setter
    def storage(self, s):
        if (s is not None) and (not isinstance(s, Storage)):
            raise TypeError(f"{type(s)} is not a valid storage type")

        self._storage = s
        if self._flow:
            self._flow.storage = self._storage


    @property
    def run_config(self):
        return self._run_config

    @run_config.setter
    def run_config(self, rc):
        if (rc is not None) and (not isinstance(rc, RunConfig)):
            raise TypeError(f"{type(rc)} is not a valid run configuration type")

        self._run_config = rc
        if self._flow:
            self._flow.storage = self._run_config

    @property
    def flow(self):
        if not len(self.targets) == 1:
            raise ValueError(
                "Zarr target requires self.targets be a length one list"
            )

        with Flow(
            self.name, storage=self.storage, run_config=self.run_config
        ) as _flow:
            processing_task(
                self.sources,
                self.nc_files_dict,
                self.zarr_exists,
                self.refresh,
                self.__test_run
            )

        self._flow = _flow
        return self._flow

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
