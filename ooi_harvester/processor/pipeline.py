import os
import datetime
from typing import List
import textwrap
import json

import fsspec
import zarr

# from loguru import logger
import prefect
from prefect import Flow, task
from prefect.storage import Docker
from prefect.run_configs import KubernetesRun
from prefect.storage import Storage
from prefect.run_configs import RunConfig
from prefect.engine import signals

from . import process_dataset, finalize_zarr
from .state_handlers import process_status_update
from ..core import AbstractPipeline
from ..utils.parser import (
    parse_response_thredds,
    filter_and_parse_datasets,
    get_storage_options,
    setup_etl,
)
import time

RUN_CONFIG_TYPES = {'kubernetes': KubernetesRun}

STORAGE_TYPES = {'docker': Docker}


# NOTE: How to pass in state_handlers for tasks?
@task()
def processing_task(
    dataset_list, nc_files_dict, zarr_exists, refresh, test_run=False
):
    name = nc_files_dict['stream']['table_name']
    try:
        logger = prefect.context.get("logger")
        logger.info(f"Processing {name}.")
        start_time = datetime.datetime.utcnow()
        final_message = ""
        if test_run:
            logger.info("RUNNING TEST RUN ... IDLING FOR 5 Seconds")
            logger.info(json.dumps(nc_files_dict))
            time.sleep(5)
            time_elapsed = datetime.datetime.utcnow() - start_time
            final_message = f"DONE. Time elapsed: {str(time_elapsed)}"
        else:
            # == Setup Local temp folder for netcdf ==========
            harvest_location = os.path.expanduser('~/.ooi-harvester')
            temp_fold = os.path.join(harvest_location, name)
            if not os.path.exists(os.path.dirname(temp_fold)):
                os.mkdir(os.path.dirname(temp_fold))
            if not os.path.exists(temp_fold):
                os.mkdir(temp_fold)
            nc_files_dict['temp_fold'] = temp_fold
            # =================================================

            for idx, d in enumerate(dataset_list):
                is_first = False
                if idx == 0 and refresh:
                    is_first = True
                else:
                    final_store = fsspec.get_mapper(
                        nc_files_dict['final_bucket'],
                        **get_storage_options(
                            nc_files_dict['final_bucket']
                        ),
                    )
                    temp_store = fsspec.get_mapper(
                        nc_files_dict['temp_bucket'],
                        **get_storage_options(
                            nc_files_dict['temp_bucket']
                        ),
                    )
                    if temp_store.fs.exists(nc_files_dict['temp_bucket']):
                        temp_store.fs.delete(
                            nc_files_dict['temp_bucket'], recursive=True
                        )
                    zarr.copy_store(final_store, temp_store)
                process_dataset(
                    d, nc_files_dict, is_first=is_first, logger=logger
                )
            logger.info(f"Finalizing data stream {name}.")
            final_path = finalize_zarr(
                source_zarr=nc_files_dict['temp_bucket'],
                final_zarr=nc_files_dict['final_bucket'],
            )
            time_elapsed = datetime.datetime.utcnow() - start_time
            final_message = (
                f"DONE. ({final_path}) Time elapsed: {str(time_elapsed)}"
            )
        logger.info(final_message)
    except Exception as exc:
        raise signals.FAIL(
            message=str(exc), result={'flow_name': name, 'exception': exc}
        )

    raise signals.SUCCESS(
        message=final_message,
        result={'flow_name': name, 'exception': None},
    )


class OOIStreamPipeline(AbstractPipeline):
    def __init__(
        self,
        response,
        refresh=True,
        existing_data_path='s3://ooi-data',
        run_config_type=None,
        storage_type=None,
        storage_options={},
        run_config_options={},
        test_run=False,
        state_handlers=[],
        goldcopy=False,
    ):
        self.response = response
        self.refresh = refresh
        self.goldcopy = goldcopy
        self.nc_files_dict = None
        self.__existing_data_path = existing_data_path
        self.fs = None

        self._flow = None
        self.__flow_so = storage_options
        self.__flow_rco = run_config_options
        self.__test_run = test_run

        self.__state_handlers = state_handlers

        # By default use Docker and Kubernetes for flows
        self._run_config = run_config_type
        self._storage = storage_type
        if run_config_type is not None:
            if run_config_type not in RUN_CONFIG_TYPES:
                UserWarning(
                    f"{run_config_type} currently not available. Defaulting to None"
                )
            else:
                self._run_config = RUN_CONFIG_TYPES[run_config_type](
                    **self.__flow_rco
                )

        if storage_type is not None:
            if storage_type not in STORAGE_TYPES:
                UserWarning(
                    f"{storage_type} currently not available. Defaulting to None"
                )
            else:
                self._storage = STORAGE_TYPES[storage_type](**self.__flow_so)

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
            raise TypeError(
                f"{type(rc)} is not a valid run configuration type"
            )

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
            self.name,
            storage=self.storage,
            run_config=self.run_config,
            state_handlers=self.__state_handlers,
        ) as _flow:
            processing_task(
                self.sources,
                self.nc_files_dict,
                self.zarr_exists,
                self.refresh,
                self.__test_run,
            )

        self._flow = _flow
        return self._flow

    def __check_target(self):
        zpath, ext = os.path.splitext(self.nc_files_dict['final_bucket'])
        return self.fs.exists(os.path.join(zpath, '.zmetadata'))

    def _setup_pipeline(self):
        if self.goldcopy:
            harvest_catalog = self.response
        else:
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
