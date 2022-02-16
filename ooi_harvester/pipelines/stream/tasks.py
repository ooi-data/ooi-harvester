import datetime
import xarray as xr
import dask
import time
import tempfile
import dateutil
import fsspec
import zarr
import numpy as np
import dask.array as da

import prefect
from prefect import task
from prefect.engine.signals import SKIP, FAIL, LOOP

from ooi_harvester.settings import harvest_settings
from ooi_harvester.producer import (
    StreamHarvest,
    fetch_streams_list,
    create_request_estimate,
    perform_request,
)
from ooi_harvester.processor.checker import check_in_progress
from ooi_harvester.processor import (
    _download,
    chunk_ds,
    preproc,
    state_handlers,
    update_metadata,
    is_zarr_ready,
    append_to_zarr,
    _update_time_coverage,
)
from ooi_harvester.processor.utils import _write_data_avail, _get_var_encoding
from ooi_harvester.processor.pipeline import _fetch_avail_dict
from ooi_harvester.utils.github import get_status_json
from ooi_harvester.utils.parser import (
    parse_response_thredds,
    filter_and_parse_datasets,
    setup_etl,
)


@task
def get_stream_harvest(config_json):
    return StreamHarvest(**config_json)


@task(max_retries=6, retry_delay=datetime.timedelta(minutes=10))
def setup_harvest(stream_harvest):
    logger = prefect.context.get('logger')
    logger.info("=== Setting up data request ===")
    table_name = stream_harvest.table_name
    streams_list = fetch_streams_list(stream_harvest)
    request_dt = datetime.datetime.utcnow().isoformat()
    try:
        stream_dct = next(
            filter(lambda s: s['table_name'] == table_name, streams_list)
        )
    except Exception:
        logger.warning("Stream not found in OOI Database.")
        message = f"{table_name} not found in OOI Database. It may be that this stream has been discontinued."
        status_json = get_status_json(table_name, request_dt, 'discontinued')
        raise SKIP(
            message=message, result={"status": status_json, "message": message}
        )

    if stream_harvest.harvest_options.goldcopy:
        status_json = get_status_json(table_name, request_dt, 'failed')
        message = "Gold Copy Harvest is not currently supported."
        logger.warning(message)
        raise SKIP(
            message=message,
            result={"status": status_json, "message": message},
        )
    else:
        estimated_request = create_request_estimate(
            stream_dct=stream_dct,
            start_dt=stream_harvest.harvest_options.custom_range.start,
            end_dt=stream_harvest.harvest_options.custom_range.end,
            refresh=stream_harvest.harvest_options.refresh,
            existing_data_path=stream_harvest.harvest_options.path,
            request_kwargs=dict(provenance=True),
        )
    estimated_request.setdefault("request_dt", request_dt)
    logger.info("Data Harvest has been setup successfully.")
    return estimated_request


# TODO: Create state handler that update to request.yaml
# TODO: Save request_response to response.json
@task
def request_data(estimated_request, stream_harvest):
    logger = prefect.context.get('logger')
    table_name = stream_harvest.table_name
    request_dt = estimated_request.get("request_dt")
    logger.info("=== Performing data request ===")
    if "requestUUID" in estimated_request['estimated']:
        logger.info("Continue to actual request ...")
        request_response = perform_request(
            estimated_request,
            refresh=stream_harvest.harvest_options.refresh,
            logger=logger,
            storage_options=stream_harvest.harvest_options.path_settings,
        )

        status_json = get_status_json(table_name, request_dt, 'pending')
    else:
        logger.info("Writing out status to failed ...")
        request_response = estimated_request
        status_json = get_status_json(table_name, request_dt, 'failed')
    return {'response': request_response, 'status': status_json}


# TODO: Create state handler that update to request.yaml each time check_data is run
@task(max_retries=6, retry_delay=datetime.timedelta(minutes=10))
def check_data(data_response):
    logger = prefect.context.get('logger')
    logger.info("=== Checking for data readiness ===")
    status_json = data_response.get("status")
    if status_json.get("status") == 'failed':
        raise SKIP("No data is available for harvesting.")
    else:
        response = data_response.get("response")
        result = response.get("result")
        status_url = result.get("status_url", None)
        if status_url is not None:
            in_progress = check_in_progress(status_url)
            if not in_progress:
                logger.info("Data available for download.")
                status_json.setdefault("status", "success")
                status_json.setdefault("data_ready", True)
                data_response.setdefault("status", status_json)
                return data_response
            else:
                time_since_request = (
                    datetime.datetime.utcnow()
                    - dateutil.parser.parse(response['result']['request_dt'])
                )
                if time_since_request >= datetime.timedelta(days=2):
                    catalog_dict = parse_response_thredds(response)
                    filtered_catalog_dict = filter_and_parse_datasets(
                        catalog_dict
                    )
                    if len(filtered_catalog_dict['datasets']) > 0:
                        logger.info(
                            "Data request timeout reached. But nc files are still available."
                        )
                        status_json.setdefault("status", "success")
                        status_json.setdefault("data_ready", True)
                        data_response.setdefault("status", status_json)
                        return data_response
                    else:
                        message = f"Data request timeout reached. Has been waiting for more than 2 days. ({str(time_since_request)})"
                        status_json.setdefault("status", "failed")
                        status_json.setdefault("data_ready", False)
                        data_response.setdefault("status", status_json)
                        raise FAIL(message, result=data_response)
                else:
                    logger.info(
                        f"Data request time elapsed: {str(time_since_request)}"
                    )
                    # Wait a minute after each request
                    time.sleep(60)
                    raise LOOP(
                        message="Data is not ready for download...",
                        result=data_response,
                    )


@task
def get_response(data_response):
    return data_response.get('response')


@task
def setup_process(response_json, target_bucket):
    logger = prefect.context.get('logger')
    logger.info(f"=== Setting up process ===")
    catalog_dict = parse_response_thredds(response_json)
    filtered_catalog_dict = filter_and_parse_datasets(catalog_dict)
    harvest_catalog = dict(**filtered_catalog_dict, **response_json)
    nc_files_dict = setup_etl(harvest_catalog, target_bucket=target_bucket)
    logger.info(
        f"{len(nc_files_dict.get('datasets', []))} netcdf files to be processed."
    )
    return nc_files_dict


@task
def data_processing(nc_files_dict, stream_harvest, max_chunk, error_test):
    logger = prefect.context.get("logger")
    stream = nc_files_dict.get("stream")
    name = stream.get("table_name")
    logger.info(f"=== Processing {name}. ===")
    dataset_list = sorted(
        nc_files_dict.get("datasets", []), key=lambda i: i.get('start_ts')
    )
    temp_zarr = nc_files_dict.get("temp_bucket")
    temp_store = fsspec.get_mapper(
        temp_zarr,
        **stream_harvest.harvest_options.path_settings,
    )

    existing_enc = None
    if not stream_harvest.harvest_options.refresh:
        final_zarr = nc_files_dict.get("final_bucket")
        final_store = fsspec.get_mapper(
            final_zarr,
            **stream_harvest.harvest_options.path_settings,
        )
        zg = zarr.open_consolidated(final_store)
        existing_enc = {k: _get_var_encoding(var) for k, var in zg.arrays()}

    if len(dataset_list) > 0:
        for idx, d in enumerate(dataset_list):
            is_first = False
            if idx == 0:
                is_first = True
                if error_test:
                    raise ValueError("Error test in progress! Not actual error found here!")
            logger.info(
                f"*** {name} ({d.get('deployment')}) | {d.get('start_ts')} - {d.get('end_ts')} ***"
            )
            try:
                with tempfile.TemporaryDirectory() as tmpdir:
                    source_url = '/'.join(
                        [nc_files_dict.get('async_url'), d.get('name')]
                    )
                    # Download the netcdf files and read to a xarray dataset obj
                    ncpath = _download(
                        source_url=source_url,
                        cache_location=tmpdir,
                    )
                    logger.info(f"Downloaded: {ncpath}")
                    with dask.config.set(scheduler="single-threaded"):
                        ds = (
                            xr.open_dataset(
                                ncpath, engine='netcdf4', decode_times=False
                            )
                            .pipe(preproc)
                            .pipe(
                                update_metadata,
                                nc_files_dict.get('retrieved_dt'),
                            )
                        )
                        logger.info(f"Finished preprocessing dataset.")

                        # Chunk dataset and write to zarr
                        if isinstance(ds, xr.Dataset):
                            mod_ds, enc = chunk_ds(
                                ds,
                                max_chunk=max_chunk,
                                existing_enc=existing_enc,
                            )
                            logger.info(f"Finished chunking dataset.")

                            if is_first:
                                # TODO: Like the _prepare_ds_to_append need to check on the dims and len for all variables
                                mod_ds.to_zarr(
                                    temp_store,
                                    consolidated=True,
                                    compute=True,
                                    mode='w',
                                    encoding=enc,
                                )
                                succeed = True
                            else:
                                succeed = append_to_zarr(
                                    mod_ds, temp_store, enc, logger=logger
                                )

                            if succeed:
                                is_done = False
                                while not is_done:
                                    store = fsspec.get_mapper(
                                        temp_zarr,
                                        **stream_harvest.harvest_options.path_settings,
                                    )
                                    is_done = is_zarr_ready(store)
                                    if is_done:
                                        continue
                                    time.sleep(5)
                                    logger.info(
                                        "Waiting for zarr file writing to finish..."
                                    )
                                logger.info(
                                    f"SUCCESS: File successfully written to zarr."
                                )
                            else:
                                logger.warning(
                                    f"SKIPPED: Issues in file found for {d.get('name')}!"
                                )
                        else:
                            logger.warning("SKIPPED: Failed pre processing!")
            except Exception as e:
                raise FAIL(message=str(e))
    else:
        raise SKIP("No datasets to process. Skipping...")
    return {
        "final_path": nc_files_dict.get("final_bucket"),
        "temp_path": nc_files_dict.get("temp_bucket"),
    }


# TODO: Create handler to process.yaml
@task
def finalize_data_stream(stores_dict, stream_harvest, max_chunk):
    logger = prefect.context.get("logger")
    logger.info("=== Finalizing data stream. ===")
    final_store = fsspec.get_mapper(
        stores_dict.get("final_path"),
        **stream_harvest.harvest_options.path_settings,
    )
    temp_store = fsspec.get_mapper(
        stores_dict.get("temp_path"),
        **stream_harvest.harvest_options.path_settings,
    )
    if stream_harvest.harvest_options.refresh:
        # Remove missing groups in the final store
        temp_group = zarr.open_consolidated(temp_store)
        final_group = zarr.open_group(final_store, mode='a')
        final_modified = False
        for k, _ in final_group.items():
            if k not in list(temp_group.array_keys()):
                final_group.pop(k)
                final_modified = True

        if final_modified:
            zarr.consolidate_metadata(final_store)

        # Copy over the store, at this point, they should be similar
        zarr.copy_store(temp_store, final_store, if_exists='replace')
    else:
        temp_ds = xr.open_dataset(
            temp_store,
            engine='zarr',
            backend_kwargs={'consolidated': True},
            decode_times=False,
        )
        mod_ds, _ = chunk_ds(temp_ds, max_chunk=max_chunk)
        mod_ds.to_zarr(
            final_store,
            consolidated=True,
            compute=True,
            mode='a',
            append_dim='time',
        )
        # append_to_zarr(mod_ds, final_store, enc, logger=logger)

    # Update start and end date in global attributes
    _update_time_coverage(final_store)

    # Clean up temp_store
    temp_store.clear()
    logger.info(f"Data stream finalized: {stores_dict.get('final_path')}")
    return stores_dict.get("final_path")


@task
def data_availability(
    nc_files_dict, stream_harvest, export=False, gh_write=False
):
    name = nc_files_dict['stream']['table_name']
    inst_rd = nc_files_dict['stream']['reference_designator']
    stream_rd = '-'.join(
        [nc_files_dict['stream']['method'], nc_files_dict['stream']['stream']]
    )
    logger = prefect.context.get("logger")
    logger.info(f"Data availability for {name}.")

    url = nc_files_dict['final_bucket']
    mapper = fsspec.get_mapper(
        url, **stream_harvest.harvest_options.path_settings
    )
    try:
        za = zarr.open_consolidated(mapper)['time']
        calendar = za.attrs.get(
            'calendar', harvest_settings.ooi_config.time['calendar']
        )
        units = za.attrs.get(
            'units', harvest_settings.ooi_config.time['units']
        )

        if any(np.isnan(za)):
            logger.info(f"Null values found. Skipping {name}")
        else:
            logger.info(
                f"Total time bytes: {dask.utils.memory_repr(za.nbytes)}"
            )
            darr = da.from_zarr(za)

            darr_dt = darr.map_blocks(
                xr.coding.times.decode_cf_datetime,
                units=units,
                calendar=calendar,
            )

            ddf = darr_dt.to_dask_dataframe(['dtindex']).set_index('dtindex')
            ddf['count'] = 0

            resolutions = {'hourly': 'H', 'daily': 'D', 'monthly': 'M'}

            avail_dict = {
                'data_stream': stream_rd,
                'inst_rd': inst_rd,
                'results': {
                    k: _fetch_avail_dict(ddf, resolution=v)
                    for k, v in resolutions.items()
                },
            }
            if export:
                _write_data_avail(avail_dict, gh_write=gh_write)

            return avail_dict
    except Exception as e:
        logger.info(f"Error found {e}. Skipping {name}")
