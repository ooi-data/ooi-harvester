import datetime
import math
import time
import os
from pathlib import Path

import zarr
import numpy as np
from loguru import logger
import fsspec
import dask
import xarray as xr
from xarray import coding

from rechunker.algorithm import prod
from rechunker import rechunk
from dask.diagnostics import ProgressBar

from .utils import (
    _reindex_zarr,
    _append_zarr,
    _prepare_existing_zarr,
    _prepare_ds_to_append,
    _validate_dims,
    _download,
)

from ..utils.parser import get_storage_options


def _update_time_coverage(store: fsspec.mapping.FSMap) -> None:
    """Updates start and end date in global attributes"""
    zg = zarr.open_group(store, mode='r+')
    calendar = zg.time.attrs.get('calendar', 'gregorian')
    units = zg.time.attrs.get('units', 'seconds since 1900-01-01 0:0:0')
    start, end = xr.coding.times.decode_cf_datetime(
        [zg.time[0], zg.time[-1]], units=units, calendar=calendar
    )
    zg.attrs['time_coverage_start'] = str(start)
    zg.attrs['time_coverage_end'] = str(end)
    zarr.consolidate_metadata(store)
    return str(start), str(end)


def finalize_data_stream(
    nc_files_dict: dict, client_kwargs: dict, refresh: bool = True
) -> str:
    """
    Finalizes data stream by copying over zarr file
    from temp bucket to final bucket.
    """
    final_path = nc_files_dict['final_bucket']
    final_store = fsspec.get_mapper(
        nc_files_dict['final_bucket'],
        client_kwargs=client_kwargs,
        **get_storage_options(nc_files_dict['final_bucket']),
    )
    temp_store = fsspec.get_mapper(
        nc_files_dict['temp_bucket'],
        client_kwargs=client_kwargs,
        **get_storage_options(nc_files_dict['temp_bucket']),
    )
    if refresh:
        # Remove missing groups in the final store
        temp_group = zarr.open_consolidated(temp_store)
        final_group = zarr.open_group(final_store, mode='r+')
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
        mod_ds, enc = chunk_ds(temp_ds, max_chunk='100MB')
        append_to_zarr(mod_ds, final_store, enc, logger=logger)

    # Update start and end date in global attributes
    _update_time_coverage(final_store)

    # Clean up temp_store
    temp_store.clear()

    return final_path


def get_logger():
    from loguru import logger

    return logger


def process_dataset(
    d, nc_files_dict, is_first=True, logger=None, client_kwargs={}
):
    if logger is None:
        logger = get_logger()
    name = nc_files_dict['stream']['table_name']
    dataset_name = d['name']
    zarr_file = nc_files_dict['temp_bucket']
    store = fsspec.get_mapper(
        zarr_file,
        client_kwargs=client_kwargs,
        **get_storage_options(zarr_file),
    )

    logger.info(
        f"*** {name} ({d['deployment']}) | {d['start_ts']} - {d['end_ts']} ***"
    )
    downloaded = False
    ds = None
    ncpath = ''
    while not downloaded:
        try:
            ncpath = _download(
                source_url=os.path.join(
                    nc_files_dict['async_url'], dataset_name
                ),
                cache_location=nc_files_dict['temp_fold'],
            )
            with dask.config.set(scheduler="single-threaded"):
                ds = (
                    xr.open_dataset(
                        ncpath, engine='netcdf4', decode_times=False
                    )
                    .pipe(preproc)
                    .pipe(update_metadata, nc_files_dict['retrieved_dt'])
                )
                if isinstance(ds, xr.Dataset):
                    downloaded = True
        except Exception as e:
            logger.warning(e)

    if isinstance(ds, xr.Dataset):
        mod_ds, enc = chunk_ds(ds, max_chunk='100MB')

        if is_first:
            # TODO: Like the _prepare_ds_to_append need to check on the dims and len for all variables
            mod_ds.to_zarr(
                store,
                consolidated=True,
                compute=True,
                mode='w',
                encoding=enc,
            )
            succeed = True
        else:
            succeed = append_to_zarr(mod_ds, store, enc, logger=logger)

        if succeed:
            is_done = False
            while not is_done:
                store = fsspec.get_mapper(
                    zarr_file,
                    client_kwargs=client_kwargs,
                    **get_storage_options(zarr_file),
                )
                is_done = is_zarr_ready(store)
                if is_done:
                    continue
                time.sleep(5)
                logger.info("Waiting for zarr file writing to finish...")
        else:
            logger.warning(
                f"SKIPPED: Issues in file found for {d.get('name')}!"
            )

        delete_netcdf(ncpath, logger=logger)
    else:
        logger.warning("Failed pre processing ... Skipping ...")
        delete_netcdf(ncpath, logger=logger)


def is_zarr_ready(store):
    meta = store.get('.zmetadata')
    if meta is None:
        return False
    return True


def preproc(ds):
    if 'obs' in ds.dims:
        rawds = ds.swap_dims({'obs': 'time'}).reset_coords(drop=True)
    else:
        rawds = ds
    for v in rawds.variables:
        var = rawds[v]
        if (
            not np.issubdtype(var.dtype, np.number)
            and not np.issubdtype(var.dtype, np.datetime64)
            and not np.issubdtype(var.dtype, np.bool_)
        ):
            if (
                not coding.strings.is_unicode_dtype(var.dtype)
                or var.dtype == object
            ):
                rawds[v] = var.astype(str)
    return rawds


def _tens_counts(num: int, places: int = 2) -> int:
    return (math.floor(math.log10(abs(num))) + 1) - places


def _round_up(to_round: int) -> int:
    """Rounds up integers to whole zeros"""
    digit_round = 10 ** _tens_counts(to_round)
    if to_round % digit_round == 0:
        return to_round
    return (digit_round - to_round % digit_round) + to_round


def _round_down(to_round: int) -> int:
    """Rounds down integers to whole zeros"""
    digit_round = 10 ** _tens_counts(to_round)
    return to_round - to_round % digit_round


def _calc_chunks(variable: xr.DataArray, max_chunk='100MB'):
    """Dynamically figure out chunk based on max chunk size"""
    max_chunk_size = dask.utils.parse_bytes(max_chunk)
    dim_shape = {
        x: y for x, y in zip(variable.dims, variable.shape) if x != 'time'
    }
    if 'time' in variable.dims:
        time_chunk = math.ceil(
            max_chunk_size / prod(dim_shape.values()) / variable.dtype.itemsize
        )
        dim_shape['time'] = _round_down(time_chunk)
    chunks = tuple(dim_shape[d] for d in list(variable.dims))
    return chunks


def chunk_ds(
    chunked_ds,
    max_chunk='100MB',
    time_max_chunks='100MB',
    existing_enc=None,
    apply=True,
):
    compress = zarr.Blosc(cname="zstd", clevel=3, shuffle=2)

    if existing_enc is None:
        raw_enc = {}
        for k, v in chunked_ds.data_vars.items():
            chunks = _calc_chunks(v, max_chunk=max_chunk)

            # add extra encodings
            extra_enc = {}
            if "_FillValue" in v.encoding:
                extra_enc["_FillValue"] = v.encoding["_FillValue"]
            raw_enc[k] = dict(
                compressor=compress, dtype=v.dtype, chunks=chunks, **extra_enc
            )

        if 'time' in chunked_ds:
            chunks = _calc_chunks(
                chunked_ds['time'], max_chunk=time_max_chunks
            )
            raw_enc["time"] = {"chunks": chunks}
    elif isinstance(existing_enc, dict):
        raw_enc = existing_enc
    else:
        raise ValueError("existing encoding only accepts dictionary!")

    # Chunk the actual xr dataset
    if apply:
        for k, v in chunked_ds.variables.items():
            encoding = raw_enc.get(k, None)
            var_chunks = {}
            if isinstance(encoding, dict):
                var_chunks = encoding['chunks']

            chunked_ds[k] = chunked_ds[k].chunk(chunks=var_chunks)

    return chunked_ds, raw_enc


def update_metadata(dstime, download_date, unit=None, extra_attrs={}):
    """Updates the dataset metadata to be more cf compliant, and add CAVA info"""

    for v in dstime.variables:
        var = dstime[v]
        att = var.attrs

        for i, j in att.items():
            if isinstance(j, list) or isinstance(j, np.ndarray):
                att.update({i: ','.join(map(str, j))})
        var.attrs.update(att)

        # unit modifier
        if 'units' in var.attrs:
            units = var.attrs['units']
            if isinstance(units, np.ndarray):
                unit = ','.join(units)
            else:
                unit = units

            # Fix celcius to correct cf unit
            if unit == "ºC":
                var.attrs["units"] = "degree_C"
            else:
                var.attrs["units"] = unit

        # ancillary variables modifier
        if "ancillary_variables" in var.attrs:
            ancillary = var.attrs["ancillary_variables"]
            anc_vars = " ".join(ancillary.split(","))
            var.attrs["ancillary_variables"] = anc_vars

        # long name modifier
        if "long_name" not in var.attrs:
            if "qc_executed" in v:
                var.attrs["long_name"] = "QC Checks Executed"
            if "qc_results" in v:
                var.attrs["long_name"] = "QC Checks Results"
            if v == "lat":
                var.attrs["long_name"] = "Location Latitude"
            if v == "lon":
                var.attrs["long_name"] = "Location Longitude"
            if v == "obs":
                var.attrs["long_name"] = "Observation"
            if v == "deployment":
                var.attrs["long_name"] = "Deployment Number"
            if v == "id":
                var.attrs["long_name"] = "Observation unique id"

    # Change preferred_timestamp data type
    # if "preferred_timestamp" in dstime.variables:
    #     dstime["preferred_timestamp"] = dstime["preferred_timestamp"].astype(
    #         "|S36"
    #     )

    dstime.attrs[
        "comment"
    ] = "Some of the metadata of this dataset has been modified to be CF-1.6 compliant."
    dstime.attrs[
        "Notes"
    ] = "This netCDF product is a copy of the data on the University of Washington AWS Cloud Infrastructure."  # noqa
    dstime.attrs[
        "Owner"
    ] = "University of Washington Cabled Array Value Add Team."  # noqa
    dstime.attrs["date_downloaded"] = download_date
    dstime.attrs["date_processed"] = datetime.datetime.now().isoformat()

    # Add extra global attributes last!
    if extra_attrs:
        for k, v in extra_attrs.items():
            dstime.attrs[k] = v

    # Clean up metadata
    dstime = _meta_cleanup(dstime)

    return dstime


def _meta_cleanup(chunked_ds):
    if "time_coverage_resolution" in chunked_ds.attrs:
        del chunked_ds.attrs["time_coverage_resolution"]
    if "uuid" in chunked_ds.attrs:
        del chunked_ds.attrs["uuid"]
    if "creator_email" in chunked_ds.attrs:
        del chunked_ds.attrs["creator_email"]
    if "contributor_name" in chunked_ds.attrs:
        del chunked_ds.attrs["contributor_name"]
    if "contributor_role" in chunked_ds.attrs:
        del chunked_ds.attrs["contributor_role"]
    if "acknowledgement" in chunked_ds.attrs:
        del chunked_ds.attrs["acknowledgement"]
    if "requestUUID" in chunked_ds.attrs:
        del chunked_ds.attrs["requestUUID"]
    if "feature_Type" in chunked_ds.attrs:
        del chunked_ds.attrs["feature_Type"]

    return chunked_ds


def get_encoding(ds):
    encoding = {key: value.encoding for key, value in ds.variables.items()}
    return encoding


def append_to_zarr(mod_ds, store, encoding, logger=None):
    if logger is None:
        logger = get_logger()
    existing_zarr = zarr.open_group(store, mode='a')
    existing_var_count = len(list(existing_zarr.array_keys()))
    to_append_var_count = len(mod_ds.variables)

    if existing_var_count < to_append_var_count:
        existing_zarr = _prepare_existing_zarr(store, mod_ds, enc=encoding)
    else:
        mod_ds = _prepare_ds_to_append(store, mod_ds)

    dim_indexer, modify_zarr_dims, issue_dims = _validate_dims(
        mod_ds, existing_zarr, append_dim='time'
    )

    if len(issue_dims) > 0:
        logger.warning(
            f"{','.join(issue_dims)} dimension(s) are problematic. Skipping append..."
        )
        return False

    if modify_zarr_dims:
        logger.info("Reindexing zarr ...")
        existing_zarr = _reindex_zarr(store, dim_indexer)
    elif dim_indexer and not modify_zarr_dims:
        logger.info("Reindexing dataset to append ...")
        mod_ds = mod_ds.reindex(dim_indexer)

    # Remove append_dim duplicates by checking for existing tail
    append_dim = 'time'
    if existing_zarr[append_dim][-1] == mod_ds[append_dim].data[0]:
        mod_ds = mod_ds.drop_isel({append_dim: 0})

    mod_ds.to_zarr(
        store,
        consolidated=True,
        compute=True,
        mode='a',
        append_dim=append_dim,
        safe_chunks=False,
    )

    return True


def delete_netcdf(ncpath: str, logger=None) -> None:
    if logger is None:
        logger = get_logger()
    source_path = Path(ncpath)
    deletion = False
    if source_path.is_file():
        source_path.unlink()
        deletion = True

    if deletion:
        # Check for delete
        if not source_path.is_file():
            logger.info(f"{ncpath} successfully cleaned up.")


def delete_dataset(ds):
    source_file = ds.encoding['source']
    if os.path.exists(source_file):
        os.unlink(source_file)


def finalize_zarr(
    source_zarr,
    final_zarr,
    time_chunk=1209600,
    max_mem='2GB',
    client_kwargs={},
):
    storage_options = get_storage_options(source_zarr)
    source_store = fsspec.get_mapper(
        source_zarr, client_kwargs=client_kwargs, **storage_options
    )
    max_byte_size = dask.utils.parse_bytes(max_mem)

    zpath, ext = os.path.splitext(source_zarr)
    temp_path = f"{zpath}__tmp{ext}"
    target_path = final_zarr
    target_store = fsspec.get_mapper(
        target_path, client_kwargs=client_kwargs, **storage_options
    )
    temp_store = fsspec.get_mapper(
        temp_path, client_kwargs=client_kwargs, **storage_options
    )

    source_group = zarr.open_group(source_store)

    group_chunk_target = {}
    for k, za in source_group.arrays():
        arr_dims = za.attrs['_ARRAY_DIMENSIONS']
        # MAYBE USE RECHUNKER ALGORITHM TO DETERMINE CHUNKSIZE?
        # from rechunker.algorithm import consolidate_chunks
        # import dask
        # max_mem_int = dask.utils.parse_bytes(max_mem)
        # new_chunks = consolidate_chunks(za.shape, za.chunks, za.dtype.itemsize, max_mem_int)
        # new_chunks_dict = dict(zip(arr_dims, new_chunks))
        # group_chunk_target[k] = new_chunks_dict
        # =================================================
        if k not in arr_dims:
            target_chunks = {}
            for dim in arr_dims:
                dim_len = source_group[dim].shape[0]
                if 'time' in dim and dim_len > time_chunk:
                    dim_len = time_chunk

                target_chunks[dim] = dim_len
            group_chunk_target[k] = target_chunks
        else:
            dim_len = za.shape[0]
            if k != 'time':
                total_bytes = za.dtype.itemsize * prod(za.shape)
                if total_bytes > max_byte_size:
                    dim_len = int(max_byte_size / za.dtype.itemsize)
            else:
                if dim_len > time_chunk:
                    dim_len = time_chunk
            group_chunk_target[k] = {k: dim_len}

    options = {
        arr_name: {'overwrite': True} for arr_name in group_chunk_target.keys()
    }
    with ProgressBar():
        array_plan = rechunk(
            source_group,
            group_chunk_target,
            max_mem,
            target_store,
            temp_store=temp_store,
            target_options=options,
            temp_options=options,
        )
        array_plan.execute()

    zarr.consolidate_metadata(target_store)
    # Delete temp store
    temp_store.fs.delete(temp_store.root, recursive=True)
    # Delete source store
    source_store.fs.delete(source_store.root, recursive=True)

    return target_path
