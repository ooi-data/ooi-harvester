import datetime
import math
import os

import zarr
import numpy as np
from loguru import logger
import fsspec
import dask
import xarray as xr

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


def process_dataset(d, nc_files_dict, is_first=True):
    name = nc_files_dict['stream']['table_name']
    dataset_name = d['name']
    zarr_file = nc_files_dict['temp_bucket']
    store = fsspec.get_mapper(zarr_file, **get_storage_options(zarr_file))

    logger.info(
        f"*** {name} ({d['deployment']}) | {d['start_ts']} - {d['end_ts']} ***"
    )
    downloaded = False
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

    mod_ds, enc = chunk_ds(ds)

    if is_first:
        # TODO: Like the _prepare_ds_to_append need to check on the dims and len for all variables
        mod_ds.to_zarr(
            store, consolidated=True, compute=True, mode='w', encoding=enc
        )
    else:
        append_to_zarr(mod_ds, store, enc)

    delete_dataset(mod_ds)


def preproc(ds):
    if 'obs' in ds.dims:
        rawds = ds.swap_dims({'obs': 'time'}).reset_coords(drop=True)
    else:
        rawds = ds
    for v in rawds.variables:
        if rawds[v].dtype in ['object', '|S36']:
            rawds[v] = rawds[v].astype(str)
    return rawds


def chunk_ds(chunked_ds, chunk=13106200):
    compress = zarr.Blosc(cname="zstd", clevel=3, shuffle=2)

    time_chunk_list = []
    raw_enc = {}
    for k, v in chunked_ds.data_vars.items():
        mb_size = v.nbytes / 1024 ** 2
        dim_shape = {x: y for x, y in zip(v.dims, v.shape)}

        # dynamically figure out chunk
        if mb_size < 100:
            time_chunk = math.ceil(100 / mb_size)
        else:
            divider = math.floor(mb_size / 100)
            time_chunk = dim_shape["time"] / divider

        if "time" in dim_shape:
            dim_shape["time"] = time_chunk
            time_chunk_list.append(time_chunk)

        # add extra encodings
        extra_enc = {}
        if "_FillValue" in v.encoding:
            extra_enc["_FillValue"] = v.encoding["_FillValue"]
        raw_enc[k] = dict(compressor=compress, dtype=v.dtype, **extra_enc)
    raw_enc["time"] = {"chunks": (chunk,)}

    chunked_ds.encoding.update({"unlimited_dims": {"time"}})
    return chunked_ds.chunk({"time": chunk}), raw_enc


def update_metadata(dstime, download_date, unit=None, extra_attrs={}):
    """ Updates the dataset metadata to be more cf compliant, and add CAVA info """

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
    if "preferred_timestamp" in dstime.variables:
        dstime["preferred_timestamp"] = dstime["preferred_timestamp"].astype(
            "|S36"
        )

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


def append_to_zarr(mod_ds, store, encoding):
    existing_zarr = zarr.open_group(store, mode='a')
    existing_var_count = len([a for a in existing_zarr.array_keys()])
    to_append_var_count = len(mod_ds.variables)

    if existing_var_count < to_append_var_count:
        existing_zarr = _prepare_existing_zarr(store, mod_ds, enc=encoding)
    else:
        mod_ds = _prepare_ds_to_append(store, mod_ds)

    dim_indexer, modify_zarr_dims = _validate_dims(
        mod_ds, existing_zarr, append_dim='time'
    )

    if modify_zarr_dims:
        logger.info("Reindexing zarr ...")
        existing_zarr = _reindex_zarr(store, dim_indexer)
    elif dim_indexer and not modify_zarr_dims:
        logger.info("Reindexing dataset to append ...")
        mod_ds = mod_ds.reindex(dim_indexer)

    _append_zarr(store, mod_ds)


def delete_dataset(ds):
    source_file = ds.encoding['source']
    if os.path.exists(source_file):
        os.unlink(source_file)


def finalize_zarr(source_zarr, final_zarr, time_chunk=13106200, max_mem='2GB'):
    storage_options = get_storage_options(source_zarr)
    source_store = fsspec.get_mapper(source_zarr, **storage_options)
    max_byte_size = dask.utils.parse_bytes(max_mem)

    zpath, ext = os.path.splitext(source_zarr)
    temp_path = f"{zpath}__tmp{ext}"
    target_path = final_zarr
    target_store = fsspec.get_mapper(target_path, **storage_options)
    temp_store = fsspec.get_mapper(temp_path, **storage_options)

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
            total_bytes = za.dtype.itemsize * prod(za.shape)
            if total_bytes > max_byte_size:
                dim_len = int(max_byte_size / za.dtype.itemsize)
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
