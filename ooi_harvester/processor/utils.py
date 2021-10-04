import os
import zarr
import numpy as np
import xarray as xr
from loguru import logger
import json
import fsspec
from pathlib import Path
from github import Github

from ..utils.encoders import NumpyEncoder
from ooi_harvester.settings import harvest_settings


def _write_data_avail(avail_dict, gh_write=False):
    for k, v in avail_dict['results'].items():
        json_path = Path(k, avail_dict['inst_rd'])
        stream_content = {avail_dict['data_stream']: v}
        if gh_write:
            json_path = str(json_path)
            try:
                gh = Github(harvest_settings.github.pat)
                repo = gh.get_repo(
                    os.path.join(
                        harvest_settings.github.data_org, 'data_availability'
                    )
                )
                contents = repo.get_contents(json_path, ref=harvest_settings.github.main_branch)

                json_content = json.loads(contents.decoded_content)
                if avail_dict['data_stream'] in json_content:
                    json_content.update(stream_content)
                else:
                    json_content = dict(**json_content, **stream_content)

                repo.update_file(
                    path=contents.path,
                    message=f"⬆️ Updated {k} data availability for {avail_dict['inst_rd']}",
                    content=json.dumps(json_content, cls=NumpyEncoder),
                    sha=contents.sha,
                    branch=harvest_settings.github.main_branch,
                )
            except Exception as e:
                json_content = stream_content
                response = e.args[1]
                if response['message'] == 'Not Found':
                    repo.create_file(
                        json_path,
                        f"✨ Created {k} data availability for {avail_dict['inst_rd']}",
                        json.dumps(json_content, cls=NumpyEncoder),
                        branch=harvest_settings.github.main_branch,
                    )
        else:
            if not json_path.parent.exists():
                json_path.parent.mkdir(exist_ok=True)

            if json_path.exists():
                json_content = json.loads(
                    json_path.read_text(encoding='utf-8')
                )
                if avail_dict['data_stream'] in json_content:
                    json_content.update(stream_content)
                else:
                    json_content = dict(**json_content, **stream_content)
            else:
                json_content = stream_content

            json_path.write_text(json.dumps(json_content, cls=NumpyEncoder))


def _validate_dims(ds_to_append, existing_zarr, append_dim):
    dim_indexer = {}
    modify_zarr_dims = False
    for dim, new_size in ds_to_append.sizes.items():
        if 'time' not in dim:
            existing_var = existing_zarr[dim]
            existing_size = existing_var.shape[0]
            if new_size < existing_size:
                dim_indexer[dim] = existing_var[:].astype(
                    ds_to_append[dim].dtype
                )
            elif new_size > existing_size:
                dim_indexer[dim] = ds_to_append[dim].values
                modify_zarr_dims = True
    return dim_indexer, modify_zarr_dims


def _prepare_existing_zarr(store, ds_to_append, enc):
    existing_zarr = zarr.open_group(store, mode='a')
    for var_name, new_var in ds_to_append.variables.items():
        if var_name not in existing_zarr:
            logger.info(f"{var_name} not in existing zarr ... creating ...")
            existing_arr_shape = tuple(
                existing_zarr[dim].shape[0]
                for dim, size in new_var.sizes.items()
            )
            existing_chunks = tuple(
                existing_zarr[dim].chunks[0]
                for dim, size in new_var.sizes.items()
            )
            fill_value = None
            if '_FillValue' in enc[var_name]:
                fill_value = enc[var_name]['_FillValue']

            za = existing_zarr.create(
                var_name,
                shape=existing_arr_shape,
                chunks=existing_chunks,
                dtype=enc[var_name]['dtype'],
                fill_value=fill_value,
                compressor=enc[var_name]['compressor'],
            )

            attributes = json.loads(
                json.dumps(new_var.attrs, cls=NumpyEncoder)
            )
            attributes['_ARRAY_DIMENSIONS'] = list(new_var.dims)

            za.attrs.put(attributes)
            logger.info(f"{var_name} creation finished.")
    zarr.consolidate_metadata(store)
    return existing_zarr


def _prepare_ds_to_append(store, ds_to_append):
    from xarray.backends.zarr import ZarrStore

    existing_zarr = zarr.open_group(store, mode='a')
    zs = ZarrStore(existing_zarr)
    ds_to_append = ds_to_append.unify_chunks()

    for var_name, new_var in zs.get_variables().items():
        existing_shape = tuple(
            ds_to_append[dim].shape[0] for dim, size in new_var.sizes.items()
        )
        existing_chunks = {
            dim: ds_to_append.chunks[dim] for dim in new_var.dims
        }
        if var_name not in ds_to_append:
            logger.info(f"{var_name} not in ds_to_append ... creating ...")
            new_arr = np.zeros(existing_shape, dtype=new_var.dtype)
            new_arr.fill(
                new_var.attrs['_FillValue']
                if '_FillValue' in new_var.attrs
                else None
            )

            ds_to_append[var_name] = xr.Variable(
                dims=new_var.dims,
                data=new_arr,
                attrs=new_var.attrs,
                encoding=new_var.encoding,
            ).chunk(existing_chunks)
        else:
            var_to_change = ds_to_append[var_name]
            if not (var_to_change.dims == new_var.dims) or (
                var_to_change.shape != existing_shape
            ):
                logger.info(
                    f"{var_name} is not aligned with existing variable ... modifying ..."
                )
                new_arr = np.zeros(existing_shape, dtype=new_var.dtype)
                new_arr.fill(
                    new_var.attrs['_FillValue']
                    if '_FillValue' in new_var.attrs
                    else None
                )
                ds_to_append[var_name] = xr.Variable(
                    dims=new_var.dims,
                    data=new_arr,
                    attrs=new_var.attrs,
                    encoding=new_var.encoding,
                ).chunk(existing_chunks)
    return ds_to_append


def _append_zarr(store, ds_to_append, append_dim='time'):
    existing_zarr = zarr.open_group(store, mode='a')

    # Remove append_dim duplicates by checking for existing tail
    if existing_zarr[append_dim][-1] == ds_to_append[append_dim].data[0]:
        ds_to_append = ds_to_append.drop_isel({append_dim: 0})

    for var_name, var_data in ds_to_append.variables.items():
        if any([append_dim in dim for dim in var_data.dims]):
            existing_arr = existing_zarr[var_name]
            existing_arr.append(var_data.values)
    zarr.consolidate_metadata(store)


def _reindex_zarr(store, dim_indexer):
    existing_zarr = zarr.open_group(store, mode='a')
    new_dim_sizes = {k: len(v) for k, v in dim_indexer.items()}
    for arr_name, za in existing_zarr.arrays():
        arr_dims = za.attrs['_ARRAY_DIMENSIONS']
        existing_dims = dict(zip(arr_dims, za.shape))
        new_shape = tuple(
            new_dim_sizes[d] if d in new_dim_sizes else existing_dims[d]
            for d in arr_dims
        )
        if arr_name in new_dim_sizes:
            za = existing_zarr[arr_name]
            za.resize(new_shape)
            za[:] = dim_indexer[arr_name]
        elif any(dim in dim_indexer for dim in arr_dims):
            za = existing_zarr[arr_name]
            za.resize(new_shape)
    zarr.consolidate_metadata(store)
    return existing_zarr


def _download(source_url: str, cache_location: str) -> str:
    """
    Download a remote file to a cache.
    Parameters
    ----------
    source_url : str
        Path or url to the source file.
    cache_location : str
        Path or url to the target location for the source file.
    Returns
    -------
    target_url : str
        Path or url in the form of `{cache_location}/hash({source_url})`.
    """
    fs = fsspec.get_mapper(cache_location).fs

    target_url = os.path.join(cache_location, os.path.basename(source_url))

    # there is probably a better way to do caching!
    try:
        fs.open(target_url)
        return target_url
    except FileNotFoundError:
        pass

    with fsspec.open(source_url, mode="rb") as source:
        with fs.open(target_url, mode="wb") as target:
            target.write(source.read())
    return target_url
