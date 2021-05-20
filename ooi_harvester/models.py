import copy
import itertools as it
from dateutil import parser
import fsspec
import zarr
import numpy as np

from dask.utils import memory_repr
import dask.array as da

import xarray as xr
from xarray.core.utils import either_dict_or_kwargs


class OOIDataset:
    def __init__(
        self,
        dataset_id,
        bucket_name="ooi-data",
        storage_options={'anon': True},
    ):
        self.dataset_id = dataset_id
        self.bucket_name = bucket_name
        self.storage_options = storage_options
        self.dimensions = []
        self.variables = {}
        self.global_attributes = {}
        self.dataset = None

        self._total_size = None
        self._zarr_group = None
        self._dataset_dict = {"variables": {}, "dims": []}

        # Private attributes
        self.__default_time_units = 'seconds since 1900-01-01 00:00:00'
        self.__default_calendar = 'gregorian'
        self.__time_filter = []

        self._open_zarr()
        self._parse_zarr_group()
        self._set_variables()

    def __repr__(self):
        text_arr = [f"<{self.dataset_id}: {self._total_size}>"]
        text_arr.append(f"Dimensions: ({', '.join(self.dimensions)})")
        variables_arr = "\n    ".join([name for name in self.variables.keys()])
        text_arr.append(f"Data variables: \n    {variables_arr}")
        return "\n".join(text_arr)

    def __getitem__(self, item):
        new_self = copy.deepcopy(self)
        new_variables = {}
        for k, v in new_self.variables.items():
            if k in item:
                new_variables[k] = v
            else:
                delattr(new_self, k)
        new_self.variables = new_variables
        return new_self

    def _open_zarr(self):
        fmap = fsspec.get_mapper(
            f's3://{self.bucket_name}/{self.dataset_id}',
            **self.storage_options,
        )
        self._zarr_group = zarr.open_consolidated(fmap)
        self._total_size = memory_repr(
            np.sum([arr.nbytes for _, arr in self._zarr_group.items()])
        )

    def _parse_zarr_group(self):
        all_dims = []
        for k in self._zarr_group.array_keys():
            arr = self._zarr_group[k]
            dims = arr.attrs['_ARRAY_DIMENSIONS']
            attrs = arr.attrs.asdict()
            attrs.pop('_ARRAY_DIMENSIONS')
            self._dataset_dict['variables'][k] = xr.DataArray(
                data=da.from_zarr(arr), dims=dims, name=k, attrs=attrs
            )
            all_dims.append(dims)
        self._dataset_dict['dims'] = list(
            set(it.chain.from_iterable(all_dims))
        )

        self.dimensions = self._dataset_dict['dims']
        self.variables = self._dataset_dict['variables']
        self.global_attributes = self._zarr_group.attrs.asdict()

    def _set_variables(self):
        for name, data_array in self.variables.items():
            setattr(self, name, data_array)

    def _get_dim_indexers(self, indexers) -> dict:
        # Retrieve dimension indexers
        pos_indexes = {}
        for dim in self._dataset_dict['dims']:
            dim_arr = self._dataset_dict['variables'][dim].data
            start, end = indexers[dim]
            pos_indexes[dim] = da.where((dim_arr >= start) & (dim_arr <= end))[
                0
            ].compute()
        return pos_indexes

    def _create_dataset_dict(self, pos_indexes) -> dict:
        data_vars = {}
        # Get data arrays
        for k, v in self.variables.items():
            key = {
                dim: pos_indexes[dim] if dim in pos_indexes else slice(None)
                for dim in v.dims
            }
            data_vars[k] = v.isel(**key)
        return data_vars

    def _create_dataset(self, data_vars: dict) -> xr.Dataset:
        data_vars = {
            k: self._set_time_attrs(
                xr.apply_ufunc(
                    xr.coding.times.decode_cf_datetime,
                    v,
                    keep_attrs=True,
                    dask="parallelized",
                    kwargs={
                        'units': v.attrs['units'],
                        'calendar': v.attrs['calendar'],
                    },
                )
            )
            if k == 'time'
            else v
            for k, v in data_vars.items()
        }
        ds = xr.Dataset(data_vars)
        # new_attrs = self.global_attributes.copy()
        # new_attrs['time_coverage_start'] = ds.time.data[0]
        # new_attrs['time_coverage_end'] = ds.time.data[-1]
        # ds.attrs = new_attrs
        self.dataset = ds

    @staticmethod
    def _set_time_attrs(da):
        new_attrs = da.attrs.copy()
        new_attrs.pop('units')
        new_attrs.pop('calendar')
        da.attrs = new_attrs
        return da

    def reset(self):
        self.variables = self._dataset_dict['variables']
        self._set_variables()
        return self

    def sel(self, indexers: dict = None, **indexers_kwargs):
        # TODO: Figure out how to handle one indexer instead of start, end
        indexers = either_dict_or_kwargs(indexers, indexers_kwargs, "sel")

        for k, v in indexers.items():
            if k not in self.dimensions:
                raise ValueError(
                    f"'{k}' is not a dimension in this data stream."
                )

            if k == "time":
                if isinstance(v, slice):
                    start_dt = parser.parse(v.start)
                    end_dt = parser.parse(v.stop)
                else:
                    start_dt, end_dt = (parser.parse(value) for value in v)

                arr = getattr(self, k)

                time_units = self.__default_time_units
                calendar = self.__default_calendar

                if 'units' in arr.attrs:
                    time_units = arr.attrs['units']
                    calendar = arr.attrs['calendar']
                self.__time_filter, _, _ = xr.coding.times.encode_cf_datetime(
                    [start_dt, end_dt], time_units, calendar
                )

                indexers[k] = self.__time_filter

        pos_indexes = self._get_dim_indexers(indexers)
        data_vars = self._create_dataset_dict(pos_indexes)
        self._create_dataset(data_vars)
        return self
