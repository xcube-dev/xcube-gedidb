# The MIT License (MIT)
# Copyright (c) 2025 by the xcube development team and contributors
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NON INFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import logging
from abc import ABC
from typing import Tuple, Iterator, Container, Union, Any

import xarray as xr
from xcube.core.store import DataDescriptor, DataStore, DataTypeLike
from xcube.util.jsonschema import (
    JsonObjectSchema,
)

import gedidb as gdb

from xcube_gedi.constant import GEDI_S3_BUCKET_NAME, GEDI_URL

_LOG = logging.getLogger("xcube.gedi")


class GediDataStore(DataStore, ABC):
    """Implementation of Gedi data store ."""

    def __init__(self):
        self.provider = gdb.GEDIProvider(
            storage_type="s3", s3_bucket=GEDI_S3_BUCKET_NAME, url=GEDI_URL
        )

    @classmethod
    def get_data_store_params_schema(cls) -> JsonObjectSchema:
        raise NotImplementedError

    @classmethod
    def get_data_types(cls) -> Tuple[str, ...]:
        raise NotImplementedError

    def get_data_types_for_data(self, data_id: str) -> Tuple[str, ...]:
        raise NotImplementedError

    def get_data_ids(
        self,
        data_type: DataTypeLike = None,
        include_attrs: Container[str] | bool = False,
    ) -> Union[Iterator[str], Iterator[tuple[str, dict[str, Any]]]]:
        raise NotImplementedError

    def has_data(self, data_id: str, data_type: str = None) -> bool:
        raise NotImplementedError

    def describe_data(
        self, data_id: str, data_type: DataTypeLike = None
    ) -> DataDescriptor:
        raise NotImplementedError

    def get_data_opener_ids(
        self, data_id: str = None, data_type: DataTypeLike = None
    ) -> Tuple[str, ...]:
        raise NotImplementedError

    def get_open_data_params_schema(
        self, data_id: str = None, opener_id: str = None
    ) -> JsonObjectSchema:
        raise NotImplementedError

    def open_data(
        self, data_id: str = None, opener_id: str = None, **open_params
    ) -> xr.Dataset:
        vars_selected = ["agbd", "rh"]
        return self.provider.get_data(
            variables=vars_selected,
            query_type="nearest",
            point=(8.948581, 50.716982),
            num_shots=5,
            radius=0.1,
            start_time="2023-01-01",
            end_time="2024-07-02",
            return_type="xarray",
        )

    def get_search_params_schema(
        cls, data_type: DataTypeLike = None
    ) -> JsonObjectSchema:
        raise NotImplementedError

    def search_data(
        self, data_type: DataTypeLike = None, **search_params
    ) -> Iterator[DataDescriptor]:
        raise NotImplementedError


# TODO:
#  1. Clarify what to use for data_ids as we can only access the
#  variables through the gedidb api.
#  2. What to do for list_data_ids()? Do we return variables or do we need a
#  new method for it? Does it seem it is deviating from the datastore
#  interface?
#  3. Although the dataset that is returned is xarray, its dimensions are
#  shot_number and profile_points. Need to clarify with stakeholders if that
#  is okay or not.s
