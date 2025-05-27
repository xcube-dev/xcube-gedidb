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
from xcube.core.store import (
    DataDescriptor,
    DataStore,
    DataTypeLike
)
from xcube.util.jsonschema import (
    JsonObjectSchema,
)

_LOG = logging.getLogger("xcube.gedi")


class GediDataStore(DataStore, ABC):
    """ Implementation of Gedi data store .
    """

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
        self, data_id: str, opener_id: str = None, **open_params
    ) -> xr.Dataset:
        raise NotImplementedError

    def get_search_params_schema(
            cls, data_type: DataTypeLike = None
    ) -> JsonObjectSchema:
        raise NotImplementedError

    def search_data(
            self, data_type: DataTypeLike = None, **search_params
    ) -> Iterator[DataDescriptor]:
        raise NotImplementedError