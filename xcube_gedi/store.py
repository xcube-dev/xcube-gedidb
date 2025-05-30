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
import pandas as pd
import gedidb as gdb


from xcube.core.store import DataDescriptor, DataStore, DataTypeLike
from xcube.util.jsonschema import (
    JsonObjectSchema,
    JsonComplexSchema,
    JsonStringSchema,
    JsonArraySchema,
    JsonIntegerSchema,
    JsonNumberSchema,
)
from xcube_gedi.constant import GEDI_S3_BUCKET_NAME, GEDI_URL, LOG
from xcube_gedi.utils import convert_bbox_to_geodf

_LOG = logging.getLogger("xcube.gedi")

_GEDI_DATA_RETURN_TYPE = "xarray"


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
        available_variables = self.get_available_variables()
        any_of_schema = [
            JsonStringSchema(title=variable.name, description=variable.description)
            for _, variable in available_variables.iterrows()
        ]
        return JsonObjectSchema(
            properties=dict(
                variables=JsonComplexSchema(
                    any_of=any_of_schema,
                    description="List of variables to retrieve from the database.",
                ),
                bbox=JsonArraySchema(
                    min_items=4,
                    max_items=4,
                    description="(Optional) A bounding box as an array "
                    "of [xmin, ymin, xmax, ymax]",
                ),
                query_type=JsonStringSchema(
                    description="(Optional) Type of query to execute, either "
                    "'nearest' or 'bounding_box', in case of nearest, a point "
                    "has to be provided as well",
                    default="bounding_box",
                ),
                start_time=JsonStringSchema(
                    description="(Optional) Start date for temporal filtering "
                    "(format: 'YYYY-MM-DD')."
                ),
                end_time=JsonStringSchema(
                    description="(Optional) End date for temporal filtering "
                    "(format: 'YYYY-MM-DD')."
                ),
                point=JsonArraySchema(
                    min_items=2,
                    max_items=2,
                    description="(Optional) Reference point for nearest "
                    "query, required if query_type is 'nearest' (format: "
                    "Tuple[longitude, latitude]).",
                ),
                num_shots=JsonIntegerSchema(
                    default=10,
                    description="(Optional) Number of shots to retrieve if the "
                    "query_type is 'nearest'",
                ),
                radius=JsonNumberSchema(
                    default=0.1,
                    description="(Optional) Radius in degrees around the point "
                    "if the query_type is 'nearest'",
                ),
            ),
            required=["variables"],
        )

    def open_data(
        self,
        data_id: str = None,
        opener_id: str = None,
        **open_params,
    ) -> xr.Dataset:
        assert open_params["variables"] is not None

        vars_selected = open_params.get("variables")
        bbox = open_params.get("bbox", [])
        if len(bbox) != 0:
            assert len(bbox) == 4, (
                "Please provide a bbox as the following list ["
                f"xmin, ymin, xmax, ymax], but got {bbox} "
                "instead"
            )
        query_type = open_params.get("query_type", "")
        start_time = open_params.get("start_time", None)
        end_time = open_params.get("end_time", None)

        point = open_params.get("point", None)
        num_shots = open_params.get("num_shots", None)
        radius = open_params.get("radius", None)
        if point is not None:
            assert (
                num_shots is not None
            ), "num_shots should be provided when using point"
            assert radius is not None, "radius should be provided when using point"

        if bbox:
            assert query_type is None or query_type == "bounding_box", (
                " When providing a bbox, the query_type should either be "
                "'bounding_box' or omitted entirely, as it is the default "
                f"value, but {query_type} was provided"
            )

        if point:
            assert query_type == "nearest", (
                "When providing point, the query_type should be 'nearest' but "
                f"{query_type} was provided."
            )

        if point is not None and bbox is not None:
            LOG.warning(
                "Both bbox and point were provided, by default bbox " "will be used."
            )

        if query_type == "" or query_type != "nearest":
            region_of_interest = convert_bbox_to_geodf(bbox)
            return self.provider.get_data(
                variables=vars_selected,
                query_type="bounding_box",
                geometry=region_of_interest,
                start_time=start_time,
                end_time=end_time,
                return_type=_GEDI_DATA_RETURN_TYPE,
            )
        else:
            return self.provider.get_data(
                variables=vars_selected,
                query_type="nearest",
                point=point,
                num_shots=num_shots,
                radius=radius,
                start_time=start_time,
                end_time=end_time,
                return_type=_GEDI_DATA_RETURN_TYPE,
            )

    def get_search_params_schema(
        cls, data_type: DataTypeLike = None
    ) -> JsonObjectSchema:
        raise NotImplementedError

    def search_data(
        self, data_type: DataTypeLike = None, **search_params
    ) -> Iterator[DataDescriptor]:
        raise NotImplementedError

    def get_available_variables(
        self,
        product_level: str = None,
    ) -> pd.DataFrame:
        variables = self.provider.get_available_variables()
        if product_level:
            variables = variables[variables["product_level"] == product_level]
        return variables


# TODO:
#  1. Clarify what to use for data_ids as we can only access the
#  variables through the gedidb api.
#  2. What to do for list_data_ids()? Do we return variables or do we need a
#  new method for it? Does it seem it is deviating from the datastore
#  interface?
#  3. Although the dataset that is returned is xarray, its dimensions are
#  shot_number and profile_points. Need to clarify with stakeholders if that
#  is okay or not.
