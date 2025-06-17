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

from typing import Any, Container, Iterator, Tuple

import gedidb as gdb
import numpy as np
import pandas as pd
import requests
import xarray as xr
from requests import RequestException
from xcube.core.store import DATASET_TYPE, DataDescriptor, DataStore, DataTypeLike
from xcube.util.jsonschema import (
    JsonArraySchema,
    JsonComplexSchema,
    JsonDateSchema,
    JsonIntegerSchema,
    JsonNumberSchema,
    JsonObjectSchema,
    JsonStringSchema,
)

from .constant import GEDIDB_S3_BUCKET_NAME, GEDIDB_URL, LOG, NASA_CMR_URL
from .utils import assert_valid_data_type, convert_bbox_to_geodf

_GEDIDB_DATA_RETURN_TYPE = "xarray"

_GEDI_PRODUCT_LEVELS_DESCRIPTION = {
    "L2A": "Geolocated waveform data and relative height metrics.",
    "L2B": "Vegetation canopy cover and vertical profile metrics.",
    "L4A": "Aboveground biomass density estimates.",
    "L4C": "Gridded biomass estimates at global scales.",
}


# These concept IDs are required by the NASA CMR API to return the metadata
# for the requested processed level GEDI data
_GEDI_PRODUCT_CONCEPT_IDS = {
    "L2A": "C2142771958-LPCLOUD",
    "L2B": "C2142776747-LPCLOUD",
    "L4A": "C2237824918-ORNL_CLOUD",
    "L4C": "C3049900163-ORNL_CLOUD",
}


class GediDbDataStore(DataStore):
    """Implementation of GediDB data store ."""

    def __init__(self):
        self.provider = gdb.GEDIProvider(
            storage_type="s3", s3_bucket=GEDIDB_S3_BUCKET_NAME, url=GEDIDB_URL
        )
        self.all_supported_variables: pd.DataFrame = (
            self.provider.get_available_variables()
        )
        self.data_ids: list[str] = list(
            self.all_supported_variables["product_level"].unique()
        )
        self.data_ids.append("all")

    @classmethod
    def get_data_store_params_schema(cls) -> JsonObjectSchema:
        LOG.info("This data store can be initialized without any params")
        return JsonObjectSchema(
            additional_properties=False,
        )

    @classmethod
    def get_data_types(cls) -> Tuple[str, ...]:
        return (DATASET_TYPE.alias,)

    def get_data_types_for_data(self, data_id: str) -> Tuple[str, ...]:
        if data_id in self.data_ids:
            return self.get_data_types()
        raise ValueError("Invalid data_id provided")

    def get_data_ids(
        self,
        data_type: DataTypeLike = None,
        include_attrs: Container[str] | bool = False,
    ) -> Iterator[str | tuple[str, dict[str, Any]]]:
        assert_valid_data_type(data_type)
        if include_attrs:
            LOG.warning("There are no attributes for data_ids in this data store")
        for data_id in self.data_ids:
            if include_attrs:
                yield data_id, {}
            else:
                yield data_id

    def has_data(self, data_id: str, data_type: str = None) -> bool:
        assert_valid_data_type(data_type)
        if data_id in self.data_ids:
            return True
        return False

    def describe_data(
        self, data_id: str, data_type: DataTypeLike = None
    ) -> DataDescriptor:
        assert_valid_data_type(data_type)
        if data_id not in self.data_ids:
            raise ValueError("No such data_id found.")
        if data_id == "all":
            raise ValueError(
                "`all` is just a placeholder to allow users to "
                "choose variables from various levels into a "
                "single data cube. Please provide a valid data_id (level_name) "
                "instead"
            )

        metadata = dict(
            **self._get_gedi_metadata(_GEDI_PRODUCT_CONCEPT_IDS.get(data_id))
        )
        return DataDescriptor(data_id, data_type, **metadata)

    def get_data_opener_ids(
        self, data_id: str = None, data_type: DataTypeLike = None
    ) -> Tuple[str, ...]:
        raise NotImplementedError(
            "Since we use gedidb to request the data, which has already been "
            "converted from HDF5 to TileDB arrays, there are no data opener "
            "IDs available. The open_data() method always returns an xarray object."
        )

    def get_open_data_params_schema(
        self, data_id: str = None, opener_id: str = None
    ) -> JsonComplexSchema:
        if opener_id:
            LOG.warning(
                "`opener_id` is ignored since only one way to open the data exists."
            )
        if data_id:
            possible_variables = self._get_available_variables(data_id)
        else:
            possible_variables = self.all_supported_variables

        common_schema = dict(
            variables=JsonArraySchema(
                items=(
                    JsonStringSchema(
                        min_length=0,
                        enum=list(possible_variables.index),
                    )
                ),
                unique_items=True,
                description="List of variables to retrieve from the database.",
            ),
            time_range=JsonDateSchema.new_range(),
        )

        bbox_schema = JsonObjectSchema(
            properties=dict(
                bbox=JsonArraySchema(
                    items=(
                        JsonNumberSchema(minimum=-180, maximum=180),
                        JsonNumberSchema(minimum=-90, maximum=90),
                        JsonNumberSchema(minimum=-180, maximum=180),
                        JsonNumberSchema(minimum=-90, maximum=90),
                    ),
                    description="A bounding box in the form of "
                    "(xmin, ymin, xmax, ymax).",
                ),
                **common_schema,
            ),
            required=["bbox", "time_range"],
        )

        point_schema = JsonObjectSchema(
            properties=dict(
                point=JsonArraySchema(
                    items=(JsonNumberSchema(), JsonNumberSchema()),
                    description="Reference point for nearest "
                    "query as (longitude, latitude).",
                ),
                num_shots=JsonIntegerSchema(
                    default=10, description="Number of shots to retrieve. "
                ),
                radius=JsonNumberSchema(
                    default=0.1, description="Radius in degrees around the point."
                ),
                **common_schema,
            ),
            required=["point", "time_range"],
        )

        return JsonComplexSchema(one_of=[bbox_schema, point_schema])

    def open_data(
        self,
        data_id: str,
        opener_id: str = None,
        **open_params,
    ) -> xr.Dataset:
        schema = self.get_open_data_params_schema()
        schema.validate_instance(open_params)

        if not self.has_data(data_id):
            raise ValueError(f"data_id {data_id} is invalid.")

        vars_selected = open_params.get("variables")
        if vars_selected is None:
            vars_selected = list(self._get_available_variables(data_id).index)

        bbox = open_params.get("bbox", [])

        start_time, end_time = open_params.get("time_range")

        point = open_params.get("point")
        num_shots = open_params.get("num_shots", 10)
        radius = open_params.get("radius", 0.1)

        if bbox:
            region_of_interest = convert_bbox_to_geodf(bbox)
            return self.provider.get_data(
                variables=vars_selected,
                query_type="bounding_box",
                geometry=region_of_interest,
                start_time=start_time,
                end_time=end_time,
                return_type=_GEDIDB_DATA_RETURN_TYPE,
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
                return_type=_GEDIDB_DATA_RETURN_TYPE,
            )

    @classmethod
    def get_search_params_schema(
        cls, data_type: DataTypeLike = None
    ) -> JsonObjectSchema:
        return JsonObjectSchema()

    def search_data(
        self, data_type: DataTypeLike = None, **search_params
    ) -> Iterator[DataDescriptor]:
        raise NotImplementedError("search_data() operation is not supported.")

    @staticmethod
    def _get_gedi_metadata(concept_id: str) -> dict[str, tuple[Any]]:
        url = f"{NASA_CMR_URL}concept_id={concept_id}"
        try:
            response = requests.get(url)
            response.raise_for_status()
        except requests.RequestException as e:
            raise RequestException(f"Failed to retrieve metadata: {e}") from e

        data = response.json()
        entries = data.get("feed", {}).get("entry", [])

        if not entries:
            raise ValueError("No entries found for the specified dataset.")

        entry = entries[0]

        bbox = entry.get("boxes", [None])[0]
        if bbox:
            x = bbox.split(" ")
            bbox = np.array(x, dtype=float)
        else:
            bbox = None

        time_start = entry.get("time_start")
        time_end = entry.get("time_end")
        time_range = (time_start, time_end)

        return {"bbox": tuple(bbox), "time_range": tuple(time_range)}

    def _get_available_variables(
        self,
        product_level: str,
    ) -> pd.DataFrame:
        if product_level == "all":
            return self.all_supported_variables
        return self.all_supported_variables[
            self.all_supported_variables["product_level"] == product_level
        ]
