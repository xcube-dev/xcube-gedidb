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

from typing import Tuple, Iterator, Container, Union, Any

import requests
import xarray as xr
import numpy as np
import pandas as pd
import gedidb as gdb
from requests import RequestException

from xcube.core.store import DataDescriptor, DataStore, DataTypeLike, DATASET_TYPE
from xcube.util.jsonschema import (
    JsonObjectSchema,
    JsonComplexSchema,
    JsonStringSchema,
    JsonArraySchema,
    JsonIntegerSchema,
    JsonNumberSchema,
)
from xcube_gedi.constant import GEDI_S3_BUCKET_NAME, GEDI_URL, LOG, NASA_CMR_URL
from xcube_gedi.utils import convert_bbox_to_geodf, assert_valid_data_type

_GEDI_DATA_RETURN_TYPE = "xarray"

_GEDI_LEVELS_DESCRIPTION = {
    "L2A": "Geolocated waveform data and relative height metrics.",
    "L2B": "Vegetation canopy cover and vertical profile metrics.",
    "L4A": "Aboveground biomass density estimates.",
    "L4C": "Gridded biomass estimates at global scales.",
}

_GEDI_CONCEPT_IDS = {
    "L2A": "C2142771958-LPCLOUD",
    "L2B": "C2142776747-LPCLOUD",
    "L4A": "C2237824918-ORNL_CLOUD",
    "L4C": "C3049900163-ORNL_CLOUD",
}


class GediDataStore(DataStore):
    """Implementation of Gedi data store ."""

    def __init__(self):
        self.provider = gdb.GEDIProvider(
            storage_type="s3", s3_bucket=GEDI_S3_BUCKET_NAME, url=GEDI_URL
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
        return self.get_data_types()

    def get_data_ids(
        self,
        data_type: DataTypeLike = None,
        include_attrs: Container[str] | bool = False,
    ) -> Union[Iterator[str], Iterator[tuple[str, dict[str, Any]]]]:
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

        metadata = dict(**self._get_gedi_metadata(_GEDI_CONCEPT_IDS.get(data_id)))
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
    ) -> JsonObjectSchema:
        any_of_schema = [
            JsonStringSchema(title=variable.name, description=variable.description)
            for _, variable in self.all_supported_variables.iterrows()
        ]
        return JsonObjectSchema(
            properties=dict(
                variables=JsonComplexSchema(
                    any_of=any_of_schema,
                    enum=list(
                        self.all_supported_variables.index
                        + ": "
                        + self.all_supported_variables.description
                    ),
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
        data_id: str,
        opener_id: str = None,
        **open_params,
    ) -> xr.Dataset:
        assert data_id in self.data_ids

        assert open_params["variables"] is not None

        vars_selected = open_params.get("variables")
        if data_id == "all":
            possible_variables = list(self.all_supported_variables.index)
        else:
            possible_variables = list(self._get_available_variables(data_id).index)
        invalid_vars = [
            elem for elem in vars_selected if elem not in possible_variables
        ]

        if len(invalid_vars) > 0:
            raise ValueError(
                f"The following variable(s) are invalid: {invalid_vars} for "
                f"data_id: {data_id}"
            )

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

        if point is not None and bbox is not None:
            LOG.warning(
                "Both bbox and point were provided, by default bbox will be used."
            )
            query_type = "bounding_box"

        if bbox:
            assert query_type == "" or query_type == "bounding_box", (
                " When providing a bbox, the query_type should either be "
                "'bounding_box' or omitted entirely, as it is the default "
                f"value, but {query_type} was provided"
            )

        if point and not bbox:
            assert query_type == "nearest", (
                "When providing point, the query_type should be 'nearest' but "
                f"{query_type} was provided."
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
        return JsonObjectSchema()

    def search_data(
        self, data_type: DataTypeLike = None, **search_params
    ) -> Iterator[DataDescriptor]:
        raise NotImplementedError("search_data() operation is not supported yet")

    @staticmethod
    def _get_gedi_metadata(concept_id: str) -> dict[str, tuple[Any]]:
        url = f"{NASA_CMR_URL}concept_id={concept_id}"
        response = requests.get(url)

        if response.status_code != 200:
            raise RequestException(
                f"Failed to retrieve metadata: HTTP" f" {response.status_code}"
            )

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
        return self.all_supported_variables[
            self.all_supported_variables["product_level"] == product_level
        ]
