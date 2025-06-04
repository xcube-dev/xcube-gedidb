# The MIT License (MIT)
# Copyright (c) 2025 by the xcube development team and contributors
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
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

import unittest

import gedidb
import pandas as pd
import xarray as xr
import pytest
from requests import RequestException
from xcube.core.store import DATASET_TYPE, DataDescriptor
from xcube.util.jsonschema import JsonObjectSchema

from xcube_gedi.store import GediDataStore, _GEDI_CONCEPT_IDS


class GediDataStoreTest(unittest.TestCase):
    def setUp(self):
        self.store = GediDataStore()

    def test_init(self):
        self.assertEqual(len(self.store.data_ids), 5)
        self.assertIn("all", self.store.data_ids)
        self.assertIsInstance(self.store.provider, gedidb.GEDIProvider)
        self.assertIsInstance(self.store.all_supported_variables, pd.DataFrame)

    def test_get_data_store_params_schema(self):
        schema = self.store.get_data_store_params_schema()
        self.assertIsInstance(schema, JsonObjectSchema)
        self.assertEqual(
            schema.to_dict(),
            {"additionalProperties": False, "properties": {}, "type": "object"},
        )

    def test_get_data_types(self):
        types = self.store.get_data_types()
        self.assertIsInstance(types, tuple)
        self.assertIn(DATASET_TYPE.alias, types)

    def test_get_data_ids_without_attrs(self):
        data_ids = list(self.store.get_data_ids())
        expected_data_ids = ["L2A", "L2B", "L4A", "L4C", "all"]
        self.assertEqual(sorted(data_ids), sorted(expected_data_ids))

    def test_get_data_ids_with_attrs(self):
        ids = list(self.store.get_data_ids(include_attrs=True))
        [
            self.assertIsInstance(item, tuple)
            and self.assertIsInstance(item[1], dict)
            and self.assertEqual(item[1], {})
            for item in ids
        ]

    def test_has_data(self):
        self.assertEqual(self.store.has_data("L2A"), True)
        self.assertEqual(self.store.has_data("L4A"), True)
        self.assertEqual(self.store.has_data("L4B"), False)
        self.assertEqual(self.store.has_data("johndoe"), False)

    def test_describe_data_valid(self):
        desc = self.store.describe_data("L4A")
        self.assertIsInstance(desc, DataDescriptor)
        self.assertEqual(desc.data_id, "L4A")
        self.assertEqual(desc.bbox, (-53.0, -180.0, 55.7983, 180.0))
        self.assertEqual(
            desc.time_range, ("2019-04-17T00:00:00.000Z", "2024-11-27T23:59:59.999Z")
        )

    def test_describe_data_invalid(self):
        with pytest.raises(ValueError, match="No such data_id found."):
            self.store.describe_data("L4B")

    def test_describe_data_all_id(self):
        with pytest.raises(ValueError, match="`all` is just a placeholder"):
            self.store.describe_data("all")

    def test_get_data_opener_ids(self):
        with pytest.raises(NotImplementedError):
            self.store.get_data_opener_ids()

    def test_get_open_data_params_schema(self):
        schema = self.store.get_open_data_params_schema()
        schema_dict = schema.to_dict()
        self.assertIn("variables", schema_dict["properties"])
        self.assertIn("bbox", schema_dict["properties"])
        self.assertIn("query_type", schema_dict["properties"])
        self.assertIn("start_time", schema_dict["properties"])
        self.assertIn("end_time", schema_dict["properties"])
        self.assertIn("point", schema_dict["properties"])
        self.assertIn("num_shots", schema_dict["properties"])
        self.assertIn("radius", schema_dict["properties"])
        self.assertIn("variables", schema_dict["required"])

    def test_open_data_with_bbox(self):
        ds = self.store.open_data(
            data_id="L2A",
            variables=["rh"],
            bbox=(-112.30, 50.63, -112.0, 50.75),
            start_time="2023-01-26",
            end_time="2023-01-30",
        )
        self.assertIsInstance(ds, xr.Dataset)
        dimensions = [d[0] for d in list(ds.dims.items())]
        self.assertIn("profile_points", dimensions)
        self.assertIn("shot_number", dimensions)

        coords = [c[0] for c in list(ds.coords.items())]
        self.assertIn("profile_points", coords)
        self.assertIn("shot_number", coords)
        self.assertIn("latitude", coords)
        self.assertIn("longitude", coords)
        self.assertIn("time", coords)

    def test_open_data_with_point_missing_numshots(self):
        with pytest.raises(AssertionError, match="num_shots should be provided"):
            self.store.open_data(data_id="L2A", variables=["rh"], point=[0, 0])

    def test_open_data_with_point_wrong_query_type(self):
        with pytest.raises(AssertionError, match="should be 'nearest'"):
            self.store.open_data(
                data_id="L2A",
                variables=["rh"],
                point=[0, 0],
                num_shots=10,
                radius=0.1,
                query_type="bounding_box",
            )

    def test_open_data_with_all_data_id(self):
        ds = self.store.open_data(
            data_id="all",
            variables=["rh", "cover"],
            bbox=(-112.30, 50.63, -112.0, 50.75),
            start_time="2023-01-26",
            end_time="2023-01-30",
        )
        self.assertIsInstance(ds, xr.Dataset)

    def test_open_data_invalid_variables(self):
        with pytest.raises(
            ValueError, match="The following variable\\(s\\) are invalid"
        ):
            self.store.open_data(
                data_id="L2A",
                variables=["invalid_var"],
                bbox=(-112.30, 50.63, -112.0, 50.75),
                start_time="2023-01-26",
                end_time="2023-01-30",
            )

    def test_open_data_invalid_bbox_length(self):
        with pytest.raises(
            AssertionError, match="Please provide a bbox as the following list"
        ):
            self.store.open_data(
                data_id="L2A",
                variables=["rh"],
                bbox=(-112.30, 50.63, -112.0),
                start_time="2023-01-26",
                end_time="2023-01-30",
            )

    def test_open_data_with_point_missing_radius(self):
        with pytest.raises(
            AssertionError, match="radius should be provided when using point"
        ):
            self.store.open_data(
                data_id="L2A",
                variables=["rh"],
                point=[0, 0],
                num_shots=10,
                start_time="2023-01-26",
                end_time="2023-01-30",
            )

    def test_open_data_with_bbox_wrong_query_type(self):
        with pytest.raises(
            AssertionError,
            match="When providing a bbox, the query_type should either be",
        ):
            self.store.open_data(
                data_id="L2A",
                variables=["rh"],
                bbox=(-112.30, 50.63, -112.0, 50.75),
                query_type="nearest",
                start_time="2023-01-26",
                end_time="2023-01-30",
            )

    def test_open_data_with_point_and_bbox_warning(self):
        with self.assertLogs(level="WARNING") as log:
            ds = self.store.open_data(
                data_id="L2A",
                variables=["rh"],
                point=[0, 0],
                bbox=(-112.30, 50.63, -112.0, 50.75),
                num_shots=10,
                radius=0.1,
                start_time="2023-01-26",
                end_time="2023-01-30",
            )
        self.assertIn("Both bbox and point were provided", log.output[0])
        self.assertIsInstance(ds, xr.Dataset)

    def test_open_data_with_point_nearest_query(self):
        ds = self.store.open_data(
            data_id="L2A",
            variables=["rh"],
            point=(-112.15, 50.69),
            num_shots=5,
            radius=0.05,
            query_type="nearest",
            start_time="2023-01-26",
            end_time="2023-01-30",
        )
        self.assertIsInstance(ds, xr.Dataset)

    def test_open_data_with_bounding_box_query_type_explicit(self):
        ds = self.store.open_data(
            data_id="L2A",
            variables=["rh"],
            bbox=(-112.30, 50.63, -112.0, 50.75),
            query_type="bounding_box",
            start_time="2023-01-26",
            end_time="2023-01-30",
        )
        self.assertIsInstance(ds, xr.Dataset)

    def test_open_data_without_bbox(self):
        with pytest.raises(ValueError):
            ds = self.store.open_data(
                data_id="L2A",
                variables=["rh"],
                start_time="2023-01-26",
                end_time="2023-01-30",
            )

        with pytest.raises(ValueError):
            ds = self.store.open_data(
                data_id="L2A",
                variables=["rh"],
                bbox=[],
                start_time="2023-01-26",
                end_time="2023-01-30",
            )

    def test_open_data_invalid_data_id(self):
        with pytest.raises(AssertionError):
            self.store.open_data(
                data_id="invalid_id",
                variables=["rh"],
            )

    def test_open_data_missing_variables(self):
        with pytest.raises(AssertionError):
            self.store.open_data(
                data_id="L2A",
                variables=None,
            )

    def test__get_gedi_metadata_success(self):
        metadata = self.store._get_gedi_metadata(_GEDI_CONCEPT_IDS.get("L4A"))
        self.assertIsInstance(metadata, dict)
        self.assertEqual(metadata.get("bbox"), (-53.0, -180.0, 55.7983, 180.0))
        self.assertEqual(
            metadata.get("time_range"),
            ("2019-04-17T00:00:00.000Z", "2024-11-27T23:59:59.999Z"),
        )

    def test__get_gedi_metadata_exception(self):
        with pytest.raises(RequestException, match="Failed to retrieve metadata"):
            self.store._get_gedi_metadata("invalid_concept_id")

    def test_search_data_not_implemented(self):
        with pytest.raises(NotImplementedError):
            list(self.store.search_data())
