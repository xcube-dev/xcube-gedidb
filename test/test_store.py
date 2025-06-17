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
import pytest
import xarray as xr
from jsonschema.exceptions import ValidationError
from requests import RequestException
from xcube.core.store import DataDescriptor, new_data_store
from xcube.util.jsonschema import JsonObjectSchema

from xcube_gedidb.store import _GEDI_PRODUCT_CONCEPT_IDS


class GediDbDataStoreTest(unittest.TestCase):
    def setUp(self):
        self.store = new_data_store("gedidb")

    def test_init(self):
        self.assertEqual(5, len(self.store.data_ids))
        self.assertIn("all", self.store.data_ids)
        self.assertIsInstance(self.store.provider, gedidb.GEDIProvider)
        self.assertIsInstance(self.store.all_supported_variables, pd.DataFrame)

    def test_get_data_store_params_schema(self):
        schema = self.store.get_data_store_params_schema()
        self.assertIsInstance(schema, JsonObjectSchema)
        self.assertEqual(
            {"additionalProperties": False, "properties": {}, "type": "object"},
            schema.to_dict(),
        )

    def test_get_data_types(self):
        types = self.store.get_data_types()
        self.assertIsInstance(types, tuple)
        self.assertEqual(("dataset",), types)

    def test_get_data_ids_without_attrs(self):
        data_ids = list(self.store.get_data_ids())
        expected_data_ids = ["L2A", "L2B", "L4A", "L4C", "all"]
        self.assertEqual(sorted(expected_data_ids), sorted(data_ids))

    def test_get_data_ids_with_attrs(self):
        ids = list(self.store.get_data_ids(include_attrs=True))
        [
            self.assertIsInstance(item, tuple)
            and self.assertIsInstance(item[1], dict)
            and self.assertEqual({}, item[1])
            for item in ids
        ]

    def test_has_data(self):
        self.assertTrue(self.store.has_data("L2A"))
        self.assertTrue(self.store.has_data("L4A"))
        self.assertFalse(self.store.has_data("L4B"))
        self.assertFalse(self.store.has_data("johndoe"))

    def test_describe_data_valid(self):
        desc = self.store.describe_data("L4A")
        self.assertIsInstance(desc, DataDescriptor)
        self.assertEqual("L4A", desc.data_id)
        self.assertEqual((-53.0, -180.0, 55.7983, 180.0), desc.bbox)
        self.assertEqual(
            ("2019-04-17T00:00:00.000Z", "2024-11-27T23:59:59.999Z"), desc.time_range
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
        self.assertIn("variables", schema_dict["oneOf"][0]["properties"])
        self.assertIn("variables", schema_dict["oneOf"][1]["properties"])
        self.assertIn("time_range", schema_dict["oneOf"][0]["properties"])
        self.assertIn("time_range", schema_dict["oneOf"][1]["properties"])
        self.assertIn("bbox", schema_dict["oneOf"][0]["properties"])
        self.assertIn("point", schema_dict["oneOf"][1]["properties"])
        self.assertIn("num_shots", schema_dict["oneOf"][1]["properties"])
        self.assertIn("radius", schema_dict["oneOf"][1]["properties"])

    def test_open_data_with_bbox(self):
        ds = self.store.open_data(
            data_id="L2A",
            variables=["rh"],
            bbox=(-112.30, 50.63, -112.0, 50.75),
            time_range=("2023-01-26", "2023-01-30"),
        )
        self.assertIsInstance(ds, xr.Dataset)
        dimensions = list(ds.dims)
        self.assertIn("profile_points", dimensions)
        self.assertIn("shot_number", dimensions)

        coords = list(ds.coords)
        self.assertIn("profile_points", coords)
        self.assertIn("shot_number", coords)
        self.assertIn("latitude", coords)
        self.assertIn("longitude", coords)
        self.assertIn("time", coords)

    def test_open_data_with_all_data_id(self):
        ds = self.store.open_data(
            data_id="all",
            variables=["rh", "cover"],
            bbox=(-112.30, 50.63, -112.0, 50.75),
            time_range=("2023-01-26", "2023-01-30"),
        )
        self.assertIsInstance(ds, xr.Dataset)

    def test_open_data_with_point_and_bbox_validation_error(self):
        with pytest.raises(ValidationError):
            ds = self.store.open_data(
                data_id="L2A",
                variables=["rh"],
                point=(0, 0),
                bbox=(-112.30, 50.63, -112.0, 50.75),
                num_shots=10,
                radius=0.1,
                time_range=("2023-01-26", "2023-01-30"),
            )

    def test_open_data_with_point(self):
        ds = self.store.open_data(
            data_id="L2A",
            variables=["rh"],
            point=(-112.15, 50.69),
            num_shots=5,
            radius=0.1,
            time_range=("2023-01-26", "2023-01-30"),
        )
        self.assertIsInstance(ds, xr.Dataset)

    def test_open_data_invalid_data_id(self):
        with pytest.raises(ValueError):
            self.store.open_data(
                data_id="invalid_id",
                variables=["rh"],
                point=(-112.15, 50.69),
                num_shots=5,
                radius=0.1,
                time_range=("2023-01-26", "2023-01-30"),
            )

    def test_open_data_no_bbox_point(self):
        with pytest.raises(ValidationError):
            self.store.open_data(
                data_id="L4C",
                variables=["rh"],
                time_range=("2023-01-26", "2023-01-30"),
            )

    @pytest.mark.vcr()
    def test__get_gedi_metadata_success(self):
        metadata = self.store._get_gedi_metadata(_GEDI_PRODUCT_CONCEPT_IDS.get("L4A"))
        self.assertIsInstance(metadata, dict)
        self.assertEqual((-53.0, -180.0, 55.7983, 180.0), metadata.get("bbox"))
        self.assertEqual(
            ("2019-04-17T00:00:00.000Z", "2024-11-27T23:59:59.999Z"),
            metadata.get("time_range"),
        )

    @pytest.mark.vcr()
    def test__get_gedi_metadata_exception(self):
        with pytest.raises(RequestException, match="Failed to retrieve metadata"):
            self.store._get_gedi_metadata("invalid_concept_id")

    def test_search_data_not_implemented(self):
        with pytest.raises(NotImplementedError):
            list(self.store.search_data())
