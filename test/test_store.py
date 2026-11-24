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
from unittest.mock import Mock, patch

import pandas as pd
import pytest
import xarray as xr
from jsonschema.exceptions import ValidationError
from requests import RequestException
from xcube.core.store import DataDescriptor, new_data_store
from xcube.util.jsonschema import JsonObjectSchema

from xcube_gedidb.constant import GEDIDB_S3_BUCKET_NAME, GEDIDB_URL
from xcube_gedidb.store import (
    GediDbDataStore,
    _GEDI_PRODUCT_CONCEPT_IDS,
    _create_provider,
)


_AVAILABLE_VARIABLES = pd.DataFrame(
    {"product_level": ["L2A", "L2B", "L4A", "L4C"]},
    index=["rh", "cover", "agbd", "wsci"],
)


def _sample_dataset() -> xr.Dataset:
    return xr.Dataset(
        data_vars={"rh": ("shot_number", [1.0])},
        coords={
            "profile_points": [0],
            "shot_number": [1],
            "latitude": ("shot_number", [50.69]),
            "longitude": ("shot_number", [-112.15]),
            "time": ("shot_number", ["2023-01-27"]),
        },
    )


class GediDbDataStoreTest(unittest.TestCase):
    def setUp(self):
        # These are unit tests. Do not make TileDB/S3 requests merely to create
        # a store or validate its schemas.
        GediDbDataStore._available_variables_cache = None
        self.provider = Mock()
        self.provider.get_available_variables.return_value = (
            _AVAILABLE_VARIABLES
        )
        self.provider.get_data.side_effect = lambda **_: _sample_dataset()
        self.provider_factory = patch(
            "xcube_gedidb.store._create_provider", return_value=self.provider
        )
        self.create_provider = self.provider_factory.start()
        self.store = new_data_store("gedidb")

    def tearDown(self):
        self.provider_factory.stop()

    def test_init(self):
        self.assertEqual(5, len(self.store.data_ids))
        self.assertIn("all", self.store.data_ids)
        self.assertIs(self.store.provider, self.provider)
        self.assertIsInstance(self.store.all_supported_variables, pd.DataFrame)

    def test_init_defers_provider_creation_and_reuses_metadata(self):
        self.create_provider.assert_not_called()

        self.assertIsInstance(self.store.all_supported_variables, pd.DataFrame)
        self.create_provider.assert_called_once()
        self.provider.get_available_variables.assert_called_once()

        another_store = new_data_store("gedidb")
        self.assertIsInstance(
            another_store.all_supported_variables, pd.DataFrame
        )
        self.assertIsNot(
            another_store.all_supported_variables,
            self.store.all_supported_variables,
        )
        self.create_provider.assert_called_once()

    @patch("xcube_gedidb.store.import_module")
    def test_create_provider(self, import_module):
        provider = _create_provider()

        import_module.assert_called_once_with("gedidb")
        import_module.return_value.GEDIProvider.assert_called_once_with(
            storage_type="s3",
            s3_bucket=GEDIDB_S3_BUCKET_NAME,
            url=GEDIDB_URL,
        )
        self.assertIs(
            provider, import_module.return_value.GEDIProvider.return_value
        )

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

    def test_get_data_types_for_data(self):
        self.assertEqual(
            ("dataset",), self.store.get_data_types_for_data("L2A")
        )
        with pytest.raises(ValueError, match="Invalid data_id provided"):
            self.store.get_data_types_for_data("invalid")

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
        with patch.object(
            GediDbDataStore,
            "_get_gedi_metadata",
            return_value={
                "bbox": (-53.0, -180.0, 55.7983, 180.0),
                "time_range": (
                    "2019-04-17T00:00:00.000Z",
                    "2024-11-27T23:59:59.999Z",
                ),
            },
        ):
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

    def test_get_open_data_params_schema_for_product_and_opener(self):
        schema = self.store.get_open_data_params_schema(
            data_id="L2A", opener_id="ignored"
        )
        variable_schema = schema.to_dict()["oneOf"][0]["properties"][
            "variables"
        ]
        self.assertEqual(["rh"], variable_schema["items"]["enum"])

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

    def test_open_data_uses_product_variables_by_default(self):
        self.store.open_data(
            data_id="L2A",
            point=(-112.15, 50.69),
            time_range=("2023-01-26", "2023-01-30"),
        )

        self.provider.get_data.assert_called_once_with(
            variables=["rh"],
            query_type="nearest",
            point=(-112.15, 50.69),
            num_shots=10,
            radius=0.1,
            start_time="2023-01-26",
            end_time="2023-01-30",
            return_type="xarray",
        )

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

    @patch("xcube_gedidb.store.requests.get")
    def test__get_gedi_metadata_no_entries(self, get):
        get.return_value.json.return_value = {"feed": {"entry": []}}

        with pytest.raises(ValueError, match="No entries found"):
            self.store._get_gedi_metadata("concept_id")

    @patch("xcube_gedidb.store.requests.get")
    def test__get_gedi_metadata_without_bbox(self, get):
        get.return_value.json.return_value = {
            "feed": {
                "entry": [
                    {
                        "time_start": "2019-04-17T00:00:00.000Z",
                        "time_end": "2024-11-27T23:59:59.999Z",
                    }
                ]
            }
        }

        metadata = self.store._get_gedi_metadata("concept_id")

        self.assertIsNone(metadata["bbox"])
        self.assertEqual(
            ("2019-04-17T00:00:00.000Z", "2024-11-27T23:59:59.999Z"),
            metadata["time_range"],
        )

    def test_get_search_params_schema(self):
        self.assertIsInstance(self.store.get_search_params_schema(), JsonObjectSchema)

    def test_get_available_variables(self):
        pd.testing.assert_frame_equal(
            _AVAILABLE_VARIABLES, self.store._get_available_variables("all")
        )
        pd.testing.assert_frame_equal(
            _AVAILABLE_VARIABLES.loc[["cover"]],
            self.store._get_available_variables("L2B"),
        )

    def test_search_data_not_implemented(self):
        with pytest.raises(NotImplementedError):
            list(self.store.search_data())
