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

import geopandas as gpd
from shapely.geometry import Polygon, box

from xcube_gedidb.utils import convert_bbox_to_geodf


class UtilsTest(unittest.TestCase):
    def test_convert_bbox_to_geodf_valid_bbox(self):
        bbox = [-10, -5, 10, 5]
        result = convert_bbox_to_geodf(bbox)

        self.assertIsInstance(result, gpd.GeoDataFrame)
        self.assertIn("geometry", result.columns)
        self.assertEqual(1, len(result))

        expected_geom = box(*bbox)
        actual_geom = result.iloc[0].geometry

        self.assertIsInstance(actual_geom, Polygon)
        self.assertEqual(expected_geom, actual_geom)
