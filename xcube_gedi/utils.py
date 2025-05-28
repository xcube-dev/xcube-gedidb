from typing import Sequence

import geopandas as gpd
from shapely.geometry import box


def convert_bbox_to_geodf(bbox: Sequence[float]) -> gpd.GeoDataFrame | None:
    if len(bbox) == 0:
        return None
    geom = box(*bbox)
    return gpd.GeoDataFrame({"geometry": [geom]})
