# pyright: reportCallIssue=false, reportUnknownMemberType=false, reportUnknownVariableType=false

# Author: LU, Rīgas Meži, SunGIS
# Created: 2024
# License: EUPL License

# Dependencies: environment.yml
# Python Version: 3.12+

import os
from collections.abc import Generator
from typing import Any

import geopandas as gpd
import pandas as pd
import shapely.wkt

from mezi.utils import config

_READ_FILE_CACHE: dict[tuple[str, str | None], gpd.GeoDataFrame] = {}


DEFAULT_WKT = None
DEFAULT_BBOX = None
DEFAULT_GPKG = None
DEFAULT_LAYER = None


def read_file(config: config.Config | None, path: str, layer: str | None = DEFAULT_LAYER, bbox: tuple[float, ...] | None = DEFAULT_BBOX) -> gpd.GeoDataFrame:
    key = (path, layer)
    if key in _READ_FILE_CACHE:
        return _READ_FILE_CACHE[key]
    if config:
        config.print(f"reading '{layer or ''}' from '{path}'")
    gdf = gpd.read_file(path, bbox=gpd.GeoDataFrame(geometry=[shapely.box(*bbox)], crs="EPSG:3059") if bbox and len(bbox) == 4 else None, engine="fiona", layer=layer)  # noqa: PLR2004
    gdf.columns = gdf.columns.str.lower()
    gdf.to_crs("EPSG:3059", inplace=True)
    _READ_FILE_CACHE[key] = gdf
    return gdf


def write_file(config: config.Config | None, df: pd.DataFrame | pd.Series, path: str, *args: Any, unlink: bool = True, **kwargs: Any) -> None:  # pyright: ignore [reportUnknownParameterType, reportMissingTypeArgument]
    if config:
        config.print(f"writing to '{path}'")
    if unlink:
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass
    df.to_file(path, *args, **kwargs)


def wkt_type(wkt: str | shapely.Geometry | None) -> shapely.Geometry | None:
    return shapely.wkt.loads(wkt) if isinstance(wkt, str) else wkt


def bbox_type(bbox: str | tuple[float, ...] | None) -> tuple[float, ...] | None:
    return tuple(float(value.strip()) for value in bbox.split(",")) if isinstance(bbox, str) else (bbox if bbox is None else tuple(bbox))


def gpkg_type(gpkg: str | shapely.Geometry | None) -> shapely.Geometry | None:
    if not isinstance(gpkg, str):
        return gpkg
    parts = tuple(gpkg.split("##"))
    parts += (None,) * (3 - len(parts))
    path, layer, fid = parts
    return read_file(None, path, layer).iloc[(fid and int(fid)) or 0].geometry  # pyright: ignore [reportArgumentType]


def filter(gdf: gpd.GeoDataFrame, wkt: shapely.Geometry | None = DEFAULT_WKT, bbox: tuple[float, ...] | None = DEFAULT_BBOX) -> gpd.GeoDataFrame:  # noqa: A001
    if wkt:
        return gdf[gdf.intersects(wkt)]  # pyright: ignore [reportReturnType]
    if bbox:
        xmin, ymin, xmax, ymax = bbox
        return gdf.cx[xmin:xmax, ymin:ymax]
    return gdf


def points(geom: shapely.Geometry) -> Generator[tuple[float, float]]:
    if isinstance(geom, shapely.Point | shapely.LineString | shapely.LinearRing):
        yield from ((coords[0], coords[1]) for coords in geom.coords)
    elif isinstance(geom, shapely.Polygon):
        yield from points(geom.exterior)
        for interior in geom.interiors:
            yield from points(interior)
    elif isinstance(geom, shapely.MultiPoint | shapely.MultiLineString | shapely.MultiPolygon | shapely.GeometryCollection):
        for _geom in geom.geoms:
            yield from points(_geom)  # pyright: ignore [reportUnknownArgumentType]
    else:
        raise TypeError(f"unknown geom type: {type(geom)}")


def polygons(geom: shapely.Geometry) -> Generator[shapely.Polygon]:
    if isinstance(geom, shapely.Polygon):
        yield geom
    elif isinstance(geom, shapely.MultiPolygon):
        yield from geom.geoms
    elif isinstance(geom, shapely.GeometryCollection):
        for _geom in geom.geoms:
            yield from polygons(_geom)  # pyright: ignore [reportUnknownArgumentType]


def index_polygons(index: int, geom: shapely.Geometry) -> Generator[tuple[int, shapely.Polygon]]:
    for _geom in polygons(geom):
        yield index, _geom
