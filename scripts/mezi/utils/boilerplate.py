
# Author: LU, Rīgas Meži, SunGIS
# Created: 2024
# License: EUPL License

# Dependencies: environment.yml
# Python Version: 3.12+

import argparse
from collections.abc import Callable, Sequence
from typing import Any

import shapely

from mezi.utils import config, geom


def get_parse_main(
    config_cls: type[config.Config],
    config_name: str,
    extra_main: Callable[[config.Config, shapely.Geometry | None, tuple[float, ...] | None], int],
    extra_parse: Callable[[argparse.ArgumentParser], None] | None = None,
) -> tuple[
    Callable[[Sequence[str] | None, argparse.Namespace | None], argparse.Namespace],
    Callable[[config.Config | str | dict[str, Any], str | shapely.Geometry | None, str | tuple[float, ...] | None, str | shapely.Geometry | None], int],
]:
    config_type = config.get_config_type(config_cls)
    default_config = config_type(f"{config_name}.json")

    def parse(args: Sequence[str] | None = None, namespace: argparse.Namespace | None = None) -> argparse.Namespace:
        parser = argparse.ArgumentParser()
        parser.add_argument("-c", "--config", default=default_config, type=config_type, help="config file or object, defaults to %(default)s")
        parser.add_argument("-w", "--wkt", default=geom.DEFAULT_WKT, type=geom.wkt_type, help="wkt, defaults to %(default)s")
        parser.add_argument("-b", "--bbox", default=geom.DEFAULT_BBOX, type=geom.bbox_type, help="bbox as 'minx,miny,maxx,maxy', defaults to %(default)s")
        parser.add_argument(
            "-g",
            "--gpkg",
            default=geom.DEFAULT_GPKG,
            type=geom.gpkg_type,
            help="gpkg as 'path##layer##fid', note that layer and fid are optional, also, fid will be remaped during load to a zero-based sequence based on the original fid, defaults to %(default)s",
        )  # see https://github.com/geopandas/geopandas/issues/2794 and https://github.com/geopandas/geopandas/issues/1035
        if extra_parse:
            extra_parse(parser)
        return parser.parse_args(args, namespace)  # pyright: ignore [reportReturnType]

    def main(
        config: config.Config | str | dict[str, Any] = default_config,
        wkt: str | shapely.Geometry | None = geom.DEFAULT_WKT,
        bbox: str | tuple[float, ...] | None = geom.DEFAULT_BBOX,
        gpkg: str | shapely.Geometry | None = geom.DEFAULT_GPKG,
        **kwargs: Any,
    ) -> int:
        return extra_main(config_type(config), geom.wkt_type(wkt) or geom.gpkg_type(gpkg), geom.bbox_type(bbox), **kwargs)

    return parse, main
