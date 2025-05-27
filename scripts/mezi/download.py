#!/usr/bin/env python
# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false


# Author: LU, Rīgas Meži, SunGIS
# Created: 2024
# License: EUPL License

# Dependencies: environment.yml
# Python Version: 3.12+

import functools
import logging
import math
import operator
import os
import sys
import warnings
import zipfile
from collections.abc import Callable, Iterable, Mapping
from typing import Any

import numpy as np
import pdal
import shapely

from mezi import config as mezi_config
from mezi import mvr, tessellate
from mezi.utils import boilerplate, geom, misc


def _filter_fiona_log(record: logging.LogRecord) -> bool:
    msg = str(record.msg) % record.args
    return msg != "year 0 is out of range"


logger = logging.getLogger("fiona.ogrext")
logger.addFilter(_filter_fiona_log)


warnings.filterwarnings("ignore", "GeoSeries.notna", UserWarning)


def _bbox(wkt: shapely.Geometry | None, bbox: tuple[float, ...] | None) -> tuple[float, ...]:
    bbox = bbox or (wkt and wkt.bounds)  # pyright: ignore [reportAttributeAccessIssue]
    if not bbox or len(bbox) != 4:  # noqa: PLR2004
        raise ValueError(f"bbox({bbox}) is invalid")
    return bbox


def name(wkt: shapely.Geometry | None, bbox: tuple[float, ...] | None) -> str:
    return str(hash(_bbox(wkt, bbox)))


def _download(
    config: mezi_config.DownloadConfig,
    name: str,
    get_path: Callable[[str], str],
    gpkg_path: str,
    layer: str,
    field: str,
    cache_path: str,
    force_invalidate: bool,
) -> list[str]:
    tails = geom.filter(geom.read_file(config, gpkg_path, layer, config.bbox), config.wkt, config.bbox)[field]
    tails_len = tails.shape[0]
    suffix = f"of {tails_len}"
    paths = []
    for current, tail in enumerate(tails):
        path = get_path(str(tail))
        mezi_config.print_progress_bar(config, current, tails_len, f"downloading {name} from {path}", suffix)
        paths.append(misc.download(path, os.path.join(cache_path, os.path.split(path)[1]), force_invalidate))
    mezi_config.print_progress_bar(config, tails_len, tails_len, f"all {name} downloaded", suffix)
    return paths


_TIF_FILLNODATA = "gdal_fillnodata -md 25 -si 0 -b 1 -of GTiff '{input}' '{output}'"
_TIF_TRI = "gdaldem TRI '{input}' '{output}'"
_TIF_TPI = "gdaldem TPI '{input}' '{output}'"
_TIF_SLOPE = "gdaldem slope '{input}' '{output}'"
_TIF_ROUGHNESS = "gdaldem roughness '{input}' '{output}'"
_TIF_ASPECT = "gdaldem aspect '{input}' '{output}'"
_TIF_BA = "gdal_calc -A '{input_dtm}' -B '{input_dsm}' --calc='B-A' --outfile '{output}'"


def _download_lgia_las(config: mezi_config.DownloadConfig) -> None:
    paths = _download(
        config,
        "lgia las",
        lambda tail: os.path.join(config.LGIA_LAS_PATH_HEAD, tail.split("-")[0], f"{tail}.las"),
        config.LGIA_LAS_PATH_TAIL_NOMENKLATURA_GPKG_PATH,
        config.LGIA_LAS_PATH_TAIL_NOMENKLATURA_LAYER,
        config.LGIA_LAS_PATH_TAIL_NOMENKLATURA_FIELD,
        config.LGIA_LAS_CACHE_PATH,
        config.LGIA_LAS_CACHE_FORCE_INVALIDATE,
    )
    if not paths:
        config.print("no lgia las")
        return
    force_invalidate = config.LGIA_TIF_CACHE_FORCE_INVALIDATE
    head = os.path.join(config.LGIA_TIF_CACHE_PATH, config.name)
    bounds = f"([{math.floor(config.bbox[0])}, {math.ceil(config.bbox[2])}], [{math.floor(config.bbox[1])}, {math.ceil(config.bbox[3])}])" if config.bbox else None
    os.makedirs(config.LGIA_TIF_CACHE_PATH, exist_ok=True)
    dtm_path = head + "_dtm.tif"
    config.OUTPUT_FILES_TO_ZIP.append(dtm_path)
    dtm_fillnodata_path = head + "_dtm_fillnodata.tif"
    config.OUTPUT_FILES_TO_ZIP.append(dtm_fillnodata_path)
    if config.LGIA_TIF_DTM and (force_invalidate or not os.path.isfile(dtm_path)):
        config.print(f"generating '{dtm_path}'")
        (
            functools.reduce(
                operator.__or__,
                (
                    pdal.Reader(
                        type="readers.las",
                        filename=path,
                        default_srs="EPSG:3059",
                    )
                    for path in paths
                ),
            )
            | pdal.Filter(
                type="filters.range",
                limits="Classification[2:2]",
            )
            | pdal.Writer(
                type="writers.gdal",
                filename=dtm_path,
                default_srs="EPSG:3059",
                resolution=1,
                gdaldriver="GTiff",
                data_type="float",
                nodata=255,
                output_type="idw",
                bounds=bounds,
            )
        ).execute_streaming()
        config.print(f"generating '{dtm_fillnodata_path}'")
        mezi_config.check_call(config, _TIF_FILLNODATA.format(input=dtm_path, output=dtm_fillnodata_path))
    for check, output, cmd in (
        (config.LGIA_TIF_TRI, head + "_tri.tif", _TIF_TRI),
        (config.LGIA_TIF_TPI, head + "_tpi.tif", _TIF_TPI),
        (config.LGIA_TIF_SLOPE, head + "_slope.tif", _TIF_SLOPE),
        (config.LGIA_TIF_ROUGHNESS, head + "_roughness.tif", _TIF_ROUGHNESS),
        (config.LGIA_TIF_ASPECT, head + "_aspect.tif", _TIF_ASPECT),
    ):
        if not check:
            continue
        config.OUTPUT_FILES_TO_ZIP.append(output)
        if force_invalidate or not os.path.isfile(output):
            config.print(f"generating '{output}'")
            mezi_config.check_call(config, cmd.format(input=dtm_fillnodata_path, output=output))
    for check, _name, limits in (
        (config.LGIA_TIF_HAG, "hag", None),
        (config.LGIA_TIF_CHM, "chm", "Classification[3:5]"),
        (config.LGIA_TIF_LCHM, "lchm", "Classification[3:3]"),
        (config.LGIA_TIF_MCHM, "mchm", "Classification[4:4]"),
        (config.LGIA_TIF_HCHM, "hchm", "Classification[5:5]"),
    ):
        if not check:
            continue
        output = f"{head}_{_name}.tif"
        config.OUTPUT_FILES_TO_ZIP.append(output)
        dsm_path = f"{head}_{_name}_dsm.tif"
        config.OUTPUT_FILES_TO_ZIP.append(dsm_path)
        dsm_fillnodata_path = f"{head}_{_name}_dsm_fillnodata.tif"
        config.OUTPUT_FILES_TO_ZIP.append(dsm_fillnodata_path)
        if force_invalidate or not os.path.isfile(output):
            config.print(f"generating '{dsm_path}'")
            inputs = functools.reduce(
                operator.__or__,
                (
                    pdal.Reader(
                        type="readers.las",
                        filename=path,
                        default_srs="EPSG:3059",
                    )
                    for path in paths
                ),
            )
            if limits:
                inputs |= pdal.Filter(type="filters.range", limits=limits)
            (
                inputs
                | pdal.Writer(
                    type="writers.gdal",
                    filename=dsm_path,
                    default_srs="EPSG:3059",
                    resolution=1,
                    gdaldriver="GTiff",
                    data_type="float",
                    nodata=255,
                    output_type="idw",
                    bounds=bounds,
                )
            ).execute_streaming()
            config.print(f"generating '{dsm_fillnodata_path}'")
            mezi_config.check_call(config, _TIF_FILLNODATA.format(input=dsm_path, output=dsm_fillnodata_path))
            config.print(f"generating '{output}'")
            mezi_config.check_call(config, _TIF_BA.format(input_dtm=dtm_fillnodata_path, input_dsm=dsm_fillnodata_path, output=output))


_TIF_MERGE_IGNORE = " -n '{ignore}'"
_TIF_MERGE_NODATA = " -a_nodata '{nodata}'"
_TIF_MERGE_INIT = " -init 0"
_TIF_MERGE = "gdal_merge -o '{output}' -ps 5 5 -tap{ignore}{nodata}{init} '{input}'"


def _merge_tif(
    config: mezi_config.DownloadConfig,
    tifs: Iterable[str],
    cache_path: str,
    force_invalidate: bool,
    output_suffix: str,
    ignore: Iterable[int] = (),
    nodata: int = 255,
) -> None:
    if not tifs:
        config.print("no tifs to merge")
        return
    os.makedirs(cache_path, exist_ok=True)
    output = os.path.join(cache_path, f"{config.name}{output_suffix}.tif")
    config.OUTPUT_FILES_TO_ZIP.append(output)
    config.print(f"merging {tifs} to '{output}'")
    if not force_invalidate and os.path.isfile(output):
        return
    mezi_config.check_call(
        config,
        _TIF_MERGE.format(
            output=output,
            ignore="".join(_TIF_MERGE_IGNORE.format(ignore=ignore) for ignore in ignore),
            nodata=_TIF_MERGE_NODATA.format(nodata=nodata) if nodata else "",
            init="" if nodata else _TIF_MERGE_INIT,
            input="' '".join(tifs),
        ),
    )


def _download_lgia_orto_rgb_tif(config: mezi_config.DownloadConfig) -> None:
    _merge_tif(
        config,
        _download(
            config,
            "lgia orto rgb tif",
            lambda tail: os.path.join(config.LGIA_ORTO_RGB_PATH_HEAD, tail.split("-")[0], f"{tail.replace('-', '_').replace('_', '-', 1)}.tif"),
            config.LGIA_ORTO_RGB_PATH_TAIL_NOMENKLATURA_GPKG_PATH,
            config.LGIA_ORTO_RGB_PATH_TAIL_NOMENKLATURA_LAYER,
            config.LGIA_ORTO_RGB_PATH_TAIL_NOMENKLATURA_FIELD,
            config.LGIA_ORTO_RGB_CACHE_PATH,
            config.LGIA_ORTO_RGB_CACHE_FORCE_INVALIDATE,
        ),
        config.LGIA_ORTO_RGB_CACHE_PATH,
        config.LGIA_ORTO_RGB_CACHE_FORCE_INVALIDATE,
        "_orto_rgb",
    )


def _download_lgia_orto_rgb_tfw(config: mezi_config.DownloadConfig) -> None:
    _download(
        config,
        "lgia orto rgb tfw",
        lambda tail: os.path.join(config.LGIA_ORTO_RGB_PATH_HEAD, tail.split("-")[0], f"{tail.replace('-', '_').replace('_', '-', 1)}.tfw"),
        config.LGIA_ORTO_RGB_PATH_TAIL_NOMENKLATURA_GPKG_PATH,
        config.LGIA_ORTO_RGB_PATH_TAIL_NOMENKLATURA_LAYER,
        config.LGIA_ORTO_RGB_PATH_TAIL_NOMENKLATURA_FIELD,
        config.LGIA_ORTO_RGB_CACHE_PATH,
        config.LGIA_ORTO_RGB_CACHE_FORCE_INVALIDATE,
    )


def _download_lgia_orto_cir_tif(config: mezi_config.DownloadConfig) -> None:
    _merge_tif(
        config,
        _download(
            config,
            "lgia orto cir tif",
            lambda tail: os.path.join(config.LGIA_ORTO_CIR_PATH_HEAD, tail.split("-")[0], f"{tail.replace('-', '_').replace('_', '-', 1)}.tif"),
            config.LGIA_ORTO_CIR_PATH_TAIL_NOMENKLATURA_GPKG_PATH,
            config.LGIA_ORTO_CIR_PATH_TAIL_NOMENKLATURA_LAYER,
            config.LGIA_ORTO_CIR_PATH_TAIL_NOMENKLATURA_FIELD,
            config.LGIA_ORTO_CIR_CACHE_PATH,
            config.LGIA_ORTO_CIR_CACHE_FORCE_INVALIDATE,
        ),
        config.LGIA_ORTO_CIR_CACHE_PATH,
        config.LGIA_ORTO_CIR_CACHE_FORCE_INVALIDATE,
        "_orto_cir",
    )


def _download_lgia_orto_cir_tfw(config: mezi_config.DownloadConfig) -> None:
    _download(
        config,
        "lgia orto cir tfw",
        lambda tail: os.path.join(config.LGIA_ORTO_CIR_PATH_HEAD, tail.split("-")[0], f"{tail.replace('-', '_').replace('_', '-', 1)}.tfw"),
        config.LGIA_ORTO_CIR_PATH_TAIL_NOMENKLATURA_GPKG_PATH,
        config.LGIA_ORTO_CIR_PATH_TAIL_NOMENKLATURA_LAYER,
        config.LGIA_ORTO_CIR_PATH_TAIL_NOMENKLATURA_FIELD,
        config.LGIA_ORTO_CIR_CACHE_PATH,
        config.LGIA_ORTO_CIR_CACHE_FORCE_INVALIDATE,
    )


def _download_silava_dtw_10(config: mezi_config.DownloadConfig) -> None:
    _merge_tif(
        config,
        _download(
            config,
            "silava dtw 10 ha",
            lambda tail: f"{config.SILAVA_DTW_10_PATH_HEAD}{int(float(tail))}.tif",
            config.SILAVA_DTW_10_PATH_TAIL_NOMENKLATURA_GPKG_PATH,
            config.SILAVA_DTW_10_PATH_TAIL_NOMENKLATURA_LAYER,
            config.SILAVA_DTW_10_PATH_TAIL_NOMENKLATURA_FIELD,
            config.SILAVA_DTW_10_CACHE_PATH,
            config.SILAVA_DTW_10_CACHE_FORCE_INVALIDATE,
        ),
        config.SILAVA_DTW_10_CACHE_PATH,
        config.SILAVA_DTW_10_CACHE_FORCE_INVALIDATE,
        "_dtw_10",
    )


def _download_silava_dtw_30(config: mezi_config.DownloadConfig) -> None:
    _merge_tif(
        config,
        _download(
            config,
            "silava dtw 30 ha",
            lambda tail: f"{config.SILAVA_DTW_30_PATH_HEAD}{int(float(tail))}.tif",
            config.SILAVA_DTW_30_PATH_TAIL_NOMENKLATURA_GPKG_PATH,
            config.SILAVA_DTW_30_PATH_TAIL_NOMENKLATURA_LAYER,
            config.SILAVA_DTW_30_PATH_TAIL_NOMENKLATURA_FIELD,
            config.SILAVA_DTW_30_CACHE_PATH,
            config.SILAVA_DTW_30_CACHE_FORCE_INVALIDATE,
        ),
        config.SILAVA_DTW_30_CACHE_PATH,
        config.SILAVA_DTW_30_CACHE_FORCE_INVALIDATE,
        "_dtw_30",
    )


_WFS_TO_GPKG_SPAT = " -spat {bbox} -spat_srs 'EPSG:3059'"
_WFS_TO_GPKG = "ogr2ogr -overwrite -skipfailures{spat} -dim XY -t_srs 'EPSG:3059' -preserve_fid -makevalid '{output}' WFS:'{wfs}' '{layers}'"


def _download_wfs(
    config: mezi_config.DownloadConfig,
    cache_path: str,
    wfs_path: str,
    layers: Iterable[str],
    force_invalidate: bool,
) -> None:
    if not layers:
        config.print("no layers to download")
        return
    os.makedirs(cache_path, exist_ok=True)
    output = os.path.join(cache_path, f"{config.name}.gpkg")
    config.OUTPUT_FILES_TO_ZIP.append(output)
    config.print(f"downloading {layers} from '{wfs_path}'")
    if not force_invalidate and os.path.isfile(output):
        return
    mezi_config.check_call(config, _WFS_TO_GPKG.format(spat=_WFS_TO_GPKG_SPAT.format(bbox=" ".join(str(coord) for coord in config.bbox)) if config.bbox else "", output=output, wfs=wfs_path, layers="' '".join(layers)))


def _download_zmni_gpkg(config: mezi_config.DownloadConfig) -> None:
    _download_wfs(config, config.ZMNI_GPKG_CACHE_PATH, config.ZMNI_GPKG_WFS_PATH, config.ZMNI_GPKG_WFS_LAYERS, config.ZMNI_GPKG_CACHE_FORCE_INVALIDATE)


def _download_mantojums_gpkg(config: mezi_config.DownloadConfig) -> None:
    _download_wfs(config, config.MANTOJUMS_GPKG_CACHE_PATH, config.MANTOJUMS_GPKG_WFS_PATH, config.MANTOJUMS_GPKG_WFS_LAYERS, config.MANTOJUMS_GPKG_CACHE_FORCE_INVALIDATE)


def _download_ozols_gpkg(config: mezi_config.DownloadConfig) -> None:
    _download_wfs(config, config.OZOLS_GPKG_CACHE_PATH, config.OZOLS_GPKG_WFS_PATH, config.OZOLS_GPKG_WFS_LAYERS, config.OZOLS_GPKG_CACHE_FORCE_INVALIDATE)


def _extract_zip(config: mezi_config.DownloadConfig, zip_path: str, cache_path: str, force_invalidate: bool, filters: Iterable[str] = ()) -> list[str]:
    config.print(f"extracting zip from '{zip_path}' to '{cache_path}'")
    extracted = set()
    with zipfile.ZipFile(zip_path) as _zip:
        for info in _zip.infolist():
            if info.is_dir() or (filters and not any(_filter in info.filename for _filter in filters)):
                continue
            info.filename = os.path.basename(info.filename)
            config.print(f"extracting '{info.filename}' to '{cache_path}'")
            path = os.path.join(cache_path, info.filename)
            if force_invalidate or not os.path.isfile(path):
                _zip.extract(info, cache_path)
            extracted.add(path)
    return sorted(extracted)


def _download_zip(config: mezi_config.DownloadConfig, data_path: str, name: str, cache_path: str, force_invalidate: bool) -> None:
    _extract_zip(config, mezi_config.download_data(config, data_path, name, cache_path, force_invalidate), cache_path, force_invalidate)


def _read_to_gpkg(
    config: mezi_config.DownloadConfig,
    cache_path: str,
    gpkg_cache: str,
    file_buffers: Mapping[str, int | tuple[int, str, Mapping[str, int]]],
    force_invalidate: bool,
    exclude_field: str | None = None,
    exclude_values: list[Any] | None = None,
) -> None:
    gpkg_path = os.path.join(gpkg_cache, f"{config.name}.gpkg")
    config.OUTPUT_FILES_TO_ZIP.append(gpkg_path)
    config.print(f"writing {list(file_buffers)} to '{gpkg_path}'")
    if not force_invalidate and os.path.isfile(gpkg_path):
        return
    os.makedirs(gpkg_cache, exist_ok=True)
    misc.silent_unlink(gpkg_path)
    for index, (file, buffer) in enumerate(file_buffers.items()):
        gdf = geom.read_file(config, os.path.join(cache_path, file), bbox=config.bbox)
        if exclude_field and exclude_values:
            gdf = gdf[~gdf[exclude_field].isin(exclude_values)]
        if buffer:
            if isinstance(buffer, int | float):
                gdf["geometry"] = gdf["geometry"].buffer(buffer)  # pyright: ignore [reportCallIssue]
            else:
                default_buffer, field, buffers = buffer
                gdf["geometry"] = gdf["geometry"].buffer(default_buffer)  # pyright: ignore [reportCallIssue]
                for value, buffer in buffers.items():
                    _gdf = gdf[field] == value
                    gdf.loc[_gdf, "geometry"] = gdf.loc[_gdf, "geometry"].buffer(buffer - default_buffer)  # pyright: ignore [reportCallIssue, reportArgumentType]
        geom.write_file(config, gdf, gpkg_path, layer=file, mode="a" if index else "w", engine="pyogrio", unlink=not index)


def _download_biotopi(config: mezi_config.DownloadConfig) -> None:
    _download_zip(config, config.BIOTOPI_DATA_PATH, config.BIOTOPI_DATA_NAME, config.BIOTOPI_CACHE_PATH, config.BIOTOPI_CACHE_FORCE_INVALIDATE)
    _read_to_gpkg(config, config.BIOTOPI_CACHE_PATH, config.BIOTOPI_GPKG_CACHE_PATH, config.BIOTOPI_DATA_FILE_BUFFERS, config.BIOTOPI_CACHE_FORCE_INVALIDATE, config.BIOTOPI_EXCLUDE_FIELD, config.BIOTOPI_EXCLUDE_VALUES)


def _download_mikroliegumi(config: mezi_config.DownloadConfig) -> None:
    _download_zip(config, config.MIKROLIEGUMI_DATA_PATH, config.MIKROLIEGUMI_DATA_NAME, config.MIKROLIEGUMI_CACHE_PATH, config.MIKROLIEGUMI_CACHE_FORCE_INVALIDATE)
    _read_to_gpkg(config, config.MIKROLIEGUMI_CACHE_PATH, config.MIKROLIEGUMI_GPKG_CACHE_PATH, config.MIKROLIEGUMI_DATA_FILE_BUFFERS, config.MIKROLIEGUMI_CACHE_FORCE_INVALIDATE)


def _download_aizsargajamas_sugas(config: mezi_config.DownloadConfig) -> None:
    _download_zip(config, config.AIZSARGAJAMAS_SUGAS_DATA_PATH, config.AIZSARGAJAMAS_SUGAS_DATA_NAME, config.AIZSARGAJAMAS_SUGAS_CACHE_PATH, config.AIZSARGAJAMAS_SUGAS_CACHE_FORCE_INVALIDATE)
    _read_to_gpkg(config, config.AIZSARGAJAMAS_SUGAS_CACHE_PATH, config.AIZSARGAJAMAS_SUGAS_GPKG_CACHE_PATH, config.AIZSARGAJAMAS_SUGAS_DATA_FILE_BUFFERS, config.AIZSARGAJAMAS_SUGAS_CACHE_FORCE_INVALIDATE)


def _download_celi(config: mezi_config.DownloadConfig) -> None:
    force_invalidate = config.CELI_CACHE_FORCE_INVALIDATE
    celi_path = config.CELI_PATH
    cache_path = config.CELI_CACHE_PATH
    gpkg_cache_path = config.CELI_GPKG_CACHE_PATH
    ext = celi_path[-3:].lower()
    os.makedirs(gpkg_cache_path, exist_ok=True)
    if ext == "zip":
        path = os.path.join(cache_path, celi_path.split("/")[-1].split("\\")[-1])
        config.print(f"downloading zip from '{celi_path}' to '{path}'")
        if force_invalidate or not os.path.isfile(path):
            misc.download(celi_path, path, force_invalidate)
        gdf = mezi_config.concat(
            geom.filter(geom.read_file(config, extracted, None, config.bbox), config.wkt, config.bbox)[["geometry"]]  # pyright: ignore [reportArgumentType]
            for extracted in _extract_zip(config, path, cache_path, force_invalidate, config.CELI_PATH_ZIP_FILTERS)
            if extracted.endswith("shp")
        )
    elif ext == "shp":
        gdf = geom.filter(geom.read_file(config, celi_path, None, config.bbox), config.wkt, config.bbox)[["geometry"]]
    else:
        gdf = geom.filter(geom.read_file(config, celi_path, config.CELI_PATH_GPKG_LAYER, config.bbox), config.wkt, config.bbox)[["geometry"]]
    gpkg_path = os.path.join(gpkg_cache_path, f"{config.name}.gpkg")
    geom.write_file(config, gdf, gpkg_path, layer="celi", engine="pyogrio")
    config.OUTPUT_FILES_TO_ZIP.append(gpkg_path)


_GPKG_TO_PRESENCE_TIF_TE = " -te {bbox}"
_GPKG_TO_PRESENCE_TIF_NODATA = " -a_nodata '{nodata}'"
_GPKG_TO_PRESENCE_TIF_INIT = " -init 0"
_GPKG_TO_PRESENCE_TIF = "gdal_rasterize -at -burn 1 {layers} {nodata}{init}{te} -tr 1 1 -tap -ot '{output_type}' -q '{input_path}' '{output_path}'"


def _rasterize_presence(
    config: mezi_config.DownloadConfig,
    gpkg_path: str,
    cache_path: str,
    layers: Iterable[str],
    force_invalidate: bool,
    output_suffix: str,
    nodata: int = 255,
    output_type: str = "Byte",
) -> None:
    if not layers:
        config.print("no layers to rasterize")
        return
    input_path = os.path.join(gpkg_path, f"{config.name}.gpkg")
    os.makedirs(cache_path, exist_ok=True)
    output_path = os.path.join(cache_path, f"{config.name}{output_suffix}.tif")
    config.OUTPUT_FILES_TO_ZIP.append(output_path)
    config.print(f"rasterizing '{input_path}' to '{output_path}'")
    if not force_invalidate and os.path.isfile(output_path):
        return
    mezi_config.check_call(
        config,
        _GPKG_TO_PRESENCE_TIF.format(
            layers=" ".join(f"-l '{layer}'" for layer in layers),
            nodata=_GPKG_TO_PRESENCE_TIF_NODATA.format(nodata=nodata) if nodata else "",
            init="" if nodata else _GPKG_TO_PRESENCE_TIF_INIT,
            te=_GPKG_TO_PRESENCE_TIF_TE.format(bbox=" ".join(str(coord) for coord in config.bbox)) if config.bbox else "",
            output_type=output_type,
            input_path=input_path,
            output_path=output_path,
        ),
    )


def _rasterize_zmni_tif(config: mezi_config.DownloadConfig) -> None:
    _rasterize_presence(config, config.ZMNI_GPKG_CACHE_PATH, config.ZMNI_TIF_CACHE_PATH, config.ZMNI_GPKG_WFS_LAYERS, config.ZMNI_TIF_CACHE_FORCE_INVALIDATE, "_zmni")


def _rasterize_mantojums_tif(config: mezi_config.DownloadConfig) -> None:
    _rasterize_presence(config, config.MANTOJUMS_GPKG_CACHE_PATH, config.MANTOJUMS_TIF_CACHE_PATH, config.MANTOJUMS_GPKG_WFS_LAYERS, config.MANTOJUMS_TIF_CACHE_FORCE_INVALIDATE, "_mantojums")


def _rasterize_ozols_tif(config: mezi_config.DownloadConfig) -> None:
    _rasterize_presence(config, config.OZOLS_GPKG_CACHE_PATH, config.OZOLS_TIF_CACHE_PATH, config.OZOLS_GPKG_WFS_LAYERS, config.OZOLS_TIF_CACHE_FORCE_INVALIDATE, "_ozols")


_GPKG_TO_FIELD_TIF_TE = " -te {bbox}"
_GPKG_TO_FIELD_TIF_NODATA = " -a_nodata '{nodata}'"
_GPKG_TO_FIELD_TIF_INIT = " -init 0"
_GPKG_TO_FIELD_TIF = "gdal_rasterize -at -a '{field}' -l '{layer}'{nodata}{init}{te} -tr 1 1 -tap -ot '{output_type}' -q '{input_path}' '{output_path}'"


def _rasterize_field(
    config: mezi_config.DownloadConfig,
    gpkg_path: str,
    cache_path: str,
    layer: str,
    field: str,
    force_invalidate: bool,
    output_suffix: str,
    nodata: int = 255,
    output_type: str = "Float32",
) -> None:
    input_path = os.path.join(gpkg_path, f"{config.name}.gpkg")
    if not os.path.exists(input_path):
        config.print(f"{input_path} does not exist")
        return
    os.makedirs(cache_path, exist_ok=True)
    output_path = os.path.join(cache_path, f"{config.name}{output_suffix}.tif")
    config.OUTPUT_FILES_TO_ZIP.append(output_path)
    config.print(f"rasterizing '{input_path}' to '{output_path}'")
    if not force_invalidate and os.path.isfile(output_path):
        return
    mezi_config.check_call(
        config,
        _GPKG_TO_FIELD_TIF.format(
            field=field,
            layer=layer,
            nodata=_GPKG_TO_FIELD_TIF_NODATA.format(nodata=nodata) if nodata else "",
            init="" if nodata else _GPKG_TO_FIELD_TIF_INIT,
            te=_GPKG_TO_FIELD_TIF_TE.format(bbox=" ".join(str(coord) for coord in config.bbox)) if config.bbox else "",
            output_type=output_type,
            input_path=input_path,
            output_path=output_path,
        ),
    )


def _rasterize_mvr_tif(config: mezi_config.DownloadConfig) -> None:
    apgs_cache_path = os.path.join(config.MVR_CACHE_PATH, "apgs")
    _rasterize_field(config, apgs_cache_path, config.MVR_TIF_CACHE_PATH, "apgs", "biez", config.MVR_TIF_CACHE_FORCE_INVALIDATE, "_biez")
    _rasterize_field(config, apgs_cache_path, config.MVR_TIF_CACHE_PATH, "apgs", "arstnieciba", config.MVR_TIF_CACHE_FORCE_INVALIDATE, "_arstnieciba")
    _rasterize_field(config, apgs_cache_path, config.MVR_TIF_CACHE_PATH, "apgs", "bruklenes", config.MVR_TIF_CACHE_FORCE_INVALIDATE, "_bruklenes")
    _rasterize_field(config, apgs_cache_path, config.MVR_TIF_CACHE_PATH, "apgs", "fitoremediacija", config.MVR_TIF_CACHE_FORCE_INVALIDATE, "_fitoremediacija")
    _rasterize_field(config, apgs_cache_path, config.MVR_TIF_CACHE_PATH, "apgs", "floristika", config.MVR_TIF_CACHE_FORCE_INVALIDATE, "_floristika")
    _rasterize_field(config, apgs_cache_path, config.MVR_TIF_CACHE_PATH, "apgs", "kosmetika", config.MVR_TIF_CACHE_FORCE_INVALIDATE, "_kosmetika")
    _rasterize_field(config, apgs_cache_path, config.MVR_TIF_CACHE_PATH, "apgs", "mellenes", config.MVR_TIF_CACHE_FORCE_INVALIDATE, "_mellenes")
    _rasterize_field(config, apgs_cache_path, config.MVR_TIF_CACHE_PATH, "apgs", "noturiba", config.MVR_TIF_CACHE_FORCE_INVALIDATE, "_noturiba")
    _rasterize_field(config, apgs_cache_path, config.MVR_TIF_CACHE_PATH, "apgs", "pievilciba", config.MVR_TIF_CACHE_FORCE_INVALIDATE, "_pievilciba")
    _rasterize_field(config, apgs_cache_path, config.MVR_TIF_CACHE_PATH, "apgs", "rekreacija", config.MVR_TIF_CACHE_FORCE_INVALIDATE, "_rekreacija")
    _rasterize_field(config, apgs_cache_path, config.MVR_TIF_CACHE_PATH, "apgs", "troksnis", config.MVR_TIF_CACHE_FORCE_INVALIDATE, "_troksnis")
    _rasterize_field(config, apgs_cache_path, config.MVR_TIF_CACHE_PATH, "apgs", "ugunsbistamiba", config.MVR_TIF_CACHE_FORCE_INVALIDATE, "_ugunsbistamiba")
    _rasterize_field(config, apgs_cache_path, config.MVR_TIF_CACHE_PATH, "apgs", "saimn_d_ie", config.MVR_TIF_CACHE_FORCE_INVALIDATE, "_saimn_d_ie")
    for col, *_ in config.MVR_RULES:
        _rasterize_field(config, apgs_cache_path, config.MVR_TIF_CACHE_PATH, "apgs", col, config.MVR_TIF_CACHE_FORCE_INVALIDATE, f"_{col}")


def _rasterize_biotopi_tif(config: mezi_config.DownloadConfig) -> None:
    _rasterize_presence(config, config.BIOTOPI_GPKG_CACHE_PATH, config.BIOTOPI_TIF_CACHE_PATH, config.BIOTOPI_DATA_FILE_BUFFERS, config.BIOTOPI_TIF_CACHE_FORCE_INVALIDATE, "_biotopi")


def _rasterize_mikroliegumi_tif(config: mezi_config.DownloadConfig) -> None:
    _rasterize_presence(config, config.MIKROLIEGUMI_GPKG_CACHE_PATH, config.MIKROLIEGUMI_TIF_CACHE_PATH, config.MIKROLIEGUMI_DATA_FILE_BUFFERS, config.MIKROLIEGUMI_TIF_CACHE_FORCE_INVALIDATE, "_mikroliegumi")


def _rasterize_aizsargajamas_sugas_tif(config: mezi_config.DownloadConfig) -> None:
    _rasterize_presence(
        config,
        config.AIZSARGAJAMAS_SUGAS_GPKG_CACHE_PATH,
        config.AIZSARGAJAMAS_SUGAS_TIF_CACHE_PATH,
        config.AIZSARGAJAMAS_SUGAS_DATA_FILE_BUFFERS,
        config.AIZSARGAJAMAS_SUGAS_TIF_CACHE_FORCE_INVALIDATE,
        "_aizsargajamas_sugas",
    )


def _rasterize_celi_tif(config: mezi_config.DownloadConfig) -> None:
    _rasterize_presence(config, config.CELI_GPKG_CACHE_PATH, config.CELI_TIF_CACHE_PATH, ("celi",), config.CELI_TIF_CACHE_FORCE_INVALIDATE, "_celi")


_BZI_CALC_DTW = "d - d + ((d > 0) & (d <= 0.2)) + (d <= 0) * 2"
_BZI_CALC_SLOPE = "s - s + ((s >= 10) & (s <= 25)) + (s > 25) * 2"
_BZI_CALC_MVR = "isin(m, (1, 2)) * 2"
_BZI_CALC_BIOTOPI = "(b != 255) * b * 2"
_BZI_CALC_AIZSARGAJAMAS_SUGAS = "(a != 255) * a * 2"
_BZI_CALC_CALC = f"max(concatenate((({_BZI_CALC_DTW})[..., None], ({_BZI_CALC_SLOPE})[..., None], ({_BZI_CALC_MVR})[..., None], ({_BZI_CALC_BIOTOPI})[..., None], ({_BZI_CALC_AIZSARGAJAMAS_SUGAS})[..., None]), 2), 2)"
_BZI_CALC = f"gdal_calc -d '{{dtw}}' -s '{{slope}}' -m '{{mvr}}' -b '{{biotopi}}' -a '{{aizsargajamas_sugas}}' --calc='{_BZI_CALC_CALC}' --outfile '{{output}}' --NoDataValue 'none' --hideNoData --extent 'intersect' --overwrite --type 'Byte' --projectionCheck"  # noqa: E501
# _BZI_CALC = """gdal_calc -d '{dtw}' -s '{slope}' -m '{mvr}' -b '{biotopi}' -a '{aizsargajamas_sugas}' --calc='exec("from mezi import download") or download._bzi_calc(d, s, m, b, a)' --outfile '{output}' --NoDataValue 'none' --hideNoData --extent 'intersect' --overwrite --type 'Byte' --projectionCheck"""  # noqa: E501
_BZI_RESAMPLE_TE = " -te {bbox} -te_srs 'EPSG:3059'"
_BZI_RESAMPLE = "gdalwarp -t_srs 'EPSG:3059'{te} -tr 1 1 -tap -ovr NONE -dstnodata '255' -overwrite '{input_path}' '{output_path}'"


def _bzi_calc(  # pyright: ignore [reportUnusedFunction]
    d: np.ndarray[tuple[int, int], np.dtype[np.float32]],
    s: np.ndarray[tuple[int, int], np.dtype[np.float32]],
    m: np.ndarray[tuple[int, int], np.dtype[np.float32]],
    b: np.ndarray[tuple[int, int], np.dtype[np.byte]],
    a: np.ndarray[tuple[int, int], np.dtype[np.byte]],
) -> np.ndarray[tuple[int, int], np.dtype[np.byte]]:
    _d = d - d + ((d > 0) & (d <= 0.2)) + (d <= 0) * 2  # noqa: PLR2004
    _s = s - s + ((s >= 10) & (s <= 25)) + (s > 25) * 2  # noqa: PLR2004
    _m = np.isin(m, (1, 2)) * 2
    _b = (b != 255) * b * 2  # noqa: PLR2004
    _a = (a != 255) * a * 2  # noqa: PLR2004
    return np.max(np.concatenate((_d[..., None], _s[..., None], _m[..., None], _b[..., None], _a[..., None]), 2), 2)


def _rasterize_bzi_tif(config: mezi_config.DownloadConfig) -> None:
    tif_path = os.path.join(config.BZI_TIF_CACHE_PATH, f"{config.name}_bzi.tif")
    config.OUTPUT_FILES_TO_ZIP.append(tif_path)
    if not config.BZI_TIF_CACHE_FORCE_INVALIDATE and os.path.isfile(tif_path):
        return
    os.makedirs(config.BZI_TIF_CACHE_PATH, exist_ok=True)
    _read_to_gpkg(config, config.AIZSARGAJAMAS_SUGAS_CACHE_PATH, config.BZI_TIF_CACHE_PATH, config.BZI_AIZSARGAJAMAS_SUGAS_DATA_FILE_BUFFERS, config.BZI_TIF_CACHE_FORCE_INVALIDATE)
    _rasterize_presence(
        config,
        config.BZI_TIF_CACHE_PATH,
        config.BZI_TIF_CACHE_PATH,
        config.BZI_AIZSARGAJAMAS_SUGAS_DATA_FILE_BUFFERS,
        config.BZI_TIF_CACHE_FORCE_INVALIDATE,
        "_aizsargajamas_sugas",
    )
    dtw_path = os.path.join(config.SILAVA_DTW_30_CACHE_PATH, f"{config.name}_dtw_30.tif") if config.BZI_TIF_DTW_30 else os.path.join(config.SILAVA_DTW_10_CACHE_PATH, f"{config.name}_dtw_10.tif")
    slope_path = os.path.join(config.LGIA_TIF_CACHE_PATH, f"{config.name}_slope.tif")
    mvr_path = os.path.join(config.MVR_TIF_CACHE_PATH, f"{config.name}_saimn_d_ie.tif")
    biotopi_path = os.path.join(config.BIOTOPI_TIF_CACHE_PATH, f"{config.name}_biotopi.tif")
    aizsargajamas_sugas_path = os.path.join(config.BZI_TIF_CACHE_PATH, f"{config.name}_aizsargajamas_sugas.tif")
    dtw_resample_path = os.path.join(config.BZI_TIF_CACHE_PATH, f"{config.name}_dtw.tif")
    config.print(f"generating '{dtw_resample_path}'")
    mezi_config.check_call(
        config,
        _BZI_RESAMPLE.format(
            te=_BZI_RESAMPLE_TE.format(bbox=" ".join(str(coord) for coord in config.bbox)) if config.bbox else "",
            input_path=dtw_path,
            output_path=dtw_resample_path,
        ),
    )
    config.print(f"generating '{tif_path}'")
    mezi_config.check_call(
        config,
        _BZI_CALC.format(
            dtw=dtw_resample_path,
            slope=slope_path,
            mvr=mvr_path,
            biotopi=biotopi_path,
            aizsargajamas_sugas=aizsargajamas_sugas_path,
            output=tif_path,
        ),
    )


def _zip_data(config: mezi_config.DownloadConfig) -> None:
    if config.OUTPUT_ZIP_CONFIG_PATH:
        path = os.path.join(config.OUTPUT_ZIP_CONFIG_PATH, f"{config.name}.json")
        config.OUTPUT_FILES_TO_ZIP.append(path)
        config.dump(path)
    os.makedirs(os.path.dirname(config.OUTPUT_ZIP_PATH), exist_ok=True)
    paths = tuple(os.path.abspath(path) for path in sorted(config.OUTPUT_FILES_TO_ZIP))
    root = os.path.commonpath(paths) if paths else None
    with zipfile.ZipFile(config.OUTPUT_ZIP_PATH, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as _zip:
        for path in paths:
            zip_path = os.path.relpath(path.replace(config.name + "_", "").replace(config.name, "data"), root)
            config.print(f"compressing '{path}' to '{zip_path}' in '{config.OUTPUT_ZIP_PATH}'")
            _zip.write(path, zip_path)


def extra_main(config: mezi_config.DownloadConfig = mezi_config.DownloadConfig(), wkt: shapely.Geometry | None = geom.DEFAULT_WKT, bbox: tuple[float, ...] | None = geom.DEFAULT_BBOX) -> int:
    # config.dump("./download.py.json")
    # raise
    config.wkt = wkt
    config.bbox = _bbox(wkt, bbox)
    config.name = name(wkt, bbox)
    if config.LGIA_LAS_DOWNLOAD:
        _download_lgia_las(config)
    if config.LGIA_ORTO_RGB_DOWNLOAD_TIF:
        _download_lgia_orto_rgb_tif(config)
    if config.LGIA_ORTO_RGB_DOWNLOAD_TFW:
        _download_lgia_orto_rgb_tfw(config)
    if config.LGIA_ORTO_CIR_DOWNLOAD_TIF:
        _download_lgia_orto_cir_tif(config)
    if config.LGIA_ORTO_CIR_DOWNLOAD_TFW:
        _download_lgia_orto_cir_tfw(config)
    if config.SILAVA_DTW_10_DOWNLOAD:
        _download_silava_dtw_10(config)
    if config.SILAVA_DTW_30_DOWNLOAD:
        _download_silava_dtw_30(config)
    if config.ZMNI_GPKG_DOWNLOAD:
        _download_zmni_gpkg(config)
    if config.MANTOJUMS_GPKG_DOWNLOAD:
        _download_mantojums_gpkg(config)
    if config.OZOLS_GPKG_DOWNLOAD:
        _download_ozols_gpkg(config)
    if config.MVR_DOWNLOAD:
        mvr.download_mvr(config)
    if config.BIOTOPI_DOWNLOAD:
        _download_biotopi(config)
    if config.MIKROLIEGUMI_DOWNLOAD:
        _download_mikroliegumi(config)
    if config.AIZSARGAJAMAS_SUGAS_DOWNLOAD:
        _download_aizsargajamas_sugas(config)
    if config.CELI_DOWNLOAD:
        _download_celi(config)
    if config.ZMNI_TIF_RASTERIZE:
        _rasterize_zmni_tif(config)
    if config.MANTOJUMS_TIF_RASTERIZE:
        _rasterize_mantojums_tif(config)
    if config.OZOLS_TIF_RASTERIZE:
        _rasterize_ozols_tif(config)
    if config.MVR_TIF_RASTERIZE:
        _rasterize_mvr_tif(config)
    if config.BIOTOPI_TIF_RASTERIZE:
        _rasterize_biotopi_tif(config)
    if config.MIKROLIEGUMI_TIF_RASTERIZE:
        _rasterize_mikroliegumi_tif(config)
    if config.AIZSARGAJAMAS_SUGAS_TIF_RASTERIZE:
        _rasterize_aizsargajamas_sugas_tif(config)
    if config.CELI_TIF_RASTERIZE:
        _rasterize_celi_tif(config)
    if config.BZI_TIF_RASTERIZE:
        _rasterize_bzi_tif(config)
    if config.TESSELLATE:
        tessellate.tessellate(config)
    if config.OUTPUT_ZIP_PATH:
        _zip_data(config)
    return 0


parse, main = boilerplate.get_parse_main(mezi_config.DownloadConfig, __file__, extra_main)  # pyright: ignore [reportArgumentType]


if __name__ == "__main__":
    sys.exit(main(**vars(parse())))  # pyright: ignore [reportCallIssue]
