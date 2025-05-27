# Author: LU, Rīgas Meži, SunGIS
# Created: 2024
# License: EUPL License

# Dependencies: environment.yml
# Python Version: 3.12+

import json
import os
import shlex
import subprocess
from collections.abc import Iterable

import geopandas as gpd
import pandas as pd

from mezi.utils import config, misc


class DownloadConfig(config.Config):
    PRINT_PROGRESS_BAR = True
    PRINT_CMD = False

    OUTPUT_ZIP_PATH = os.path.join("..", "data.zip")
    OUTPUT_ZIP_CONFIG_PATH = os.path.join("..", "data", "config")
    OUTPUT_FILES_TO_ZIP = []

    LGIA_LAS_DOWNLOAD = True
    LGIA_LAS_CACHE_PATH = os.path.join("..", "data", "lgia", "las")
    LGIA_LAS_CACHE_FORCE_INVALIDATE = False
    LGIA_LAS_PATH_HEAD = "https://s3.storage.pub.lvdc.gov.lv/lgia-opendata/las"
    LGIA_LAS_PATH_TAIL_NOMENKLATURA_GPKG_PATH = os.path.join("..", "basedata", "nomenklaturas.gpkg")
    LGIA_LAS_PATH_TAIL_NOMENKLATURA_LAYER = "tks1_1000"
    LGIA_LAS_PATH_TAIL_NOMENKLATURA_FIELD = "numurs"

    LGIA_TIF_CACHE_PATH = os.path.join("..", "data", "lgia", "tif")
    LGIA_TIF_CACHE_FORCE_INVALIDATE = False
    LGIA_TIF_DTM = True
    LGIA_TIF_TRI = True
    LGIA_TIF_TPI = True
    LGIA_TIF_SLOPE = True
    LGIA_TIF_ROUGHNESS = True
    LGIA_TIF_ASPECT = True
    LGIA_TIF_HAG = True
    LGIA_TIF_CHM = True
    LGIA_TIF_LCHM = True
    LGIA_TIF_MCHM = True
    LGIA_TIF_HCHM = True

    LGIA_ORTO_RGB_DOWNLOAD_TIF = False
    LGIA_ORTO_RGB_DOWNLOAD_TFW = False
    LGIA_ORTO_RGB_CACHE_PATH = os.path.join("..", "data", "lgia", "orto", "rgb")
    LGIA_ORTO_RGB_CACHE_FORCE_INVALIDATE = False
    LGIA_ORTO_RGB_PATH_HEAD = "https://s3.storage.pub.lvdc.gov.lv/lgia-opendata/ortofoto_rgb_v6"
    LGIA_ORTO_RGB_PATH_TAIL_NOMENKLATURA_GPKG_PATH = os.path.join("..", "basedata", "nomenklaturas.gpkg")
    LGIA_ORTO_RGB_PATH_TAIL_NOMENKLATURA_LAYER = "tks1_5000"
    LGIA_ORTO_RGB_PATH_TAIL_NOMENKLATURA_FIELD = "numurs"

    LGIA_ORTO_CIR_DOWNLOAD_TIF = False
    LGIA_ORTO_CIR_DOWNLOAD_TFW = False
    LGIA_ORTO_CIR_CACHE_PATH = os.path.join("..", "data", "lgia", "orto", "cir")
    LGIA_ORTO_CIR_CACHE_FORCE_INVALIDATE = False
    LGIA_ORTO_CIR_PATH_HEAD = "https://s3.storage.pub.lvdc.gov.lv/lgia-opendata/ortofoto_cir_v6"
    LGIA_ORTO_CIR_PATH_TAIL_NOMENKLATURA_GPKG_PATH = os.path.join("..", "basedata", "nomenklaturas.gpkg")
    LGIA_ORTO_CIR_PATH_TAIL_NOMENKLATURA_LAYER = "tks1_5000"
    LGIA_ORTO_CIR_PATH_TAIL_NOMENKLATURA_FIELD = "numurs"

    SILAVA_DTW_10_DOWNLOAD = False
    SILAVA_DTW_10_CACHE_PATH = os.path.join("..", "data", "silava", "dtw", "10")
    SILAVA_DTW_10_CACHE_FORCE_INVALIDATE = False
    SILAVA_DTW_10_PATH_HEAD = "https://silava.forestradar.com/data/rastra-dati/DTW/DTW_10ha_"
    SILAVA_DTW_10_PATH_TAIL_NOMENKLATURA_GPKG_PATH = os.path.join("..", "basedata", "nomenklaturas.gpkg")
    SILAVA_DTW_10_PATH_TAIL_NOMENKLATURA_LAYER = "baltic_grid"
    SILAVA_DTW_10_PATH_TAIL_NOMENKLATURA_FIELD = "id"

    SILAVA_DTW_30_DOWNLOAD = True
    SILAVA_DTW_30_CACHE_PATH = os.path.join("..", "data", "silava", "dtw", "30")
    SILAVA_DTW_30_CACHE_FORCE_INVALIDATE = False
    SILAVA_DTW_30_PATH_HEAD = "https://silava.forestradar.com/data/rastra-dati/DTW_30ha/DTW_30ha_"
    SILAVA_DTW_30_PATH_TAIL_NOMENKLATURA_GPKG_PATH = os.path.join("..", "basedata", "nomenklaturas.gpkg")
    SILAVA_DTW_30_PATH_TAIL_NOMENKLATURA_LAYER = "baltic_grid"
    SILAVA_DTW_30_PATH_TAIL_NOMENKLATURA_FIELD = "id"

    ZMNI_GPKG_DOWNLOAD = True
    ZMNI_GPKG_CACHE_PATH = os.path.join("..", "data", "zmni", "gpkg")
    ZMNI_GPKG_CACHE_FORCE_INVALIDATE = False
    ZMNI_GPKG_WFS_PATH = "https://lvmgeoserver.lvm.lv/geoserver/zmni/ows"
    ZMNI_GPKG_WFS_LAYERS = [
        "zmni:zmni_dam",
        "zmni:zmni_watercourses",
        "zmni:zmni_ditches",
        "zmni:zmni_bigdraincollectors",
        "zmni:zmni_statecontrolledrivers",
        "zmni:zmni_waterdrainditches",
        "zmni:zmni_stateriverspolygon",
    ]

    MANTOJUMS_GPKG_DOWNLOAD = True
    MANTOJUMS_GPKG_CACHE_PATH = os.path.join("..", "data", "mantojums", "gpkg")
    MANTOJUMS_GPKG_CACHE_FORCE_INVALIDATE = False
    MANTOJUMS_GPKG_WFS_PATH = "https://geoserver.mantojums.lv/geoserver/ows"
    MANTOJUMS_GPKG_WFS_LAYERS = [
        "monument:monuments_culturalobject_unesco",
        "monument:monuments_protectionzone_public",
    ]

    OZOLS_GPKG_DOWNLOAD = True
    OZOLS_GPKG_CACHE_PATH = os.path.join("..", "data", "ozols", "gpkg")
    OZOLS_GPKG_CACHE_FORCE_INVALIDATE = False
    OZOLS_GPKG_WFS_PATH = "https://ozols.gov.lv/arcgis/services/OZOLS_DABASDATI_PUB_INSPIRE/MapServer/WFSServer"
    OZOLS_GPKG_WFS_LAYERS = [
        "OZOLS_DABASDATI_PUB_INSPIRE:IADT_aizsargajamie_koki",
        "OZOLS_DABASDATI_PUB_INSPIRE:Mikroliegumi",
        "OZOLS_DABASDATI_PUB_INSPIRE:Mikroliegumu_buferzonas",
        "OZOLS_DABASDATI_PUB_INSPIRE:IADT_pamatteritorijas",
        "OZOLS_DABASDATI_PUB_INSPIRE:IADT_zonejums",
    ]

    MVR_DOWNLOAD = True
    MVR_CACHE_PATH = os.path.join("..", "data", "mvr", "shp")
    MVR_CACHE_FORCE_INVALIDATE = False
    MVR_DIRECT_DATA_PATH = ""
    MVR_DATA_PATH = "https://data.gov.lv/dati/lv/dataset/meza-valsts-registra-meza-dati.jsonld"
    MVR_TERITORIJAS_VM_GPKG_PATH = os.path.join("..", "basedata", "vmd_teritorijas.gpkg")
    MVR_TERITORIJAS_VM_LAYER = "vm_cleaned"
    MVR_TERITORIJAS_VM_FIELD = "vmd_head_1"
    MVR_TERITORIJAS_MZN_GPKG_PATH = os.path.join("..", "basedata", "vmd_teritorijas.gpkg")
    MVR_TERITORIJAS_MZN_LAYER = "mzn_cleaned"
    MVR_TERITORIJAS_MZN_FIELD = "vmd_forest"
    MVR_RULES = [
        ("bioremediacija", "", [os.path.join("..", "basedata", "csv", "bioremediacija.csv")]),
        ("filtracija", "", [os.path.join("..", "basedata", "csv", "filtracija.csv")]),
        (
            "pludi",
            "int(ZKAT) != 10",
            [
                os.path.join("..", "basedata", "csv", "pludiMT.csv"),
                os.path.join("..", "basedata", "csv", "pludiZKAT.csv"),
            ],
        ),
        (
            "klimats",
            "2 if int(ZKAT) != 10 else (1 if int(VGR) in {1, 2, 3} or int(A10) < 60 else 0)",
            [
                os.path.join("..", "basedata", "csv", "klima_MT.csv"),
                os.path.join("..", "basedata", "csv", "klima_MT_123.csv"),
                os.path.join("..", "basedata", "csv", "klima_ZKAT.csv"),
            ],
        ),
    ]

    BIOTOPI_DOWNLOAD = True
    BIOTOPI_EXCLUDE_FIELD = "code_ec"
    BIOTOPI_EXCLUDE_VALUES = ["2180"]
    BIOTOPI_CACHE_PATH = os.path.join("..", "data", "dap", "biotopi", "shp")
    BIOTOPI_GPKG_CACHE_PATH = os.path.join("..", "data", "dap", "biotopi", "gpkg")
    BIOTOPI_CACHE_FORCE_INVALIDATE = False
    BIOTOPI_DATA_PATH = "https://data.gov.lv/dati/lv/dataset/aizsargajamas-dzivotnes-biotopi.jsonld"
    BIOTOPI_DATA_NAME = "Aizsargājamās dzīvotnes - biotopi (shapefile)."
    BIOTOPI_DATA_FILE_BUFFERS = {
        "Biotopi.shp": 0,
    }

    MIKROLIEGUMI_DOWNLOAD = True
    MIKROLIEGUMI_CACHE_PATH = os.path.join("..", "data", "dap", "mikroliegumi", "shp")
    MIKROLIEGUMI_GPKG_CACHE_PATH = os.path.join("..", "data", "dap", "mikroliegumi", "gpkg")
    MIKROLIEGUMI_CACHE_FORCE_INVALIDATE = False
    MIKROLIEGUMI_DATA_PATH = "https://data.gov.lv/dati/lv/dataset/mikroliegumi.jsonld"
    MIKROLIEGUMI_DATA_NAME = "Mikroliegumi un to buferzonas (shapefile)"
    MIKROLIEGUMI_DATA_FILE_BUFFERS = {
        "MikroliegumuBuferzonas.shp": 0,
        "Mikroliegumi.shp": 0,
    }

    AIZSARGAJAMAS_SUGAS_DOWNLOAD = True
    AIZSARGAJAMAS_SUGAS_CACHE_PATH = os.path.join("..", "data", "dap", "aizsargajamas_sugas", "shp")
    AIZSARGAJAMAS_SUGAS_GPKG_CACHE_PATH = os.path.join("..", "data", "dap", "aizsargajamas_sugas", "gpkg")
    AIZSARGAJAMAS_SUGAS_CACHE_FORCE_INVALIDATE = False
    AIZSARGAJAMAS_SUGAS_DATA_PATH = "https://data.gov.lv/dati/lv/dataset/aizsargajamo-sugu-atradnes.jsonld"
    AIZSARGAJAMAS_SUGAS_DATA_NAME = "Aizsargājamo sugu atradnes (shapefile)"
    AIZSARGAJAMAS_SUGAS_DATA_FILE_BUFFERS = {
        "Sugas_punkti.shp": 10,
        "Sugas_laukumi.shp": 0,
    }

    CELI_DOWNLOAD = True
    CELI_CACHE_PATH = os.path.join("..", "data", "celi", "shp")
    CELI_GPKG_CACHE_PATH = os.path.join("..", "data", "celi", "gpkg")
    CELI_CACHE_FORCE_INVALIDATE = False
    CELI_PATH = "https://download.geofabrik.de/europe/latvia-latest-free.shp.zip"
    CELI_PATH_ZIP_FILTERS = ["gis_osm_roads"]
    CELI_PATH_GPKG_LAYER = "RM_celi"

    ZMNI_TIF_RASTERIZE = True
    ZMNI_TIF_CACHE_PATH = os.path.join("..", "data", "zmni", "tif")
    ZMNI_TIF_CACHE_FORCE_INVALIDATE = False

    MANTOJUMS_TIF_RASTERIZE = True
    MANTOJUMS_TIF_CACHE_PATH = os.path.join("..", "data", "mantojums", "tif")
    MANTOJUMS_TIF_CACHE_FORCE_INVALIDATE = False

    OZOLS_TIF_RASTERIZE = True
    OZOLS_TIF_CACHE_PATH = os.path.join("..", "data", "ozols", "tif")
    OZOLS_TIF_CACHE_FORCE_INVALIDATE = False

    MVR_TIF_RASTERIZE = True
    MVR_TIF_CACHE_PATH = os.path.join("..", "data", "mvr", "tif")
    MVR_TIF_CACHE_FORCE_INVALIDATE = False

    BIOTOPI_TIF_RASTERIZE = True
    BIOTOPI_TIF_CACHE_PATH = os.path.join("..", "data", "dap", "biotopi", "tif")
    BIOTOPI_TIF_CACHE_FORCE_INVALIDATE = False

    MIKROLIEGUMI_TIF_RASTERIZE = True
    MIKROLIEGUMI_TIF_CACHE_PATH = os.path.join("..", "data", "dap", "mikroliegumi", "tif")
    MIKROLIEGUMI_TIF_CACHE_FORCE_INVALIDATE = False

    AIZSARGAJAMAS_SUGAS_TIF_RASTERIZE = True
    AIZSARGAJAMAS_SUGAS_TIF_CACHE_PATH = os.path.join("..", "data", "dap", "aizsargajamas_sugas", "tif")
    AIZSARGAJAMAS_SUGAS_TIF_CACHE_FORCE_INVALIDATE = False

    CELI_TIF_RASTERIZE = True
    CELI_TIF_CACHE_PATH = os.path.join("..", "data", "celi", "tif")
    CELI_TIF_CACHE_FORCE_INVALIDATE = False

    BZI_TIF_RASTERIZE = True
    BZI_TIF_CACHE_PATH = os.path.join("..", "data", "bzi", "tif")
    BZI_TIF_CACHE_FORCE_INVALIDATE = False
    BZI_TIF_DTW_30 = True
    BZI_AIZSARGAJAMAS_SUGAS_DATA_FILE_BUFFERS = {
        "Sugas_punkti.shp": 50,
        "Sugas_laukumi.shp": 50,
    }

    TESSELLATE = True
    TESSELLATE_REMOVE_BEFORE_TESSELLATION = {
        MANTOJUMS_GPKG_CACHE_PATH: MANTOJUMS_GPKG_WFS_LAYERS,
        OZOLS_GPKG_CACHE_PATH: OZOLS_GPKG_WFS_LAYERS,
        BIOTOPI_GPKG_CACHE_PATH: BIOTOPI_DATA_FILE_BUFFERS,
        MIKROLIEGUMI_GPKG_CACHE_PATH: MIKROLIEGUMI_DATA_FILE_BUFFERS,
    }
    TESSELLATE_REMOVE_AFTER_TESSELLATION = {
        AIZSARGAJAMAS_SUGAS_GPKG_CACHE_PATH: AIZSARGAJAMAS_SUGAS_DATA_FILE_BUFFERS,
    }
    TESSELLATE_SEED = 42
    TESSELLATE_POINT_METHOD = "tif"
    TESSELLATE_POINT_TIF_PATH = LGIA_TIF_CACHE_PATH
    TESSELLATE_POINT_TIF_SUFFIX = "_hchm_dsm_fillnodata"
    TESSELLATE_POINT_TIF_RESAMPLE = True
    TESSELLATE_POINT_TIF_RESAMPLE_METHOD = "average"
    TESSELLATE_POINT_TIF_RESAMPLE_RESOLUTION = 10
    TESSELLATE_POINT_MIN_DISTANCE = 35
    TESSELLATE_POINT_INCLUDE_BORDER = False
    TESSELLATE_POINT_INCLUDE_BORDER_BEFORE_DISTANCE_FILTER = False
    TESSELLATE_POLYGON_METHOD = "voronoi"
    TESSELLATE_POLYGON_MIN_AREA = 1
    TESSELLATE_POLYGON_SPLIT_PATH = os.path.join(MVR_CACHE_PATH, "apgs")
    TESSELLATE_POLYGON_SPLIT_LAYER = "apgs"
    TESSELLATE_POLYGON_SPLIT_ADD_FIELDS = ["kadastrs", "kvart", "nog", "anog"]
    TESSELLATE_POLYGON_SPLIT_INCLUDE_RULE = "zkat == '10' and a10 >= 60"
    TESSELLATE_POLYGON_SIMPLIFY_TOLERANCE = 1
    TESSELLATE_STATS = ["min", "max", "mean", "std", "var"]
    TESSELLATE_PERCENTILES = [1, 25, 50, 75, 99]
    TESSELLATE_CALC = {"mean_div_std": "data.mean() / data.std()"}
    TESSELLATE_VALUE_PERCENTILES = {"bzi": [0, 1, 2]}
    TESSELLATE_GPKG_CACHE_PATH = os.path.join("..", "data", "tessellation", "gpkg")
    TESSELLATE_GPKG_CACHE_LAYER = "tessellation"
    TESSELLATE_GPKG_CACHE_FORCE_INVALIDATE = False
    TESSELLATE_PRIORITY_SPLIT_KEY = ["kadastrs", "kvart", "nog"]
    TESSELLATE_PRIORITY_AREA_DIVISIONS = [20, 20, 20]
    TESSELLATE_PRIORITY_OPTIMIZE_FIELD = "hchm_dsm_fillnodata_mean_div_std"
    TESSELLATE_PRIORITY_NEIGHBOR_CORNERS = True


def print_progress_bar(config: DownloadConfig, current: int, total: int, prefix: str = "", suffix: str = "") -> None:
    misc.print_progress_bar(current, total, prefix, suffix) if config.PRINT_PROGRESS_BAR else config.print(f"{prefix} {100 * current / total}% {suffix}")


def check_call(config: DownloadConfig, cmd: str, unlink_on_exc: str | None = None, throw: bool = True) -> None:
    if config.PRINT_CMD:
        config.print(cmd)
    try:
        subprocess.check_call(shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except subprocess.CalledProcessError as exc:
        if unlink_on_exc:
            misc.silent_unlink(unlink_on_exc)
        if throw:
            raise
        config.print(f"got '{exc.returncode}' from '{cmd}', skipping")


def concat(gdfs: Iterable[gpd.GeoDataFrame]) -> gpd.GeoDataFrame:
    gdfs = tuple(gdfs)
    if gdfs:
        return pd.concat(gdfs, ignore_index=True)  # pyright: ignore [reportReturnType]
    return gpd.GeoDataFrame(geometry=[], crs="EPSG:3059")  # pyright: ignore [reportCallIssue]


def download_data(config: DownloadConfig, data_path: str, name: str, cache_path: str, force_invalidate: bool) -> str:
    index_path = os.path.join(cache_path, "index.json")
    config.print(f"downloading index from '{data_path}'")
    misc.download(data_path, index_path, force_invalidate)
    with open(index_path, encoding="utf-8") as file:
        index = json.load(file)
    for node in index.get("@graph", ()):
        if node.get("@type") == "dcat:Distribution" and node.get("dct:title") == name:
            path = node.get("dcat:accessURL", {}).get("@id")
            config.print(f"downloading '{name}' from '{path}'")
            return misc.download(path, os.path.join(cache_path, name.lower()), force_invalidate)
    raise ValueError(f"'{name}' not found")
