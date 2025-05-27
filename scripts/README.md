# Scripts

This directory contains python project for data download and processing.

## Install

1. Install [Micromamba](https://mamba.readthedocs.io/en/latest/installation/micromamba-installation.html) or [Miniconda](https://docs.anaconda.com/free/miniconda/miniconda-install/).
1. Open bash in this folder, on windows it can be installed from [www.git-scm.com](https://www.git-scm.com/). Also, for convenience an improved terminal emulator like [Tabby](https://tabby.sh/) is suggested.
    * Note that these instructions will also work with cmd or ps but remember that parameter passing/call syntax may be different.
    * Also, don't forget to replace conda with micromamba if you're using micromamba.
1. Execute the following to create python environment from "scripts" directory:
    ```bash
    conda env create -f environment.yml
    ```
1. Thats it, the environment is created.

## Use

1. Remember to bash this folder and activate the environment before use with:
    ```bash
    conda activate mezi
    ```
1. Following that download the data with methods listed below:
    Example with layer geometries from GPKG file:
    ```bash
    python -m mezi.download -g "../pilot/teritorija.gpkg##parcels"
    ```

    Example with BBOX, expected coordinates are EPSG:3059
    ```bash
    python -m mezi.download -b "513875,322782,515253,323533"
    ```

    Example with WKT, , expected coordinates are EPSG:3059
    ```bash
    python -m mezi.download --wkt "Polygon ((514261.23531709046801552 323513.3520204636733979, 514065.89180423523066565 323498.00360159651609138, 513982.17315586871700361 323390.56466952612390742, 513951.47631813428597525 323227.3133052114280872, 513972.40598022594349459 322817.09192821540636942, 515169.58265186724020168 322807.32475257263286039, 515110.97959801071556285 323287.31166987406322733, 515080.28276027628453448 323479.86456111707957461, 514261.23531709046801552 323513.3520204636733979))"
    ```

    Example with BBOX and custom configuration parameter application
    ```bash
    python -m mezi.download -b "513875,322782,515253,323533" -c "..\paraugs_conf.json"
    ```

    Example JSON, where specified custom path for ZMNI data store and directive not to download specified dataset.
    ```json
    {
    "LGIA_ORTO_CIR_DOWNLOAD_TIF": false,
    "ZMNI_GPKG_CACHE_PATH": "../custom_resources/inputdata/zmni/gpkg"
    }
    ```

    * Note that you can view further usage instructions with:
        ```bash
        python -m mezi.download -h
        ```

1. By default the resulting data will be available at [resources/inputdata](resources/inputdata).


## Config

Due to the number of configurable parameters the cli is minimal and accepts a json file with detailed configuration options. Most of the options are systematic and follow a suffixed pattern. What follows is a description of all suffixes used:

* [*_DOWNLOAD](.), [*_DOWNLOAD_TIF](.), [*_DOWNLOAD_TFW](.) - Booleans, these determine if data is downloaded at all, note that for some sources there are options for specific formats only.
* [*_CACHE_PATH](.), [*_GPKG_CACHE_PATH](.) - Folder paths, these determine the cache locations, note that for some sources there are caches for intermediate gpkg files.
    * [*_CACHE_FORCE_INVALIDATE](.) - Booleans, these force the script to ignore specific caches, note that the cache folders can also be simply deleted.
* [*_PATH_HEAD](.) - Folder paths/urls, root paths for file-grid based sources, these follow a specific pattern to construct file paths and download only the required ones.
    * [*_PATH_TAIL_NOMENKLATURA_GPKG_PATH](.) - File paths/urls, these point to grid index gpkg files, these files hold geometries and tail keys for head patterns.
    * [*_PATH_TAIL_NOMENKLATURA_LAYER](.) - Layer names, layer to look for when opening index gpkg files.
    * [*_PATH_TAIL_NOMENKLATURA_FIELD](.) - Field names, field from layer to use for tail keys.
* [*_WFS_PATH](.) - WFS urls, these point to WFS services to use for data.
    * [*_WFS_LAYERS](.) - Layer names, these will be downloaded from the respective WFS.
* [*_DATA_PATH](.) - File paths/urls, these point to [data.gov.lv](https://data.gov.lv) jsonld files.
    * [*_DATA_NAME](.) - Strings, these are the names of resource zips to download.
    * [*_FILE_BUFFERS](.) - Dicts of str and float, these hold shp file names to extract from zips and buffer sizes to apply to features(if 0 then the geometries won't be modified at all).
    * [MVR_TERITORIJAS_*_GPKG_PATH](.) - File paths/urls, MVR is a bit more complicated, there is a three level hierarchy, these work similar to [*_PATH_TAIL_NOMENKLATURA_GPKG_PATH](.), they point to grid index gpkg files, these files hold geometries and keys for following hierarchy level patterns.
        * [MVR_TERITORIJAS_*_LAYER](.) - Layer names, works similar to [*_PATH_TAIL_NOMENKLATURA_LAYER](.).
        * [MVR_TERITORIJAS_*_FIELD](.) - Field names, works similar to [*_PATH_TAIL_NOMENKLATURA_FIELD](.).
* [*_TIF_RASTERIZE](.) - Booleans, similar to [*_DOWNLOAD](.), determine if the data should be rasterized.

There are also some special options for debugging:

* [PRINT_PROGRESS_BAR](.) - Boolean, determines if progress bar should be printed, use this if there are problems with terminal output.
* [PRINT_CMD](.) - Boolean, the script uses some external utilities called from cli, this determines if constructed command should be printed before the call, use this to debug problems with external utilities.

Heres the full list of all available options and their defaults:

```pyhon
PRINT_PROGRESS_BAR = True
PRINT_CMD = False

LGIA_LAS_DOWNLOAD = True
LGIA_LAS_CACHE_PATH = "../resources/inputdata/lgia/las"
LGIA_LAS_CACHE_FORCE_INVALIDATE = False
LGIA_LAS_PATH_HEAD = "https://s3.storage.pub.lvdc.gov.lv/lgia-opendata/las"
LGIA_LAS_PATH_TAIL_NOMENKLATURA_GPKG_PATH = "../basedata/nomenklaturas.gpkg"
LGIA_LAS_PATH_TAIL_NOMENKLATURA_LAYER = "tks1_1000"
LGIA_LAS_PATH_TAIL_NOMENKLATURA_FIELD = "NUMURS"

LGIA_ORTO_RGB_DOWNLOAD_TIF = False
LGIA_ORTO_RGB_DOWNLOAD_TFW = False
LGIA_ORTO_RGB_CACHE_PATH = "../resources/inputdata/lgia/orto/rgb"
LGIA_ORTO_RGB_CACHE_FORCE_INVALIDATE = False
LGIA_ORTO_RGB_PATH_HEAD = "https://s3.storage.pub.lvdc.gov.lv/lgia-opendata/ortofoto_rgb_v6"
LGIA_ORTO_RGB_PATH_TAIL_NOMENKLATURA_GPKG_PATH = "../basedata/nomenklaturas.gpkg"
LGIA_ORTO_RGB_PATH_TAIL_NOMENKLATURA_LAYER = "tks1_5000"
LGIA_ORTO_RGB_PATH_TAIL_NOMENKLATURA_FIELD = "NUMURS"

LGIA_ORTO_CIR_DOWNLOAD_TIF = False
LGIA_ORTO_CIR_DOWNLOAD_TFW = False
LGIA_ORTO_CIR_CACHE_PATH = "../resources/inputdata/lgia/orto/cir"
LGIA_ORTO_CIR_CACHE_FORCE_INVALIDATE = False
LGIA_ORTO_CIR_PATH_HEAD = "https://s3.storage.pub.lvdc.gov.lv/lgia-opendata/ortofoto_cir_v6"
LGIA_ORTO_CIR_PATH_TAIL_NOMENKLATURA_GPKG_PATH = "../basedata/nomenklaturas.gpkg"
LGIA_ORTO_CIR_PATH_TAIL_NOMENKLATURA_LAYER = "tks1_5000"
LGIA_ORTO_CIR_PATH_TAIL_NOMENKLATURA_FIELD = "NUMURS"

SILAVA_DTW_10_DOWNLOAD = False
SILAVA_DTW_10_CACHE_PATH = "../resources/inputdata/silava/dtw/10"
SILAVA_DTW_10_CACHE_FORCE_INVALIDATE = False
SILAVA_DTW_10_PATH_HEAD = "https://silava.forestradar.com/data/rastra-dati/DTW/DTW_10ha_"
SILAVA_DTW_10_PATH_TAIL_NOMENKLATURA_GPKG_PATH = "../basedata/nomenklaturas.gpkg"
SILAVA_DTW_10_PATH_TAIL_NOMENKLATURA_LAYER = "baltic_grid"
SILAVA_DTW_10_PATH_TAIL_NOMENKLATURA_FIELD = "id"

SILAVA_DTW_30_DOWNLOAD = True
SILAVA_DTW_30_CACHE_PATH = "../resources/inputdata/silava/dtw/30"
SILAVA_DTW_30_CACHE_FORCE_INVALIDATE = False
SILAVA_DTW_30_PATH_HEAD = "https://silava.forestradar.com/data/rastra-dati/DTW_30ha/DTW_30ha_"
SILAVA_DTW_30_PATH_TAIL_NOMENKLATURA_GPKG_PATH = "../basedata/nomenklaturas.gpkg"
SILAVA_DTW_30_PATH_TAIL_NOMENKLATURA_LAYER = "baltic_grid"
SILAVA_DTW_30_PATH_TAIL_NOMENKLATURA_FIELD = "id"

ZMNI_GPKG_DOWNLOAD = True
ZMNI_GPKG_CACHE_PATH = "../resources/inputdata/zmni/gpkg"
ZMNI_GPKG_CACHE_FORCE_INVALIDATE = False
ZMNI_GPKG_WFS_PATH = "https://lvmgeoserver.lvm.lv/geoserver/zmni/ows"
ZMNI_GPKG_WFS_LAYERS = ["zmni:zmni_dam", "zmni:zmni_watercourses", "zmni:zmni_ditches", "zmni:zmni_bigdraincollectors", "zmni:zmni_statecontrolledrivers", "zmni:zmni_waterdrainditches", "zmni:zmni_stateriverspolygon"]

MANTOJUMS_GPKG_DOWNLOAD = True
MANTOJUMS_GPKG_CACHE_PATH = "../resources/inputdata/mantojums/gpkg"
MANTOJUMS_GPKG_CACHE_FORCE_INVALIDATE = False
MANTOJUMS_GPKG_WFS_PATH = "https://geoserver.mantojums.lv/geoserver/ows"
MANTOJUMS_GPKG_WFS_LAYERS = ["monument:monuments_culturalobject_unesco", "monument:monuments_protectionzone_public"]

OZOLS_GPKG_DOWNLOAD = True
OZOLS_GPKG_CACHE_PATH = "../resources/inputdata/ozols/gpkg"
OZOLS_GPKG_CACHE_FORCE_INVALIDATE = False
OZOLS_GPKG_WFS_PATH = "https://ozols.gov.lv/arcgis/services/OZOLS_DABASDATI_PUB_INSPIRE/MapServer/WFSServer"
OZOLS_GPKG_WFS_LAYERS = ["OZOLS_DABASDATI_PUB_INSPIRE:IADT_aizsargajamie_koki", "OZOLS_DABASDATI_PUB_INSPIRE:Mikroliegumi", "OZOLS_DABASDATI_PUB_INSPIRE:Mikroliegumu_buferzonas", "OZOLS_DABASDATI_PUB_INSPIRE:IADT_pamatteritorijas", "OZOLS_DABASDATI_PUB_INSPIRE:IADT_zonejums"]

MVR_DOWNLOAD = True
MVR_CACHE_PATH = "../resources/inputdata/mvr/shp"
MVR_CACHE_FORCE_INVALIDATE = False
MVR_DATA_PATH = "https://data.gov.lv/dati/lv/dataset/meza-valsts-registra-meza-dati.jsonld"
MVR_TERITORIJAS_VM_GPKG_PATH = "../basedata/vmd_teritorijas.gpkg"
MVR_TERITORIJAS_VM_LAYER = "vm_cleaned"
MVR_TERITORIJAS_VM_FIELD = "vmd_head_1"
MVR_TERITORIJAS_MZN_GPKG_PATH = "../basedata/vmd_teritorijas.gpkg"
MVR_TERITORIJAS_MZN_LAYER = "mzn_cleaned"
MVR_TERITORIJAS_MZN_FIELD = "vmd_forest"

BIOTOPI_DOWNLOAD = True
BIOTOPI_CACHE_PATH = "../resources/inputdata/dap/biotopi/shp"
BIOTOPI_GPKG_CACHE_PATH = "../resources/inputdata/dap/biotopi/gpkg"
BIOTOPI_CACHE_FORCE_INVALIDATE = False
BIOTOPI_DATA_PATH = "https://data.gov.lv/dati/lv/dataset/aizsargajamas-dzivotnes-biotopi.jsonld"
BIOTOPI_DATA_NAME = "Aizsargājamās dzīvotnes - biotopi"
BIOTOPI_DATA_FILE_BUFFERS = {
    "GIS_OZOLS_Habitat_poly.shp": 0,
}

MIKROLIEGUMI_DOWNLOAD = True
MIKROLIEGUMI_CACHE_PATH = "../resources/inputdata/dap/mikroliegumi/shp"
MIKROLIEGUMI_GPKG_CACHE_PATH = "../resources/inputdata/dap/mikroliegumi/gpkg"
MIKROLIEGUMI_CACHE_FORCE_INVALIDATE = False
MIKROLIEGUMI_DATA_PATH = "https://data.gov.lv/dati/lv/dataset/mikroliegumi.jsonld"
MIKROLIEGUMI_DATA_NAME = "mikroliegumi"
MIKROLIEGUMI_DATA_FILE_BUFFERS = {
    "GIS_OZOLS_Microres_buffer_PUB.shp": 0,
    "GIS_OZOLS_Microreserves_PUB.shp": 0,
}

AIZSARGAJAMAS_SUGAS_DOWNLOAD = True
AIZSARGAJAMAS_SUGAS_CACHE_PATH = "../resources/inputdata/dap/aizsargajamas_sugas/shp"
AIZSARGAJAMAS_SUGAS_GPKG_CACHE_PATH = "../resources/inputdata/dap/aizsargajamas_sugas/gpkg"
AIZSARGAJAMAS_SUGAS_CACHE_FORCE_INVALIDATE = False
AIZSARGAJAMAS_SUGAS_DATA_PATH = "https://data.gov.lv/dati/lv/dataset/aizsargajamo-sugu-atradnes.jsonld"
AIZSARGAJAMAS_SUGAS_DATA_NAME = "Aizsargājamo sugu atradnes"
AIZSARGAJAMAS_SUGAS_DATA_FILE_BUFFERS = {
    "GIS_OZOLS_Species_point.shp": 10,
    "GIS_OZOLS_Species_poly.shp": 0,
}

ZMNI_TIF_RASTERIZE = True
ZMNI_TIF_CACHE_PATH = "../resources/inputdata/zmni/ti"
ZMNI_TIF_CACHE_FORCE_INVALIDATE = False

MANTOJUMS_TIF_RASTERIZE = True
MANTOJUMS_TIF_CACHE_PATH = "../resources/inputdata/mantojums/ti"
MANTOJUMS_TIF_CACHE_FORCE_INVALIDATE = False

OZOLS_TIF_RASTERIZE = True
OZOLS_TIF_CACHE_PATH = "../resources/inputdata/ozols/ti"
OZOLS_TIF_CACHE_FORCE_INVALIDATE = False

MVR_TIF_RASTERIZE = True
MVR_TIF_CACHE_PATH = "../resources/inputdata/mvr/ti"
MVR_TIF_CACHE_FORCE_INVALIDATE = False

BIOTOPI_TIF_RASTERIZE = True
BIOTOPI_TIF_CACHE_PATH = "../resources/inputdata/dap/biotopi/ti"
BIOTOPI_TIF_CACHE_FORCE_INVALIDATE = False

MIKROLIEGUMI_TIF_RASTERIZE = True
MIKROLIEGUMI_TIF_CACHE_PATH = "../resources/inputdata/dap/mikroliegumi/ti"
MIKROLIEGUMI_TIF_CACHE_FORCE_INVALIDATE = False

AIZSARGAJAMAS_SUGAS_TIF_RASTERIZE = True
AIZSARGAJAMAS_SUGAS_TIF_CACHE_PATH = "../resources/inputdata/dap/aizsargajamas_sugas/ti"
AIZSARGAJAMAS_SUGAS_TIF_CACHE_FORCE_INVALIDATE = False
```
