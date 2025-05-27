# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false


# Author: LU, Rīgas Meži, SunGIS
# Created: 2024
# License: EUPL License

# Dependencies: environment.yml
# Python Version: 3.12+

import copy
import decimal
import math
import os
import sqlite3
from collections.abc import MutableSequence
from typing import Any

import pandas as pd
import py7zr

from mezi import config as mezi_config
from mezi.utils import geom

pd.options.mode.copy_on_write = True

_MVR_SCHEMA = {
    "properties": {
        "id": "int",
        "objectid_1": "str",
        "objectid": "str",
        "adm1": "str",
        "adm2": "str",
        "atvk": "str",
        "kadastrs": "str",
        "gtf": "float",
        "kvart": "str",
        "nog": "str",
        "anog": "str",
        "nog_plat": "float",
        "expl_mezs": "float",
        "expl_celi": "float",
        "expl_gravj": "float",
        "zkat": "str",
        "mt": "str",
        "izc": "str",
        "s10": "str",
        "a10": "float",
        "h10": "float",
        "d10": "float",
        "g10": "float",
        "n10": "float",
        "bv10": "str",
        "ba10": "str",
        "s11": "str",
        "a11": "float",
        "h11": "float",
        "d11": "float",
        "g11": "float",
        "n11": "float",
        "bv11": "str",
        "ba11": "str",
        "s12": "str",
        "a12": "float",
        "h12": "float",
        "d12": "float",
        "g12": "float",
        "n12": "float",
        "bv12": "str",
        "ba12": "str",
        "s13": "str",
        "a13": "float",
        "h13": "float",
        "d13": "float",
        "g13": "float",
        "n13": "float",
        "bv13": "str",
        "ba13": "str",
        "s14": "str",
        "a14": "float",
        "h14": "float",
        "d14": "float",
        "g14": "float",
        "n14": "float",
        "bv14": "str",
        "ba14": "str",
        "jakopj": "float",
        "s22": "str",
        "a22": "float",
        "h22": "float",
        "d22": "float",
        "g22": "float",
        "n22": "float",
        "bv22": "str",
        "ba22": "str",
        "s23": "str",
        "a23": "float",
        "h23": "float",
        "d23": "float",
        "g23": "float",
        "n23": "float",
        "bv23": "str",
        "ba23": "str",
        "s24": "str",
        "a24": "float",
        "h24": "float",
        "d24": "float",
        "g24": "float",
        "n24": "float",
        "bv24": "str",
        "ba24": "str",
        "jaatjauno": "float",
        "p_darbv": "str",
        "p_darbg": "float",
        "p_cirp": "str",
        "p_cirg": "float",
        "atj_gads": "float",
        "saimn_d_ie": "float",
        "plant_audz": "str",
        "forestry_c": "str",
        "vmd_headfo": "str",
        "shape_leng": "float",
        "shape_area": "float",
        "k22": "float",
        "biez": "float",
        "vgr": "float",
        "arstnieciba": "float",
        "bruklenes": "float",
        "fitoremediacija": "float",
        "floristika": "float",
        "kosmetika": "float",
        "mellenes": "float",
        "noturiba": "float",
        "pievilciba": "float",
        "rekreacija": "float",
        "troksnis": "float",
        "ugunsbistamiba": "float",
    },
    "geometry": "Polygon",
}


_MVR_ARSTNIECIBA = """
UPDATE apgs SET arstnieciba = CASE
    WHEN apgs.mt IN (7, 8, 12, 17, 18, 22) THEN 1
    WHEN apgs.mt IN (1, 9, 14, 23) THEN 2
    WHEN apgs.mt IN (2, 10, 15, 19, 24) THEN 3
    WHEN apgs.mt IN (3, 11, 16, 21) THEN 4
    WHEN apgs.mt IN (4, 5, 6, 25) THEN 5
    ELSE NULL
END;
"""
_MVR_BRUKLENES = """
UPDATE apgs SET bruklenes = (CASE
    WHEN apgs.mt IN (1, 7, 17) THEN 0.22
    WHEN apgs.mt = 2 THEN 0.18
    WHEN apgs.mt = 8 THEN 0.15
    WHEN apgs.mt IN (3, 18, 22, 23) THEN 0.13
    WHEN apgs.mt IN (14, 12, 9) THEN 0.07
    ELSE 0
END * CASE
    WHEN apgs.s10 IN (1, 22, 14, 28, 13) THEN 1.25
    WHEN apgs.s10 IN (3, 15, 23) THEN 1.0
    WHEN apgs.s10 IN (4, 8) THEN 0.5
    ELSE 0
END) * (-0.00007 * apgs.a10 * apgs.a10 + (0.0132 * apgs.a10) + 0.3957) * (-0.0154 * apgs.biez * apgs.biez + 0.2269 * apgs.biez + 0.1061) * (CASE
    WHEN apgs.mt = 1 THEN 203
    WHEN apgs.mt = 2 THEN 488
    WHEN apgs.mt = 3 THEN 378
    WHEN apgs.mt = 4 THEN 189
    WHEN apgs.mt = 5 THEN 189
    WHEN apgs.mt = 6 THEN 0
    WHEN apgs.mt = 7 THEN 265
    WHEN apgs.mt = 8 THEN 642
    WHEN apgs.mt = 9 THEN 287
    WHEN apgs.mt = 10 THEN 0
    WHEN apgs.mt = 11 THEN 0
    WHEN apgs.mt = 12 THEN 0
    WHEN apgs.mt = 14 THEN 0
    WHEN apgs.mt = 15 THEN 0
    WHEN apgs.mt = 16 THEN 0
    WHEN apgs.mt = 17 THEN 275
    WHEN apgs.mt = 18 THEN 0
    WHEN apgs.mt = 19 THEN 0
    WHEN apgs.mt = 21 THEN 0
    WHEN apgs.mt = 22 THEN 275
    WHEN apgs.mt = 23 THEN 558
    WHEN apgs.mt = 24 THEN 0
    WHEN apgs.mt = 25 THEN 0
    ELSE 0
END) * (1.0 / (1 + exp(-3.5722 + 0.648 * apgs.biez)));
"""
_MVR_FITOREMEDIACIJA = """
UPDATE apgs SET fitoremediacija = CASE
    WHEN apgs.mt IN (11, 16, 25) THEN 1
    WHEN apgs.mt IN (7, 10, 15, 21) THEN 2
    WHEN apgs.mt IN (1, 5, 6, 8, 12, 17, 19, 23) THEN 3
    WHEN apgs.mt IN (2, 3, 4, 9, 14, 22) THEN 4
    WHEN apgs.mt IN (18, 24) THEN 5
    ELSE NULL
END;
"""
_MVR_FLORISTIKA = """
UPDATE apgs SET floristika = CASE
    WHEN apgs.mt IN (7, 8, 12, 14, 17, 18, 22, 23) THEN 1
    WHEN apgs.mt IN (9, 10, 15, 24) THEN 2
    WHEN apgs.mt IN (1, 2, 11, 16, 19, 25) THEN 3
    WHEN apgs.mt = 21 THEN 4
    WHEN apgs.mt IN (3, 4, 5, 6) THEN 5
    ELSE NULL
END;
"""
_MVR_KOSMETIKA = """
UPDATE apgs SET kosmetika = CASE
    WHEN apgs.mt IN (1, 7, 17) THEN 1
    WHEN apgs.mt IN (8, 12, 14, 22) THEN 2
    WHEN apgs.mt IN (2, 3, 4, 5, 10, 11, 15, 16, 23) THEN 3
    WHEN apgs.mt IN (9, 18, 24, 25, 21, 19) THEN 4
    WHEN apgs.mt = 6 THEN 5
    ELSE NULL
END;
"""
_MVR_MELLENES = """
UPDATE apgs SET mellenes = (CASE
    WHEN apgs.mt IN (8, 18, 23) THEN 0.28
    WHEN apgs.mt = 3 THEN 0.24
    WHEN apgs.mt = 2 THEN 0.20
    WHEN apgs.mt IN (17, 9) THEN 0.16
    WHEN apgs.mt = 4 THEN 0.13
    WHEN apgs.mt IN (14, 22) THEN 0.11
    WHEN apgs.mt IN (1, 24, 19) THEN 0.09
    WHEN apgs.mt IN (7, 12) THEN 0.07
    ELSE 0
END * CASE
    WHEN apgs.s10 IN (1, 22, 14, 28, 13) THEN 1.25
    WHEN apgs.s10 IN (3, 15, 23) THEN 1.0
    WHEN apgs.s10 IN (4, 8) THEN 0.5
    ELSE 0
END) * (CASE
    WHEN apgs.a10 > 110 THEN 1.1
    WHEN apgs.a10 > 0 AND apgs.a10 < 111 THEN apgs.a10 / 100.0
    ELSE 0
END) * (-0.0154 * apgs.biez * apgs.biez + 0.2269 * apgs.biez + 0.1061) * (CASE
    WHEN apgs.mt = 1 THEN 205
    WHEN apgs.mt = 2 THEN 485
    WHEN apgs.mt = 3 THEN 408
    WHEN apgs.mt = 4 THEN 408
    WHEN apgs.mt = 5 THEN 408
    WHEN apgs.mt = 6 THEN 0
    WHEN apgs.mt = 7 THEN 377
    WHEN apgs.mt = 8 THEN 1040
    WHEN apgs.mt = 9 THEN 287
    WHEN apgs.mt = 10 THEN 287
    WHEN apgs.mt = 11 THEN 0
    WHEN apgs.mt = 12 THEN 377
    WHEN apgs.mt = 14 THEN 1040
    WHEN apgs.mt = 15 THEN 287
    WHEN apgs.mt = 16 THEN 0
    WHEN apgs.mt = 17 THEN 377
    WHEN apgs.mt = 18 THEN 782
    WHEN apgs.mt = 19 THEN 287
    WHEN apgs.mt = 21 THEN 0
    WHEN apgs.mt = 22 THEN 377
    WHEN apgs.mt = 23 THEN 782
    WHEN apgs.mt = 24 THEN 287
    WHEN apgs.mt = 25 THEN 0
    ELSE 0
END) * (0.04009 * pow(0.22456, apgs.biez) * pow(apgs.biez, 6.6579));
"""
_MVR_NOTURIBA = """
UPDATE apgs SET noturiba = CASE
    WHEN (apgs.mt IN (1, 12)) OR (apgs.zkat IN (21, 22, 23, 31, 32, 33, 34, 41)) THEN 1
    WHEN apgs.mt IN (7, 14, 15, 22, 23) THEN 2
    WHEN apgs.mt IN (8, 9, 16, 17, 18, 24, 25) THEN 3
    WHEN apgs.mt IN (2, 3, 10, 11, 19, 21) THEN 4
    WHEN (apgs.mt IN (4, 5, 6)) OR apgs.zkat = 544 THEN 5
    ELSE NULL
END + CASE
    WHEN (apgs.s10 IN (4, 6, 8, 68, 12, 19, 20, 25, 26, 27, 32, 35, 50, 62, 67)) AND apgs.a10 < 11 OR (apgs.s10 IN (1, 3, 23, 10, 11, 13, 14, 65, 22, 15, 63, 64, 61, 17, 28, 66, 18, 24, 16)) AND apgs.a10 < 21 OR (apgs.s10 IN (9, 21)) AND apgs.a10 < 6 THEN -2
    WHEN (apgs.s10 IN (4, 6, 8, 68, 12, 19, 20, 25, 26, 27, 32, 35, 50, 62, 67)) AND apgs.a10 > 10 AND apgs.a10 < 21 OR (apgs.s10 IN (1, 3, 23, 10, 11, 13, 14, 65, 22, 15, 63, 64, 61, 17, 28, 66, 18, 24, 16)) AND apgs.a10 > 20 AND apgs.a10 < 41 OR (apgs.s10 IN (9, 21)) AND apgs.a10 > 5 AND apgs.a10 < 11 THEN -1
    WHEN (apgs.s10 IN (4, 6, 8, 68, 12, 19, 20, 25, 26, 27, 32, 35, 50, 62, 67)) AND apgs.a10 > 20 AND apgs.a10 < 41 OR (apgs.s10 IN (1, 3, 23, 10, 11, 13, 14, 65, 22, 15, 63, 64, 61, 17, 28, 66, 18, 24, 16)) AND apgs.a10 > 40 AND apgs.a10 < 81 OR (apgs.s10 IN (9, 21)) AND apgs.a10 > 10 AND apgs.a10 < 21 THEN 0
    WHEN (apgs.s10 IN (4, 6, 8, 68, 12, 19, 20, 25, 26, 27, 32, 35, 50, 62, 67)) AND apgs.a10 > 40 AND apgs.a10 < 61 OR (apgs.s10 IN (1, 3, 23, 10, 11, 13, 14, 65, 22, 15, 63, 64, 61, 17, 28, 66, 18, 24, 16)) AND apgs.a10 > 80 AND apgs.a10 < 121 OR (apgs.s10 IN (9, 21)) AND apgs.a10 > 20 AND apgs.a10 < 31 THEN + 1
    WHEN (apgs.s10 IN (4, 6, 8, 68, 12, 19, 20, 25, 26, 27, 32, 35, 50, 62, 67)) AND apgs.a10 > 60 OR (apgs.s10 IN (1, 3, 23, 10, 11, 13, 14, 65, 22, 15, 63, 64, 61, 17, 28, 66, 18, 24, 16)) AND apgs.a10 > 120 OR (apgs.s10 IN (9, 21)) AND apgs.a10 > 20 AND apgs.a10 < 31 THEN 0
    ELSE 0
END + CASE
    WHEN apgs.s10 IN (1, 22, 14, 28, 13) THEN 0
    WHEN apgs.s10 IN (3, 15, 23) THEN -1
    WHEN apgs.s10 IN (4, 6, 8, 68, 12, 19, 20, 25, 26, 27, 32, 35, 50, 62, 67, 10, 11, 65, 63, 64, 61, 17, 66, 18, 24, 16, 9, 21) THEN 1
    ELSE 0
END;
"""  # noqa: E501
_MVR_PIEVILCIBA = """
UPDATE apgs SET pievilciba = 4.80 + CASE
    WHEN apgs.s10 IN (1, 22, 14, 13) THEN 0.15
    WHEN apgs.s10 IN (3, 15, 23, 28) THEN -0.24
    WHEN apgs.s10 IN (4, 8, 12, 63, 66, 24, 16, 10, 61) THEN 0
    ELSE -0.20
END + CASE
    WHEN apgs.h10 >= 0.1 AND apgs.h10 <= 10 THEN 1.16
    WHEN apgs.h10 > 10 AND apgs.h10 <= 20 THEN 1.66
    WHEN apgs.h10 > 20 THEN 1.82
    ELSE 0
END + CASE
    WHEN apgs.s10 <> 3 AND apgs.biez >= 8 AND apgs.k22 = 0 THEN 0.40
    WHEN apgs.k22 > 0 OR (apgs.s10 IN (3, 15, 23, 28)) THEN 0.06
    WHEN apgs.k22 = 0 AND apgs.s10 <> 3 AND apgs.biez < 8 AND apgs.biez >= 5 THEN 0.85
    WHEN apgs.s10 <> 3 AND apgs.biez <= 4 AND apgs.biez >= 2 THEN 0.72
    ELSE 0
END;
"""
_MVR_REKREACIJA = """
UPDATE apgs SET rekreacija = CASE
    WHEN (apgs.mt IN (1, 2, 3, 4, 5, 6)) AND (apgs.s10 IN (1, 22, 14, 28, 13)) AND apgs.a10 > 80 AND apgs.biez > 3 AND apgs.biez < 9 THEN 100
    WHEN (apgs.mt IN (1, 2, 3, 4, 5, 6)) AND (apgs.s10 IN (1, 22, 14, 28, 13)) AND apgs.a10 > 80 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 57
    WHEN (apgs.mt IN (1, 2, 3, 4, 5, 6)) AND (apgs.s10 IN (1, 22, 14, 28, 13)) AND apgs.a10 <= 80 AND apgs.a10 > 40 AND apgs.biez > 3 AND apgs.biez < 9 THEN 60
    WHEN (apgs.mt IN (1, 2, 3, 4, 5, 6)) AND (apgs.s10 IN (1, 22, 14, 28, 13)) AND apgs.a10 <= 80 AND apgs.a10 > 40 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 35
    WHEN (apgs.mt IN (1, 2, 3, 4, 5, 6)) AND (apgs.s10 IN (1, 22, 14, 28, 13)) AND apgs.a10 <= 40 AND apgs.biez > 3 AND apgs.biez < 9 THEN 20
    WHEN (apgs.mt IN (1, 2, 3, 4, 5, 6)) AND (apgs.s10 IN (1, 22, 14, 28, 13)) AND apgs.a10 <= 40 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 11
    WHEN (apgs.mt IN (17, 18, 19, 21, 22, 23, 24, 25)) AND (apgs.s10 IN (1, 22, 14, 28, 13)) AND apgs.a10 > 80 AND apgs.biez > 3 AND apgs.biez < 9 THEN 61
    WHEN (apgs.mt IN (17, 18, 19, 21, 22, 23, 24, 25)) AND (apgs.s10 IN (1, 22, 14, 28, 13)) AND apgs.a10 > 80 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 35
    WHEN (apgs.mt IN (17, 18, 19, 21, 22, 23, 24, 25)) AND (apgs.s10 IN (1, 22, 14, 28, 13)) AND apgs.a10 <= 80 AND apgs.a10 > 40 AND apgs.biez > 3 AND apgs.biez < 9 THEN 37
    WHEN (apgs.mt IN (17, 18, 19, 21, 22, 23, 24, 25)) AND (apgs.s10 IN (1, 22, 14, 28, 13)) AND apgs.a10 <= 80 AND apgs.a10 > 40 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 21
    WHEN (apgs.mt IN (17, 18, 19, 21, 22, 23, 24, 25)) AND (apgs.s10 IN (1, 22, 14, 28, 13)) AND apgs.a10 <= 40 AND apgs.biez > 3 AND apgs.biez < 9 THEN 12
    WHEN (apgs.mt IN (17, 18, 19, 21, 22, 23, 24, 25)) AND (apgs.s10 IN (1, 22, 14, 28, 13)) AND apgs.a10 <= 40 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 7
    WHEN (apgs.mt IN (7, 8, 9, 10, 11, 12, 14, 15, 16)) AND (apgs.s10 IN (1, 22, 14, 28, 13)) AND apgs.a10 > 80 AND apgs.biez > 3 AND apgs.biez < 9 THEN 19
    WHEN (apgs.mt IN (7, 8, 9, 10, 11, 12, 14, 15, 16)) AND (apgs.s10 IN (1, 22, 14, 28, 13)) AND apgs.a10 > 80 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 11
    WHEN (apgs.mt IN (7, 8, 9, 10, 11, 12, 14, 15, 16)) AND (apgs.s10 IN (1, 22, 14, 28, 13)) AND apgs.a10 <= 80 AND apgs.a10 > 40 AND apgs.biez > 3 AND apgs.biez < 9 THEN 11
    WHEN (apgs.mt IN (7, 8, 9, 10, 11, 12, 14, 15, 16)) AND (apgs.s10 IN (1, 22, 14, 28, 13)) AND apgs.a10 <= 80 AND apgs.a10 > 40 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 7
    WHEN (apgs.mt IN (7, 8, 9, 10, 11, 12, 14, 15, 16)) AND (apgs.s10 IN (1, 22, 14, 28, 13)) AND apgs.a10 <= 40 AND apgs.biez > 3 AND apgs.biez < 9 THEN 4
    WHEN (apgs.mt IN (7, 8, 9, 10, 11, 12, 14, 15, 16)) AND (apgs.s10 IN (1, 22, 14, 28, 13)) AND apgs.a10 <= 40 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 2
    WHEN (apgs.mt IN (1, 2, 3, 4, 5, 6)) AND (apgs.s10 IN (10, 61)) AND apgs.a10 > 80 AND apgs.biez > 3 AND apgs.biez < 9 THEN 80
    WHEN (apgs.mt IN (1, 2, 3, 4, 5, 6)) AND (apgs.s10 IN (10, 61)) AND apgs.a10 > 80 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 46
    WHEN (apgs.mt IN (1, 2, 3, 4, 5, 6)) AND (apgs.s10 IN (10, 61)) AND apgs.a10 <= 80 AND apgs.a10 > 40 AND apgs.biez > 3 AND apgs.biez < 9 THEN 48
    WHEN (apgs.mt IN (1, 2, 3, 4, 5, 6)) AND (apgs.s10 IN (10, 61)) AND apgs.a10 <= 80 AND apgs.a10 > 40 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 28
    WHEN (apgs.mt IN (1, 2, 3, 4, 5, 6)) AND (apgs.s10 IN (10, 61)) AND apgs.a10 <= 40 AND apgs.biez > 3 AND apgs.biez < 9 THEN 16
    WHEN (apgs.mt IN (1, 2, 3, 4, 5, 6)) AND (apgs.s10 IN (10, 61)) AND apgs.a10 <= 40 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 9
    WHEN (apgs.mt IN (17, 18, 19, 21, 22, 23, 24, 25)) AND (apgs.s10 IN (10, 61)) AND apgs.a10 > 80 AND apgs.biez > 3 AND apgs.biez < 9 THEN 48
    WHEN (apgs.mt IN (17, 18, 19, 21, 22, 23, 24, 25)) AND (apgs.s10 IN (10, 61)) AND apgs.a10 > 80 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 28
    WHEN (apgs.mt IN (17, 18, 19, 21, 22, 23, 24, 25)) AND (apgs.s10 IN (10, 61)) AND apgs.a10 <= 80 AND apgs.a10 > 40 AND apgs.biez > 3 AND apgs.biez < 9 THEN 29
    WHEN (apgs.mt IN (17, 18, 19, 21, 22, 23, 24, 25)) AND (apgs.s10 IN (10, 61)) AND apgs.a10 <= 80 AND apgs.a10 > 40 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 17
    WHEN (apgs.mt IN (17, 18, 19, 21, 22, 23, 24, 25)) AND (apgs.s10 IN (10, 61)) AND apgs.a10 <= 40 AND apgs.biez > 3 AND apgs.biez < 9 THEN 10
    WHEN (apgs.mt IN (17, 18, 19, 21, 22, 23, 24, 25)) AND (apgs.s10 IN (10, 61)) AND apgs.a10 <= 40 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 5
    WHEN (apgs.mt IN (7, 8, 9, 10, 11, 12, 14, 15, 16)) AND (apgs.s10 IN (10, 61)) AND apgs.a10 > 80 AND apgs.biez > 3 AND apgs.biez < 9 THEN 16
    WHEN (apgs.mt IN (7, 8, 9, 10, 11, 12, 14, 15, 16)) AND (apgs.s10 IN (10, 61)) AND apgs.a10 > 80 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 9
    WHEN (apgs.mt IN (7, 8, 9, 10, 11, 12, 14, 15, 16)) AND (apgs.s10 IN (10, 61)) AND apgs.a10 <= 80 AND apgs.a10 > 40 AND apgs.biez > 3 AND apgs.biez < 9 THEN 10
    WHEN (apgs.mt IN (7, 8, 9, 10, 11, 12, 14, 15, 16)) AND (apgs.s10 IN (10, 61)) AND apgs.a10 <= 80 AND apgs.a10 > 40 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 6
    WHEN (apgs.mt IN (7, 8, 9, 10, 11, 12, 14, 15, 16)) AND (apgs.s10 IN (10, 61)) AND apgs.a10 <= 40 AND apgs.biez > 3 AND apgs.biez < 9 THEN 3
    WHEN (apgs.mt IN (7, 8, 9, 10, 11, 12, 14, 15, 16)) AND (apgs.s10 IN (10, 61)) AND apgs.a10 <= 40 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 2
    WHEN (apgs.mt IN (1, 2, 3, 4, 5, 6)) AND apgs.s10 = 4 AND apgs.a10 > 40 AND apgs.biez > 3 AND apgs.biez < 9 THEN 70
    WHEN (apgs.mt IN (1, 2, 3, 4, 5, 6)) AND apgs.s10 = 4 AND apgs.a10 > 40 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 40
    WHEN (apgs.mt IN (1, 2, 3, 4, 5, 6)) AND apgs.s10 = 4 AND apgs.a10 <= 40 AND apgs.a10 > 20 AND apgs.biez > 3 AND apgs.biez < 9 THEN 42
    WHEN (apgs.mt IN (1, 2, 3, 4, 5, 6)) AND apgs.s10 = 4 AND apgs.a10 <= 40 AND apgs.a10 > 20 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 24
    WHEN (apgs.mt IN (1, 2, 3, 4, 5, 6)) AND apgs.s10 = 4 AND apgs.a10 <= 20 AND apgs.biez > 3 AND apgs.biez < 9 THEN 14
    WHEN (apgs.mt IN (1, 2, 3, 4, 5, 6)) AND apgs.s10 = 4 AND apgs.a10 <= 20 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 8
    WHEN (apgs.mt IN (17, 18, 19, 21, 22, 23, 24, 25)) AND apgs.s10 = 4 AND apgs.a10 > 40 AND apgs.biez > 3 AND apgs.biez < 9 THEN 42
    WHEN (apgs.mt IN (17, 18, 19, 21, 22, 23, 24, 25)) AND apgs.s10 = 4 AND apgs.a10 > 40 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 24
    WHEN (apgs.mt IN (17, 18, 19, 21, 22, 23, 24, 25)) AND apgs.s10 = 4 AND apgs.a10 <= 40 AND apgs.a10 > 20 AND apgs.biez > 3 AND apgs.biez < 9 THEN 25
    WHEN (apgs.mt IN (17, 18, 19, 21, 22, 23, 24, 25)) AND apgs.s10 = 4 AND apgs.a10 <= 40 AND apgs.a10 > 20 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 14
    WHEN (apgs.mt IN (17, 18, 19, 21, 22, 23, 24, 25)) AND apgs.s10 = 4 AND apgs.a10 <= 20 AND apgs.biez > 3 AND apgs.biez < 9 THEN 8
    WHEN (apgs.mt IN (17, 18, 19, 21, 22, 23, 24, 25)) AND apgs.s10 = 4 AND apgs.a10 <= 20 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 5
    WHEN (apgs.mt IN (7, 8, 9, 10, 11, 12, 14, 15, 16)) AND apgs.s10 = 4 AND apgs.a10 > 40 AND apgs.biez > 3 AND apgs.biez < 9 THEN 14
    WHEN (apgs.mt IN (7, 8, 9, 10, 11, 12, 14, 15, 16)) AND apgs.s10 = 4 AND apgs.a10 > 40 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 8
    WHEN (apgs.mt IN (7, 8, 9, 10, 11, 12, 14, 15, 16)) AND apgs.s10 = 4 AND apgs.a10 <= 40 AND apgs.a10 > 20 AND apgs.biez > 3 AND apgs.biez < 9 THEN 8
    WHEN (apgs.mt IN (7, 8, 9, 10, 11, 12, 14, 15, 16)) AND apgs.s10 = 4 AND apgs.a10 <= 40 AND apgs.a10 > 20 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 5
    WHEN (apgs.mt IN (7, 8, 9, 10, 11, 12, 14, 15, 16)) AND apgs.s10 = 4 AND apgs.a10 <= 20 AND apgs.biez > 3 AND apgs.biez < 9 THEN 3
    WHEN (apgs.mt IN (7, 8, 9, 10, 11, 12, 14, 15, 16)) AND apgs.s10 = 4 AND apgs.a10 <= 20 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 1
    WHEN (apgs.mt IN (1, 2, 3, 4, 5, 6)) AND (apgs.s10 IN (3, 15, 23)) AND apgs.a10 > 60 AND apgs.biez > 3 AND apgs.biez < 9 THEN 50
    WHEN (apgs.mt IN (1, 2, 3, 4, 5, 6)) AND (apgs.s10 IN (3, 15, 23)) AND apgs.a10 > 60 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 28
    WHEN (apgs.mt IN (1, 2, 3, 4, 5, 6)) AND (apgs.s10 IN (3, 15, 23)) AND apgs.a10 <= 60 AND apgs.a10 > 40 AND apgs.biez > 3 AND apgs.biez < 9 THEN 30
    WHEN (apgs.mt IN (1, 2, 3, 4, 5, 6)) AND (apgs.s10 IN (3, 15, 23)) AND apgs.a10 <= 60 AND apgs.a10 > 40 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 17
    WHEN (apgs.mt IN (1, 2, 3, 4, 5, 6)) AND (apgs.s10 IN (3, 15, 23)) AND apgs.a10 <= 40 AND apgs.biez > 3 AND apgs.biez < 9 THEN 10
    WHEN (apgs.mt IN (1, 2, 3, 4, 5, 6)) AND (apgs.s10 IN (3, 15, 23)) AND apgs.a10 <= 40 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 5
    WHEN (apgs.mt IN (17, 18, 19, 21, 22, 23, 24, 25)) AND (apgs.s10 IN (3, 15, 23)) AND apgs.a10 > 60 AND apgs.biez > 3 AND apgs.biez < 9 THEN 30
    WHEN (apgs.mt IN (17, 18, 19, 21, 22, 23, 24, 25)) AND (apgs.s10 IN (3, 15, 23)) AND apgs.a10 > 60 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 17
    WHEN (apgs.mt IN (17, 18, 19, 21, 22, 23, 24, 25)) AND (apgs.s10 IN (3, 15, 23)) AND apgs.a10 <= 60 AND apgs.a10 > 40 AND apgs.biez > 3 AND apgs.biez < 9 THEN 18
    WHEN (apgs.mt IN (17, 18, 19, 21, 22, 23, 24, 25)) AND (apgs.s10 IN (3, 15, 23)) AND apgs.a10 <= 60 AND apgs.a10 > 40 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 10
    WHEN (apgs.mt IN (17, 18, 19, 21, 22, 23, 24, 25)) AND (apgs.s10 IN (3, 15, 23)) AND apgs.a10 <= 40 AND apgs.biez > 3 AND apgs.biez < 9 THEN 6
    WHEN (apgs.mt IN (17, 18, 19, 21, 22, 23, 24, 25)) AND (apgs.s10 IN (3, 15, 23)) AND apgs.a10 <= 40 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 3
    WHEN (apgs.mt IN (7, 8, 9, 10, 11, 12, 14, 15, 16)) AND (apgs.s10 IN (3, 15, 23)) AND apgs.a10 > 60 AND apgs.biez > 3 AND apgs.biez < 9 THEN 10
    WHEN (apgs.mt IN (7, 8, 9, 10, 11, 12, 14, 15, 16)) AND (apgs.s10 IN (3, 15, 23)) AND apgs.a10 > 60 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 5
    WHEN (apgs.mt IN (7, 8, 9, 10, 11, 12, 14, 15, 16)) AND (apgs.s10 IN (3, 15, 23)) AND apgs.a10 <= 60 AND apgs.a10 > 40 AND apgs.biez > 3 AND apgs.biez < 9 THEN 5
    WHEN (apgs.mt IN (7, 8, 9, 10, 11, 12, 14, 15, 16)) AND (apgs.s10 IN (3, 15, 23)) AND apgs.a10 <= 60 AND apgs.a10 > 40 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 3
    WHEN (apgs.mt IN (7, 8, 9, 10, 11, 12, 14, 15, 16)) AND (apgs.s10 IN (3, 15, 23)) AND apgs.a10 <= 40 AND apgs.biez > 3 AND apgs.biez < 9 THEN 2
    WHEN (apgs.mt IN (7, 8, 9, 10, 11, 12, 14, 15, 16)) AND (apgs.s10 IN (3, 15, 23)) AND apgs.a10 <= 40 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 1
    WHEN (apgs.mt IN (1, 2, 3, 4, 5, 6)) AND (apgs.s10 IN (11, 64, 12, 25, 26, 27, 35, 50, 62, 67, 65, 63, 17, 66, 18, 24, 16)) AND apgs.a10 > 60 AND apgs.biez > 3 AND apgs.biez < 9 THEN 50
    WHEN (apgs.mt IN (1, 2, 3, 4, 5, 6)) AND (apgs.s10 IN (11, 64, 12, 25, 26, 27, 35, 50, 62, 67, 65, 63, 17, 66, 18, 24, 16)) AND apgs.a10 > 60 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 28
    WHEN (apgs.mt IN (1, 2, 3, 4, 5, 6)) AND (apgs.s10 IN (11, 64, 12, 25, 26, 27, 35, 50, 62, 67, 65, 63, 17, 66, 18, 24, 16)) AND apgs.a10 <= 60 AND apgs.a10 > 40 AND apgs.biez > 3 AND apgs.biez < 9 THEN 30
    WHEN (apgs.mt IN (1, 2, 3, 4, 5, 6)) AND (apgs.s10 IN (11, 64, 12, 25, 26, 27, 35, 50, 62, 67, 65, 63, 17, 66, 18, 24, 16)) AND apgs.a10 <= 60 AND apgs.a10 > 40 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 17
    WHEN (apgs.mt IN (1, 2, 3, 4, 5, 6)) AND (apgs.s10 IN (11, 64, 12, 25, 26, 27, 35, 50, 62, 67, 65, 63, 17, 66, 18, 24, 16)) AND apgs.a10 <= 40 AND apgs.biez > 3 AND apgs.biez < 9 THEN 10
    WHEN (apgs.mt IN (1, 2, 3, 4, 5, 6)) AND (apgs.s10 IN (11, 64, 12, 25, 26, 27, 35, 50, 62, 67, 65, 63, 17, 66, 18, 24, 16)) AND apgs.a10 <= 40 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 5
    WHEN (apgs.mt IN (17, 18, 19, 21, 22, 23, 24, 25)) AND (apgs.s10 IN (11, 64, 12, 25, 26, 27, 35, 50, 62, 67, 65, 63, 17, 66, 18, 24, 16)) AND apgs.a10 > 60 AND apgs.biez > 3 AND apgs.biez < 9 THEN 30
    WHEN (apgs.mt IN (17, 18, 19, 21, 22, 23, 24, 25)) AND (apgs.s10 IN (11, 64, 12, 25, 26, 27, 35, 50, 62, 67, 65, 63, 17, 66, 18, 24, 16)) AND apgs.a10 > 60 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 17
    WHEN (apgs.mt IN (17, 18, 19, 21, 22, 23, 24, 25)) AND (apgs.s10 IN (11, 64, 12, 25, 26, 27, 35, 50, 62, 67, 65, 63, 17, 66, 18, 24, 16)) AND apgs.a10 <= 60 AND apgs.a10 > 40 AND apgs.biez > 3 AND apgs.biez < 9 THEN 18
    WHEN (apgs.mt IN (17, 18, 19, 21, 22, 23, 24, 25)) AND (apgs.s10 IN (11, 64, 12, 25, 26, 27, 35, 50, 62, 67, 65, 63, 17, 66, 18, 24, 16)) AND apgs.a10 <= 60 AND apgs.a10 > 40 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 10
    WHEN (apgs.mt IN (17, 18, 19, 21, 22, 23, 24, 25)) AND (apgs.s10 IN (11, 64, 12, 25, 26, 27, 35, 50, 62, 67, 65, 63, 17, 66, 18, 24, 16)) AND apgs.a10 <= 40 AND apgs.biez > 3 AND apgs.biez < 9 THEN 6
    WHEN (apgs.mt IN (17, 18, 19, 21, 22, 23, 24, 25)) AND (apgs.s10 IN (11, 64, 12, 25, 26, 27, 35, 50, 62, 67, 65, 63, 17, 66, 18, 24, 16)) AND apgs.a10 <= 40 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 3
    WHEN (apgs.mt IN (7, 8, 9, 10, 11, 12, 14, 15, 16)) AND (apgs.s10 IN (11, 64, 12, 25, 26, 27, 35, 50, 62, 67, 65, 63, 17, 66, 18, 24, 16)) AND apgs.a10 > 60 AND apgs.biez > 3 AND apgs.biez < 9 THEN 10
    WHEN (apgs.mt IN (7, 8, 9, 10, 11, 12, 14, 15, 16)) AND (apgs.s10 IN (11, 64, 12, 25, 26, 27, 35, 50, 62, 67, 65, 63, 17, 66, 18, 24, 16)) AND apgs.a10 > 60 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 5
    WHEN (apgs.mt IN (7, 8, 9, 10, 11, 12, 14, 15, 16)) AND (apgs.s10 IN (11, 64, 12, 25, 26, 27, 35, 50, 62, 67, 65, 63, 17, 66, 18, 24, 16)) AND apgs.a10 <= 60 AND apgs.a10 > 40 AND apgs.biez > 3 AND apgs.biez < 9 THEN 5
    WHEN (apgs.mt IN (7, 8, 9, 10, 11, 12, 14, 15, 16)) AND (apgs.s10 IN (11, 64, 12, 25, 26, 27, 35, 50, 62, 67, 65, 63, 17, 66, 18, 24, 16)) AND apgs.a10 <= 60 AND apgs.a10 > 40 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 3
    WHEN (apgs.mt IN (7, 8, 9, 10, 11, 12, 14, 15, 16)) AND (apgs.s10 IN (11, 64, 12, 25, 26, 27, 35, 50, 62, 67, 65, 63, 17, 66, 18, 24, 16)) AND apgs.a10 <= 40 AND apgs.biez > 3 AND apgs.biez < 9 THEN 2
    WHEN (apgs.mt IN (7, 8, 9, 10, 11, 12, 14, 15, 16)) AND (apgs.s10 IN (11, 64, 12, 25, 26, 27, 35, 50, 62, 67, 65, 63, 17, 66, 18, 24, 16)) AND apgs.a10 <= 40 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 1
    WHEN (apgs.mt IN (1, 2, 3, 4, 5, 6)) AND (apgs.s10 IN (8, 68, 19, 20)) AND apgs.a10 > 40 AND apgs.biez > 3 AND apgs.biez < 9 THEN 40
    WHEN (apgs.mt IN (1, 2, 3, 4, 5, 6)) AND (apgs.s10 IN (8, 68, 19, 20)) AND apgs.a10 > 40 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 23
    WHEN (apgs.mt IN (1, 2, 3, 4, 5, 6)) AND (apgs.s10 IN (8, 68, 19, 20)) AND apgs.a10 <= 40 AND apgs.a10 > 20 AND apgs.biez > 3 AND apgs.biez < 9 THEN 24
    WHEN (apgs.mt IN (1, 2, 3, 4, 5, 6)) AND (apgs.s10 IN (8, 68, 19, 20)) AND apgs.a10 <= 40 AND apgs.a10 > 20 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 14
    WHEN (apgs.mt IN (1, 2, 3, 4, 5, 6)) AND (apgs.s10 IN (8, 68, 19, 20)) AND apgs.a10 <= 20 AND apgs.biez > 3 AND apgs.biez < 9 THEN 8
    WHEN (apgs.mt IN (1, 2, 3, 4, 5, 6)) AND (apgs.s10 IN (8, 68, 19, 20)) AND apgs.a10 <= 20 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 4
    WHEN (apgs.mt IN (17, 18, 19, 21, 22, 23, 24, 25)) AND (apgs.s10 IN (8, 68, 19, 20)) AND apgs.a10 > 40 AND apgs.biez > 3 AND apgs.biez < 9 THEN 24
    WHEN (apgs.mt IN (17, 18, 19, 21, 22, 23, 24, 25)) AND (apgs.s10 IN (8, 68, 19, 20)) AND apgs.a10 > 40 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 14
    WHEN (apgs.mt IN (17, 18, 19, 21, 22, 23, 24, 25)) AND (apgs.s10 IN (8, 68, 19, 20)) AND apgs.a10 <= 40 AND apgs.a10 > 20 AND apgs.biez > 3 AND apgs.biez < 9 THEN 15
    WHEN (apgs.mt IN (17, 18, 19, 21, 22, 23, 24, 25)) AND (apgs.s10 IN (8, 68, 19, 20)) AND apgs.a10 <= 40 AND apgs.a10 > 20 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 8
    WHEN (apgs.mt IN (17, 18, 19, 21, 22, 23, 24, 25)) AND (apgs.s10 IN (8, 68, 19, 20)) AND apgs.a10 <= 20 AND apgs.biez > 3 AND apgs.biez < 9 THEN 5
    WHEN (apgs.mt IN (17, 18, 19, 21, 22, 23, 24, 25)) AND (apgs.s10 IN (8, 68, 19, 20)) AND apgs.a10 <= 20 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 3
    WHEN (apgs.mt IN (7, 8, 9, 10, 11, 12, 14, 15, 16)) AND (apgs.s10 IN (8, 68, 19, 20)) AND apgs.a10 > 40 AND apgs.biez > 3 AND apgs.biez < 9 THEN 8
    WHEN (apgs.mt IN (7, 8, 9, 10, 11, 12, 14, 15, 16)) AND (apgs.s10 IN (8, 68, 19, 20)) AND apgs.a10 > 40 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 4
    WHEN (apgs.mt IN (7, 8, 9, 10, 11, 12, 14, 15, 16)) AND (apgs.s10 IN (8, 68, 19, 20)) AND apgs.a10 <= 40 AND apgs.a10 > 20 AND apgs.biez > 3 AND apgs.biez < 9 THEN 4
    WHEN (apgs.mt IN (7, 8, 9, 10, 11, 12, 14, 15, 16)) AND (apgs.s10 IN (8, 68, 19, 20)) AND apgs.a10 <= 40 AND apgs.a10 > 20 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 3
    WHEN (apgs.mt IN (7, 8, 9, 10, 11, 12, 14, 15, 16)) AND (apgs.s10 IN (8, 68, 19, 20)) AND apgs.a10 <= 20 AND apgs.biez > 3 AND apgs.biez < 9 THEN 2
    WHEN (apgs.mt IN (7, 8, 9, 10, 11, 12, 14, 15, 16)) AND (apgs.s10 IN (8, 68, 19, 20)) AND apgs.a10 <= 20 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 1
    WHEN (apgs.mt IN (1, 2, 3, 4, 5, 6)) AND apgs.s10 = 6 AND apgs.a10 > 40 AND apgs.biez > 3 AND apgs.biez < 9 THEN 30
    WHEN (apgs.mt IN (1, 2, 3, 4, 5, 6)) AND apgs.s10 = 6 AND apgs.a10 > 40 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 17
    WHEN (apgs.mt IN (1, 2, 3, 4, 5, 6)) AND apgs.s10 = 6 AND apgs.a10 <= 40 AND apgs.a10 > 20 AND apgs.biez > 3 AND apgs.biez < 9 THEN 18
    WHEN (apgs.mt IN (1, 2, 3, 4, 5, 6)) AND apgs.s10 = 6 AND apgs.a10 <= 40 AND apgs.a10 > 20 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 10
    WHEN (apgs.mt IN (1, 2, 3, 4, 5, 6)) AND apgs.s10 = 6 AND apgs.a10 <= 20 AND apgs.biez > 3 AND apgs.biez < 9 THEN 6
    WHEN (apgs.mt IN (1, 2, 3, 4, 5, 6)) AND apgs.s10 = 6 AND apgs.a10 <= 20 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 3
    WHEN (apgs.mt IN (17, 18, 19, 21, 22, 23, 24, 25)) AND apgs.s10 = 6 AND apgs.a10 > 40 AND apgs.biez > 3 AND apgs.biez < 9 THEN 21
    WHEN (apgs.mt IN (17, 18, 19, 21, 22, 23, 24, 25)) AND apgs.s10 = 6 AND apgs.a10 > 40 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 12
    WHEN (apgs.mt IN (17, 18, 19, 21, 22, 23, 24, 25)) AND apgs.s10 = 6 AND apgs.a10 <= 40 AND apgs.a10 > 20 AND apgs.biez > 3 AND apgs.biez < 9 THEN 13
    WHEN (apgs.mt IN (17, 18, 19, 21, 22, 23, 24, 25)) AND apgs.s10 = 6 AND apgs.a10 <= 40 AND apgs.a10 > 20 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 7
    WHEN (apgs.mt IN (17, 18, 19, 21, 22, 23, 24, 25)) AND apgs.s10 = 6 AND apgs.a10 <= 20 AND apgs.biez > 3 AND apgs.biez < 9 THEN 4
    WHEN (apgs.mt IN (17, 18, 19, 21, 22, 23, 24, 25)) AND apgs.s10 = 6 AND apgs.a10 <= 20 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 2
    WHEN (apgs.mt IN (7, 8, 9, 10, 11, 12, 14, 15, 16)) AND apgs.s10 = 6 AND apgs.a10 > 40 AND apgs.biez > 3 AND apgs.biez < 9 THEN 6
    WHEN (apgs.mt IN (7, 8, 9, 10, 11, 12, 14, 15, 16)) AND apgs.s10 = 6 AND apgs.a10 > 40 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 3
    WHEN (apgs.mt IN (7, 8, 9, 10, 11, 12, 14, 15, 16)) AND apgs.s10 = 6 AND apgs.a10 <= 40 AND apgs.a10 > 20 AND apgs.biez > 3 AND apgs.biez < 9 THEN 4
    WHEN (apgs.mt IN (7, 8, 9, 10, 11, 12, 14, 15, 16)) AND apgs.s10 = 6 AND apgs.a10 <= 40 AND apgs.a10 > 20 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 2
    WHEN (apgs.mt IN (7, 8, 9, 10, 11, 12, 14, 15, 16)) AND apgs.s10 = 6 AND apgs.a10 <= 20 AND apgs.biez > 3 AND apgs.biez < 9 THEN 1
    WHEN (apgs.mt IN (7, 8, 9, 10, 11, 12, 14, 15, 16)) AND apgs.s10 = 6 AND apgs.a10 <= 20 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 1
    WHEN (apgs.mt IN (1, 2, 3, 4, 5, 6)) AND (apgs.s10 IN (9, 21, 32)) AND apgs.a10 > 20 AND apgs.biez > 3 AND apgs.biez < 9 THEN 20
    WHEN (apgs.mt IN (1, 2, 3, 4, 5, 6)) AND (apgs.s10 IN (9, 21, 32)) AND apgs.a10 > 20 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 11
    WHEN (apgs.mt IN (1, 2, 3, 4, 5, 6)) AND (apgs.s10 IN (9, 21, 32)) AND apgs.a10 <= 20 AND apgs.a10 > 10 AND apgs.biez > 3 AND apgs.biez < 9 THEN 12
    WHEN (apgs.mt IN (1, 2, 3, 4, 5, 6)) AND (apgs.s10 IN (9, 21, 32)) AND apgs.a10 <= 20 AND apgs.a10 > 10 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 7
    WHEN (apgs.mt IN (1, 2, 3, 4, 5, 6)) AND (apgs.s10 IN (9, 21, 32)) AND apgs.a10 <= 10 AND apgs.biez > 3 AND apgs.biez < 9 THEN 4
    WHEN (apgs.mt IN (1, 2, 3, 4, 5, 6)) AND (apgs.s10 IN (9, 21, 32)) AND apgs.a10 <= 10 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 2
    WHEN (apgs.mt IN (17, 18, 19, 21, 22, 23, 24, 25)) AND (apgs.s10 IN (9, 21, 32)) AND apgs.a10 > 20 AND apgs.biez > 3 AND apgs.biez < 9 THEN 14
    WHEN (apgs.mt IN (17, 18, 19, 21, 22, 23, 24, 25)) AND (apgs.s10 IN (9, 21, 32)) AND apgs.a10 > 20 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 8
    WHEN (apgs.mt IN (17, 18, 19, 21, 22, 23, 24, 25)) AND (apgs.s10 IN (9, 21, 32)) AND apgs.a10 <= 20 AND apgs.a10 > 10 AND apgs.biez > 3 AND apgs.biez < 9 THEN 8
    WHEN (apgs.mt IN (17, 18, 19, 21, 22, 23, 24, 25)) AND (apgs.s10 IN (9, 21, 32)) AND apgs.a10 <= 20 AND apgs.a10 > 10 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 5
    WHEN (apgs.mt IN (17, 18, 19, 21, 22, 23, 24, 25)) AND (apgs.s10 IN (9, 21, 32)) AND apgs.a10 <= 10 AND apgs.biez > 3 AND apgs.biez < 9 THEN 3
    WHEN (apgs.mt IN (17, 18, 19, 21, 22, 23, 24, 25)) AND (apgs.s10 IN (9, 21, 32)) AND apgs.a10 <= 10 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 1
    WHEN (apgs.mt IN (7, 8, 9, 10, 11, 12, 14, 15, 16)) AND (apgs.s10 IN (9, 21, 32)) AND apgs.a10 > 20 AND apgs.biez > 3 AND apgs.biez < 9 THEN 4
    WHEN (apgs.mt IN (7, 8, 9, 10, 11, 12, 14, 15, 16)) AND (apgs.s10 IN (9, 21, 32)) AND apgs.a10 > 20 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 2
    WHEN (apgs.mt IN (7, 8, 9, 10, 11, 12, 14, 15, 16)) AND (apgs.s10 IN (9, 21, 32)) AND apgs.a10 <= 20 AND apgs.a10 > 10 AND apgs.biez > 3 AND apgs.biez < 9 THEN 2
    WHEN (apgs.mt IN (7, 8, 9, 10, 11, 12, 14, 15, 16)) AND (apgs.s10 IN (9, 21, 32)) AND apgs.a10 <= 20 AND apgs.a10 > 10 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 1
    WHEN (apgs.mt IN (7, 8, 9, 10, 11, 12, 14, 15, 16)) AND (apgs.s10 IN (9, 21, 32)) AND apgs.a10 <= 10 AND apgs.biez > 3 AND apgs.biez < 9 THEN 1
    WHEN (apgs.mt IN (7, 8, 9, 10, 11, 12, 14, 15, 16)) AND (apgs.s10 IN (9, 21, 32)) AND apgs.a10 <= 10 AND (apgs.biez <= 3 OR apgs.biez >= 9) THEN 0
    WHEN apgs.zkat = 14 THEN 0
    ELSE NULL
END;
"""
_MVR_TROKSNIS = """
UPDATE apgs SET troksnis = CASE
    WHEN (apgs.s10 IN (1, 22, 14, 28, 13, 3, 15, 23)) AND apgs.biez < 2 THEN 1
    WHEN (apgs.s10 IN (1, 22, 14, 28, 13, 3, 15, 23)) AND apgs.biez >= 2 AND apgs.biez < 4 THEN 2
    WHEN (apgs.s10 IN (1, 22, 14, 28, 13, 3, 15, 23)) AND apgs.biez >= 4 AND apgs.biez < 6 THEN 3
    WHEN (apgs.s10 IN (1, 22, 14, 28, 13, 3, 15, 23)) AND apgs.biez >= 6 AND apgs.biez < 8 THEN 4
    WHEN (apgs.s10 IN (1, 22, 14, 28, 13, 3, 15, 23)) AND apgs.biez >= 8 THEN 5
    WHEN (apgs.s10 IN (4, 6, 8, 68, 12, 19, 20, 25, 26, 27, 32, 35, 50, 62, 67, 10, 11, 65, 63, 64, 61, 17, 66, 18, 24, 16, 9, 21)) AND apgs.biez < 3 THEN 1
    WHEN (apgs.s10 IN (4, 6, 8, 68, 12, 19, 20, 25, 26, 27, 32, 35, 50, 62, 67, 10, 11, 65, 63, 64, 61, 17, 66, 18, 24, 16, 9, 21)) AND apgs.biez >= 3 AND apgs.biez < 5 THEN 2
    WHEN (apgs.s10 IN (4, 6, 8, 68, 12, 19, 20, 25, 26, 27, 32, 35, 50, 62, 67, 10, 11, 65, 63, 64, 61, 17, 66, 18, 24, 16, 9, 21)) AND apgs.biez >= 5 AND apgs.biez < 7 THEN 3
    WHEN (apgs.s10 IN (4, 6, 8, 68, 12, 19, 20, 25, 26, 27, 32, 35, 50, 62, 67, 10, 11, 65, 63, 64, 61, 17, 66, 18, 24, 16, 9, 21)) AND apgs.biez >= 7 AND apgs.biez < 9 THEN 4
    WHEN (apgs.s10 IN (4, 6, 8, 68, 12, 19, 20, 25, 26, 27, 32, 35, 50, 62, 67, 10, 11, 65, 63, 64, 61, 17, 66, 18, 24, 16, 9, 21)) AND apgs.biez >= 9 THEN 5
    ELSE 0
END;
"""
_MVR_UGUNSBISTAMIBA = """
UPDATE apgs SET ugunsbistamiba = CASE
    WHEN (apgs.mt IN (1, 2, 3, 4, 7, 17, 18, 19, 22, 23, 24)) AND (apgs.s10 IN (1, 3, 13, 14, 15, 22, 23, 28, 29)) OR (apgs.mt IN (5)) AND (apgs.s10 IN (1, 3, 13, 14, 15, 22, 23, 28, 29)) AND apgs.a10 < 40 THEN 1
    WHEN (apgs.mt IN (5)) AND (apgs.s10 IN (1, 3, 13, 14, 15, 22, 23, 28, 29)) AND apgs.a10 > 39 OR (apgs.mt IN (4, 5, 6, 19, 22, 23, 24)) AND (apgs.s10 IN (4, 6, 8, 9, 10, 11, 12, 16, 17, 18, 19, 20, 21, 24, 25, 26, 27, 32, 25, 50, 61, 62, 63, 64, 65, 66, 67, 68)) OR (apgs.mt IN (8, 9, 10, 11, 21, 25)) THEN 2
    WHEN apgs.mt IN (12, 14, 15, 16) THEN 3
    ELSE 0
END;
"""  # noqa: E501


_GNORM_TABLE_VS = (
    (decimal.Decimal("15.85"), decimal.Decimal("15.67"), -5),
    (decimal.Decimal("-114.91"), decimal.Decimal("92.49"), 20),
    (decimal.Decimal("-143.92"), decimal.Decimal("99.88"), 30),
    (decimal.Decimal("-173.99"), decimal.Decimal("120.46"), 30),
    (-149, decimal.Decimal("104.49"), 30),
    (decimal.Decimal("-0.09"), decimal.Decimal("21.08"), -6),
)
_GNORM_TABLE_KS = (
    (1,),
    (3, 13, 14, 15, 22, 23),
    (4, 12),
    (6, 8, 9, 19, 20, 21),
    (10, 16, 17, 18, 24),
    (11,),
)
_GNORM_TABLE = {k: v for ks, v in zip(_GNORM_TABLE_KS, _GNORM_TABLE_VS) for k in ks}


def _get_gnorm(h: int, s: int) -> decimal.Decimal:
    if h < (7 if s == 1 else 6) or h > 35:  # noqa: PLR2004
        return decimal.Decimal(0)
    a, b, c = _GNORM_TABLE.get(s, (0, 0, 0))
    return a + b * decimal.Decimal(math.log10(h + c))


_NNORM_TABLE_HS = {
    6: (4000, 3200, 3200, 3200, 2000),
    7: (3800, 3200, 3200, 3000, 2000),
    8: (3600, 3000, 3000, 2800, 1800),
    9: (3400, 3000, 3000, 2600, 1800),
    10: (3000, 3000, 3000, 2400, 1600),
    11: (2800, 2800, 2600, 2200, 1600),
    12: (2600, 2800, 2400, 2000, 1600),
}
_NNORM_TABLE_KS = (
    (1,),
    (3, 13, 14, 15, 22, 23),
    (4, 12),
    (6, 8, 9, 11, 19, 20, 21, 25, 26, 27, 28, 29, 32, 35, 50, 61, 62, 63, 64, 65, 66, 67, 68),
    (10, 16, 17, 18, 24),
)
_NNORM_TABLE = {h: {k: v for ks, v in zip(_NNORM_TABLE_KS, vs) for k in ks} for h, vs in _NNORM_TABLE_HS.items()}


def _get_nnorm(h: int, s: int) -> int:
    return _NNORM_TABLE.get(max(h, 6), {}).get(s, 0)


def _div(config: mezi_config.DownloadConfig, a: decimal.Decimal, b: decimal.Decimal, msg: str = "") -> decimal.Decimal:
    try:
        return a / b
    except decimal.DecimalException as exc:
        config.print(f"{msg}\n{exc}")
    return decimal.Decimal(0)


def _round_ks(ks: MutableSequence[decimal.Decimal]) -> None:
    while True:
        # grab, sum and check rounded ks
        rks = tuple(round(k) for k in ks)
        srk = sum(rks)
        if srk == 10:  # noqa: PLR2004
            # sum 10, all ok
            break
        # not 10, grab deltas
        dks = tuple(abs(rk - k) for rk, k in zip(rks, ks))
        # offset k with max delta so that sum approaches 10
        ks[dks.index(max(dks))] += decimal.Decimal(math.copysign(0.5, 10 - srk))


def _get_g(d: decimal.Decimal, n: int) -> decimal.Decimal:
    # Izriet no vienkarsas geometriskas sakaribas N rinka laukumu summam
    # Attiecigi skerslaukums (m2/ha) iegustams sekojosi:
    # GXX = pi()* DXX^2 * NXX / 40000, kur
    # DXX - meza elementa videjais krusaugstuma caurmers, cm
    # NXX - meza elementa koku skaits, gb/ha
    return decimal.Decimal(math.pi) * d * d * n / 40000


def _get_biez(config: mezi_config.DownloadConfig, row: pd.Series) -> float:  #  pyright: ignore [reportUnknownParameterType, reportMissingTypeArgument]
    ks = [decimal.Decimal(0), decimal.Decimal(0), decimal.Decimal(0), decimal.Decimal(0), decimal.Decimal(0)]
    ss = [decimal.Decimal(row.get("s10", 0)), decimal.Decimal(row.get("s11", 0)), decimal.Decimal(row.get("s12", 0)), decimal.Decimal(row.get("s13", 0)), decimal.Decimal(row.get("s14", 0))]
    hs = [decimal.Decimal(row.get("h10", 0)), decimal.Decimal(row.get("h11", 0)), decimal.Decimal(row.get("h12", 0)), decimal.Decimal(row.get("h13", 0)), decimal.Decimal(row.get("h14", 0))]
    ds = [decimal.Decimal(row.get("d10", 0)), decimal.Decimal(row.get("d11", 0)), decimal.Decimal(row.get("d12", 0)), decimal.Decimal(row.get("d13", 0)), decimal.Decimal(row.get("d14", 0))]
    gs = [decimal.Decimal(row.get("g10", 0)), decimal.Decimal(row.get("g11", 0)), decimal.Decimal(row.get("g12", 0)), decimal.Decimal(row.get("g13", 0)), decimal.Decimal(row.get("g14", 0))]
    ns = [decimal.Decimal(row.get("n10", 0)), decimal.Decimal(row.get("n11", 0)), decimal.Decimal(row.get("n12", 0)), decimal.Decimal(row.get("n13", 0)), decimal.Decimal(row.get("n14", 0))]
    for index in range(4, -1, -1):
        if not sum((ss[index], hs[index], ds[index], gs[index], ns[index])):
            for s in (ks, ss, hs, ds, gs, ns):
                s.pop(index)
    if not ks:
        config.print(f"[{row.name}] Nav audzes datu.")
        return 0
    if decimal.Decimal(0) not in gs:
        # ja g visam sugam rekinam
        # config.print(f"[{row.name}] Rekinam pec skerslaukuma.")
        # g total
        sg = decimal.Decimal(sum(gs))
        # k sugam
        for j, g in enumerate(gs):
            ks[j] = _div(config, g, sg, f"[{row.name}] Skerslaukumu summa dod 0, teoretiski sim nevajadzetu notikt ...") * 10
        _round_ks(ks)
        # calc valdosa by k
        vi = ks.index(max(ks))
        # b total
        return float(_div(config, sg, _get_gnorm(int(hs[vi]), int(ss[vi])), f"[{row.name}] Nezinama gnorm vertiba sadai sugas un augstuma kombinacijai.") * 10)
    if gs != [0] * len(gs):
        # ja tikai kadam ir g tad warn
        config.print(f"[{row.name}] Lai rekinatu pec skerslaukumiem, tiem jabut pie visam sugam.")
        return 0
    # g nav nevienam
    if decimal.Decimal(0) not in ns:
        # ja n visam sugam rekinam
        # config.print(f"[{row.name}] Rekinam pec skaita.")
        sn = decimal.Decimal(sum(ns))
        # g sugam
        for j, (d, n) in enumerate(zip(ds, ns)):
            gs[j] = _get_g(d, int(n))
        # g total
        sg = decimal.Decimal(sum(gs))
        # k sugam
        for j, g in enumerate(gs):
            ks[j] = _div(config, g, sg, f"[{row.name}] Skerslaukumu summa dod 0, teoretiski sim nevajadzetu notikt ...") * 10
        _round_ks(ks)
        # calc valdosa by k
        vi = ks.index(max(ks))
        # b total
        return float(_div(config, sn, decimal.Decimal(_get_nnorm(int(hs[vi]), int(ss[vi]))), f"[{row.name}] Nezinama nnorm vertiba sadai sugas un augstuma kombinacijai.") * 10)
    if ns != [0] * len(ns):
        # ja tikai kadam ir n tad warn
        config.print(f"[{row.name}] Lai rekinatu pec skaitiem, tiem jabut pie visam sugam.")
        return 0
    config.print(f"[{row.name}] Neviena aprekina metode neatbilst stava konfiguracijai.")
    return 0


def download_mvr(config: mezi_config.DownloadConfig) -> None:
    apgs_cache_path = os.path.join(config.MVR_CACHE_PATH, "apgs")
    os.makedirs(apgs_cache_path, exist_ok=True)
    apgs_path = os.path.join(apgs_cache_path, f"{config.name}.gpkg")
    config.OUTPUT_FILES_TO_ZIP.append(apgs_path)
    if not config.MVR_CACHE_FORCE_INVALIDATE and os.path.isfile(apgs_path):
        return
    if config.MVR_DIRECT_DATA_PATH:
        config.print(f"loading apgs from '{config.MVR_DIRECT_DATA_PATH}'")
        apgs = geom.filter(geom.read_file(config, config.MVR_DIRECT_DATA_PATH, bbox=config.bbox), config.wkt, config.bbox)
    else:
        config.print(f"loading apgs from '{config.MVR_DATA_PATH}'")
        vms_cache_path = os.path.join(config.MVR_CACHE_PATH, "vms")
        vms: pd.Series[str] = geom.filter(geom.read_file(config, config.MVR_TERITORIJAS_VM_GPKG_PATH, config.MVR_TERITORIJAS_VM_LAYER, config.bbox), config.wkt, config.bbox)[config.MVR_TERITORIJAS_VM_FIELD]  # pyright: ignore [reportAssignmentType]
        vms_len = vms.shape[0]
        suffix = f"of {vms_len}"
        for current, vm in enumerate(vms):
            mezi_config.print_progress_bar(config, current, vms_len, f"downloading '{vm}' from {config.MVR_DATA_PATH}", suffix)
            mezi_config.download_data(config, config.MVR_DATA_PATH, vm, vms_cache_path, config.MVR_CACHE_FORCE_INVALIDATE)
        mezi_config.print_progress_bar(config, vms_len, vms_len, "all vms downloaded", suffix)
        mzns_cache_path = os.path.join(config.MVR_CACHE_PATH, "mzns")
        os.makedirs(mzns_cache_path, exist_ok=True)
        mzns = {f"nodala{mzn}" for mzn in geom.filter(geom.read_file(config, config.MVR_TERITORIJAS_MZN_GPKG_PATH, config.MVR_TERITORIJAS_MZN_LAYER, config.bbox), config.wkt, config.bbox)[config.MVR_TERITORIJAS_MZN_FIELD]}
        for current, vm in enumerate(vms):
            with py7zr.SevenZipFile(os.path.join(vms_cache_path, vm.lower())) as szip:
                names = {name for name in szip.getnames() if name.split(".")[0] in mzns}
                mezi_config.print_progress_bar(config, current, vms_len, f"extracting {len(names)} mzns from '{vm}'", suffix)
                if not config.MVR_CACHE_FORCE_INVALIDATE:
                    names = {name for name in names if not os.path.isfile(os.path.join(mzns_cache_path, name))}
                if names:
                    szip.extract(mzns_cache_path, names)
        mezi_config.print_progress_bar(config, vms_len, vms_len, "all mzns extracted", suffix)
        apgs = []
        mzns_len = len(mzns)
        suffix = f"of {mzns_len}"
        for current, mzn in enumerate(mzns):
            mzn = f"{mzn}.shp"
            mezi_config.print_progress_bar(config, current, mzns_len, f"loading apgs from '{mzn}'", suffix)
            apgs.append(geom.filter(geom.read_file(config, os.path.join(mzns_cache_path, mzn), bbox=config.bbox), config.wkt, config.bbox))
        mezi_config.print_progress_bar(config, mzns_len, mzns_len, "all apgs loaded", suffix)
        apgs = mezi_config.concat(apgs)
    if apgs.empty:
        config.print("empty apgs")
        config.OUTPUT_FILES_TO_ZIP.remove(apgs_path)
        return
    if "biez" not in apgs.columns:
        apgs["biez"] = apgs.apply(lambda row: _get_biez(config, row), axis=1)
    if "vgr" not in apgs.columns:
        apgs["vgr"] = 0  # TODO
    for col in set(_MVR_SCHEMA["properties"]) - set(apgs.columns):
        apgs[col] = None
    schema = copy.deepcopy(_MVR_SCHEMA)
    for col, rule, csvs in config.MVR_RULES:
        config.print(f"applying rule '{col}'")
        schema["properties"][col] = "float"  # pyright: ignore [reportIndexIssue]
        if not rule:
            rule = "0"
        rule = rule.lower()
        maps = []
        for csv in csvs:
            csv = pd.read_csv(csv)
            csv.columns = csv.columns.str.lower()
            data = csv.to_dict("list")
            keys = data[csv.columns[0]]
            keys.extend(tuple(str(key) for key in keys))
            values = data[csv.columns[1]]
            values.extend(values)
            maps.append((csv.columns[0].lower(), dict(zip(keys, values))))

        def f(rec: pd.Series) -> Any:  #  pyright: ignore [reportUnknownParameterType, reportMissingTypeArgument]
            _rec = rec.to_dict()
            col, _map = maps[eval(rule, {}, _rec)]  # noqa: S307
            return _map.get(_rec[col])

        apgs[col] = apgs.apply(f, axis=1)
    geom.write_file(config, apgs[["geometry", *schema["properties"]]].explode(ignore_index=True), apgs_path, layer="apgs", schema=schema, engine="fiona")
    with sqlite3.connect(apgs_path) as conn:
        conn.enable_load_extension(True)
        conn.load_extension("mod_spatialite")
        cursor = conn.cursor()
        try:
            cursor.execute(_MVR_ARSTNIECIBA)
            cursor.execute(_MVR_BRUKLENES)
            cursor.execute(_MVR_FITOREMEDIACIJA)
            cursor.execute(_MVR_FLORISTIKA)
            cursor.execute(_MVR_KOSMETIKA)
            cursor.execute(_MVR_MELLENES)
            cursor.execute(_MVR_NOTURIBA)
            cursor.execute(_MVR_PIEVILCIBA)
            cursor.execute(_MVR_REKREACIJA)
            cursor.execute(_MVR_TROKSNIS)
            cursor.execute(_MVR_UGUNSBISTAMIBA)
        finally:
            cursor.close()
