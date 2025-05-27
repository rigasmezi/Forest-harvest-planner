# Author: LU, Rīgas Meži, SunGIS
# Created: 2024
# License: EUPL License

# Dependencies: environment.yml
# Python Version: 3.12+

import json
import os
from collections.abc import Callable
from copy import deepcopy
from typing import Any, Self

import shapely


class Config:
    _path: str | None
    _config: dict[str, Any]
    _defaults: dict[str, Any]

    _print: Callable[[str], None] = print

    name: str
    wkt: shapely.Geometry | None
    bbox: tuple[float, ...] | None

    def print(self: Self, msg: str) -> None:
        self._print(f"[{self.name}] {msg}")

    def __init__(self: Self, path: str | None = None, config: dict[str, Any] | None = None) -> None:
        self._path = path
        if path:
            try:
                with open(path, encoding="utf-8") as file:
                    config = json.load(file)
            except FileNotFoundError:
                pass
        self._config = config or {}
        self._defaults = {key: deepcopy(getattr(self, key)) for key in dir(self) if key.isupper()}
        for key, value in self._config.items():
            if key not in self._defaults:
                raise ValueError(f"unknown key({key})")
            if not issubclass(type(value), type(self._defaults[key])):
                raise TypeError(f"key({key}) value({value}) type mismatch, default({type(self._defaults[key])}) - config({type(value)})")
            setattr(self, key, value)

    def __str__(self: Self) -> str:
        return str({key: getattr(self, key) for key in self._defaults})

    def dump(self: Self, path: str | None = None) -> dict[str, Any]:
        copy = {key: deepcopy(getattr(self, key)) for key in self._defaults}
        if path:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w", encoding="utf-8") as file:
                json.dump(copy, file, ensure_ascii=False, indent=4, sort_keys=True)
        return copy


def get_config_type(config_cls: type[Config]) -> Callable[[Config | str | dict[str, Any] | None], Config]:
    def config_type(config: Config | str | dict[str, Any] | None = None) -> Config:
        if isinstance(config, config_cls):
            return config
        if isinstance(config, str):
            return config_cls(os.path.abspath(config))
        return config_cls(config=config)  # pyright: ignore [reportArgumentType]

    return config_type
