
# Author: LU, Rīgas Meži, SunGIS
# Created: 2024
# License: EUPL License

# Dependencies: environment.yml
# Python Version: 3.12+

import datetime
import functools
import os
import shutil
import time
from collections.abc import Callable
from typing import Any

import requests

_SILENCER_SENTINEL = object()


def _silencer(func: Callable[..., Any], exc_class: type[BaseException] | tuple[type[BaseException], ...], default: Any = _SILENCER_SENTINEL) -> Callable[..., Any]:
    """
    catches exc_class from func

    returns wrapper of func that returns:
        if default is not given: True if func executed successfully or False on Exception
        if default is given: func result on successful execution or default on Exception
    """

    def silent(*args: Any, **kwargs: Any) -> Any:
        try:
            result = func(*args, **kwargs)
        except exc_class:
            return default is not _SILENCER_SENTINEL and default
        return default is _SILENCER_SENTINEL or result

    return silent


silent_makedirs = _silencer(os.makedirs, OSError)
silent_unlink = _silencer(os.unlink, OSError)
silent_rmdir = _silencer(os.rmdir, OSError)
silent_rmtree = _silencer(shutil.rmtree, OSError)


_start = time.time()


def print_progress_bar(
    current: int,
    total: int,
    prefix: str = "",
    suffix: str = "",
    elapsed: bool = True,
    decimals: int = 2,
    min_length: int = 5,
    max_length: int | None = None,
    fill: str = "█",
    empty: str = "-",
    swap_bar_percent: bool = False,
) -> None:
    percent = f"{((100 * current / total) if total else 100):{decimals + 4}.{decimals}f}%"
    if elapsed and not current:
        global _start  # noqa: PLW0603
        _start = time.time()
    prefix = f"{prefix.expandtabs()} " if prefix else ""
    len_prefix = len(prefix)
    suffix = f" {suffix.expandtabs()}" if suffix else ""
    len_suffix = len(suffix)
    _elapsed = f" {datetime.timedelta(seconds=time.time() - _start)}" if elapsed else ""
    length = shutil.get_terminal_size()[0] - len_prefix - len(percent) - len_suffix - len(_elapsed) - 1
    min_length = min_length if max_length is None else min(min_length, max_length)
    if length < min_length:
        delta = min_length - length
        if _elapsed:
            cutoff = min(7, delta)
            if cutoff == 6:  # noqa: PLR2004
                cutoff = 7
            _elapsed = _elapsed[:-cutoff]
            delta -= cutoff
        if prefix and delta > 0:
            cutoff = min(len_prefix, delta)
            if cutoff == len_prefix - 1:
                cutoff = len_prefix
            prefix = prefix[:-cutoff]
            if prefix:
                prefix = prefix[:-1] + " "
            delta -= cutoff
        if suffix and delta > 0:
            cutoff = min(len_suffix, delta)
            if cutoff == len_suffix - 1:
                cutoff = len_suffix
            suffix = suffix[cutoff:]
            if suffix:
                suffix = " " + suffix[1:]
            delta -= cutoff
        length = min_length - delta
    length = max(0, min(max_length or length, length))
    fill_length = (length * current // total) if total else length
    bar = f"{fill * fill_length}{empty * (length - fill_length)}"
    # see https://stackoverflow.com/a/5291396, https://stackoverflow.com/a/5291044
    # and https://en.wikipedia.org/wiki/ANSI_escape_code#CSI_(Control_Sequence_Introducer)_sequences
    print(f"\033[G\033[K\r{prefix}{f'{percent} {bar}' if swap_bar_percent else f'{bar} {percent}'}{suffix}{_elapsed}", end="\r" if current < total else "\n", flush=True)


def download(source_path: str, sink_path: str, force: bool) -> str:
    sink_path = os.path.abspath(sink_path)
    if not force and os.path.isfile(sink_path):
        return sink_path
    os.makedirs(os.path.dirname(sink_path), exist_ok=True)
    try:
        with open(os.path.abspath(source_path), "rb") as source, open(sink_path, "wb") as sink:
            shutil.copyfileobj(source, sink)
    except (FileNotFoundError, OSError):
        try:
            # modified version of https://stackoverflow.com/a/39217788 and https://github.com/psf/requests/issues/2155#issuecomment-50771010
            with requests.get(source_path.replace("\\", "/"), stream=True, timeout=30) as response, open(sink_path, "wb") as sink:
                response.raise_for_status()
                response.raw.read = functools.partial(response.raw.read, decode_content=True)
                shutil.copyfileobj(response.raw, sink)
        except:
            silent_unlink(sink_path)
            raise
    except:
        silent_unlink(sink_path)
        raise
    return sink_path
