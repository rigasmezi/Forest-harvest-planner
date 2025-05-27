# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false


# Author: LU, Rīgas Meži, SunGIS
# Created: 2024
# License: EUPL License

# Dependencies: environment.yml
# Python Version: 3.12+

import itertools
import os
from collections import defaultdict
from collections.abc import Generator, Iterable

import geopandas as gpd
import numpy as np
import rasterio
import shapely

from mezi import config as mezi_config
from mezi.utils import geom

_TIF_RESAMPLE_NODATA = " -dstnodata '{nodata}'"
_TIF_RESAMPLE = "gdalwarp -tr {resolution} {resolution} -tap -ovr NONE -ot '{output_type}' -r '{method}'{nodata} -overwrite '{input_path}' '{output_path}'"


def _distance_filter(xy: np.ndarray[tuple[int, int], np.dtype[np.float32]], distance: float, mask: np.ndarray[tuple[int], np.dtype[np.bool]]) -> np.ndarray[tuple[int, int], np.dtype[np.float32]]:
    distance *= distance
    for i in range(xy.shape[0]):
        if not mask[i]:
            continue
        delta_xy = xy[mask] - xy[i]
        delta_xy *= delta_xy
        mask[mask] = delta_xy.sum(1) >= distance
        mask[i] = True
    return xy[mask]


def _non_neighbor_combinations(cluster: set[int], count: int, neighbors: dict[int, set[int]]) -> Generator[tuple[int, ...]]:
    _cluster = tuple(cluster)
    _len = len(_cluster)
    if count > _len:
        return
    indices = list(range(count))
    dimensions = tuple(reversed(range(count)))
    yield tuple(_cluster[index] for index in indices)
    while True:
        for dimension in dimensions:
            if indices[dimension] != dimension + _len - count:
                break
        else:
            return
        indices[dimension] += 1
        for _dimension in range(dimension + 1, count):
            indices[_dimension] = indices[_dimension - 1] + 1
        for key_index, value_index in itertools.combinations(indices, 2):
            if _cluster[value_index] in neighbors[_cluster[key_index]]:
                break
        else:
            yield tuple(_cluster[index] for index in indices)


def _iter_cluster_candidates(cluster: set[int], neighbors: dict[int, set[int]]) -> Generator[Iterable[int]]:
    yield from itertools.chain.from_iterable(_non_neighbor_combinations(cluster, count, neighbors) for count in range(1, len(cluster)))


def _iter_candidates(clusters: list[set[int]], neighbors: dict[int, set[int]]) -> Generator[Iterable[Iterable[int]]]:
    yield from itertools.product(*(_iter_cluster_candidates(cluster, neighbors) for cluster in clusters))


def _iter_demotion_candidates(chop_set: set[int], clusters: list[set[int]], neighbors: dict[int, set[int]]) -> Generator[tuple[set[int], set[int]]]:
    free = chop_set.difference(itertools.chain.from_iterable(clusters))
    yield from ((_candidate, chop_set - _candidate) for candidate in _iter_candidates(clusters, neighbors) if not _has_chop_clusters(_candidate := free.union(itertools.chain.from_iterable(candidate)), neighbors))


def _has_chop_clusters(chop_set: set[int], neighbors: dict[int, set[int]]) -> bool:
    return any(neighbors[index] & chop_set for index in chop_set)


def _get_chop_clusters(chop_set: set[int], neighbors: dict[int, set[int]]) -> list[set[int]]:
    clusters = []
    _chop_set = chop_set.copy()
    while _chop_set:
        index = _chop_set.pop()
        _neighbors = neighbors[index] & chop_set
        if not _neighbors:
            continue
        _neighbors = _neighbors.copy()
        cluster = {index}
        while _neighbors:
            neighbor = _neighbors.pop()
            cluster.add(neighbor)
            _neighbors.update(neighbors[neighbor] & chop_set)
            _neighbors.difference_update(cluster)
        _chop_set.difference_update(cluster)
        clusters.append(cluster)
    return clusters


def tessellate(config: mezi_config.DownloadConfig) -> None:
    os.makedirs(config.TESSELLATE_GPKG_CACHE_PATH, exist_ok=True)
    output_path = os.path.join(config.TESSELLATE_GPKG_CACHE_PATH, f"{config.name}.gpkg")
    config.OUTPUT_FILES_TO_ZIP.append(output_path)
    config.print(f"tessellating to '{output_path}'")
    if not config.TESSELLATE_GPKG_CACHE_FORCE_INVALIDATE and os.path.isfile(output_path):
        return
    bbox: tuple[float, float, float, float] = config.bbox or (0, 0, 1, 1)  # pyright: ignore [reportAssignmentType]
    _geom = config.wkt or shapely.box(*bbox)
    if config.TESSELLATE_REMOVE_BEFORE_TESSELLATION:
        _remove = shapely.unary_union(tuple(geom.read_file(config, os.path.join(path, f"{config.name}.gpkg"), layer, bbox).geometry.union_all() for path, layers in config.TESSELLATE_REMOVE_BEFORE_TESSELLATION.items() for layer in layers))
        _geom = shapely.difference(_geom.buffer(0), _remove.buffer(0))  # pyright: ignore [reportAttributeAccessIssue]
    _remove = None
    if config.TESSELLATE_REMOVE_AFTER_TESSELLATION:
        _remove = shapely.unary_union(tuple(geom.read_file(config, os.path.join(path, f"{config.name}.gpkg"), layer, bbox).geometry.union_all() for path, layers in config.TESSELLATE_REMOVE_AFTER_TESSELLATION.items() for layer in layers))
        _remove = _remove.buffer(0)
    points = []
    cells = []
    cell_fields = tuple([] for _ in config.TESSELLATE_POLYGON_SPLIT_ADD_FIELDS)
    cell_areas = []
    split_gdf = None
    split_indices = (0,)
    split_geoms = (_geom,)
    if config.TESSELLATE_POLYGON_SPLIT_PATH:
        split_gdf = geom.read_file(config, os.path.join(config.TESSELLATE_POLYGON_SPLIT_PATH, f"{config.name}.gpkg"), config.TESSELLATE_POLYGON_SPLIT_LAYER, bbox)
        if config.TESSELLATE_POLYGON_SPLIT_INCLUDE_RULE:
            split_gdf = split_gdf[split_gdf.apply(lambda rec: eval(config.TESSELLATE_POLYGON_SPLIT_INCLUDE_RULE, globals(), rec), 1)]  # noqa: S307
        split_indices = split_gdf.index
        split_geoms = split_gdf.geometry
    tif_path = os.path.join(config.TESSELLATE_POINT_TIF_PATH, f"{config.name}{config.TESSELLATE_POINT_TIF_SUFFIX}.tif")
    if config.TESSELLATE_POINT_METHOD == "tif" and config.TESSELLATE_POINT_TIF_RESAMPLE:
        resample_path = os.path.join(config.TESSELLATE_POINT_TIF_PATH, f"{config.name}{config.TESSELLATE_POINT_TIF_SUFFIX}_resample.tif")
        mezi_config.check_call(
            config,
            _TIF_RESAMPLE.format(
                resolution=config.TESSELLATE_POINT_TIF_RESAMPLE_RESOLUTION,
                output_type="Float32",
                method=config.TESSELLATE_POINT_TIF_RESAMPLE_METHOD,
                nodata=_TIF_RESAMPLE_NODATA.format(nodata=255) if 255 else "",
                input_path=tif_path,
                output_path=(tif_path := resample_path),
            ),
        )
    for split_index, split_geom in itertools.chain.from_iterable(geom.index_polygons(split_index, _geom.intersection(split_geom).buffer(0)) for split_index, split_geom in zip(split_indices, split_geoms)):  # pyright: ignore [reportAttributeAccessIssue]
        if config.TESSELLATE_POLYGON_SIMPLIFY_TOLERANCE:
            split_geom = split_geom.simplify(config.TESSELLATE_POLYGON_SIMPLIFY_TOLERANCE)
        if split_geom.area < config.TESSELLATE_POLYGON_MIN_AREA:
            continue
        if config.TESSELLATE_POINT_METHOD == "direct":  # pyright: ignore [reportUnnecessaryComparison]
            _points = split_geom
        elif config.TESSELLATE_POINT_METHOD == "tif":  # pyright: ignore [reportUnnecessaryComparison]
            with rasterio.open(tif_path) as raster:
                bbox = split_geom.bounds
                window = raster.window(bbox[0] - 1, bbox[1] - 1, bbox[2] + 1, bbox[3] + 1)
                rows, cols = window.toslices()
                out = np.zeros((rows.stop - rows.start, cols.stop - cols.start), raster.dtypes[0])
                x, y = raster.xy(*zip(*itertools.product(range(rows.start, rows.stop), range(cols.start, cols.stop))))
                values = raster.read(1, out, window=window, masked=False, boundless=True, fill_value=raster.nodata)
                mask = shapely.intersects_xy(split_geom, x, y).reshape(out.shape) & (values != raster.nodata)
            xy = np.array(tuple(zip(x, y))).reshape(*out.shape, 2)[mask]
            values = values[mask]
            values_xy = np.append(values.reshape(*values.shape, 1), xy, 1)
            values_xy = values_xy[np.argsort(values_xy[:, 0], stable=True)[::-1]]
            xy = values_xy[:, 1:]
            if config.TESSELLATE_POINT_INCLUDE_BORDER and config.TESSELLATE_POINT_INCLUDE_BORDER_BEFORE_DISTANCE_FILTER:
                xy = np.append(xy, tuple(geom.points(split_geom)), 0)
            _points = shapely.MultiPoint(_distance_filter(xy, config.TESSELLATE_POINT_MIN_DISTANCE, np.ones(xy.shape[0], np.bool)))
        elif config.TESSELLATE_POINT_METHOD == "random":
            min_x, min_y, max_x, max_y = split_geom.bounds
            delta_x = max_x - min_x
            delta_y = max_y - min_y
            count = int((delta_x / config.TESSELLATE_POINT_MIN_DISTANCE + 1) * (delta_y / config.TESSELLATE_POINT_MIN_DISTANCE + 1) * 10)
            random = np.random.default_rng(config.TESSELLATE_SEED)
            xy = random.uniform((min_x, min_y), (max_x, max_y), (count, 2)).astype(np.float32)
            if config.TESSELLATE_POINT_INCLUDE_BORDER and config.TESSELLATE_POINT_INCLUDE_BORDER_BEFORE_DISTANCE_FILTER:
                xy = np.append(xy, tuple(geom.points(split_geom)), 0)
            _points = shapely.MultiPoint(_distance_filter(xy, config.TESSELLATE_POINT_MIN_DISTANCE, shapely.contains_xy(split_geom, xy)))
        else:
            config.print(f"unknown tesselation point method: '{config.TESSELLATE_POINT_METHOD}', known: 'direct', 'tif', 'random'")
            return
        if _points.is_empty:
            _points = shapely.MultiPoint((split_geom.centroid,))
        if config.TESSELLATE_POINT_INCLUDE_BORDER and not config.TESSELLATE_POINT_INCLUDE_BORDER_BEFORE_DISTANCE_FILTER:
            _points = shapely.MultiPoint(tuple({*geom.points(_points), *geom.points(split_geom)}))
        try:
            if config.TESSELLATE_POLYGON_METHOD == "voronoi":
                _cells = shapely.voronoi_polygons(_points).geoms
            elif config.TESSELLATE_POLYGON_METHOD == "delaunay":
                _cells = shapely.delaunay_triangles(_points).geoms
            else:
                config.print(f"unknown tesselation polygon method: '{config.TESSELLATE_POLYGON_METHOD}', known: 'voronoi', 'delaunay'")
                return
        except shapely.errors.GEOSException as exc:
            config.print(f"got '{exc}' during '{config.TESSELLATE_POLYGON_METHOD}' tesselation of '{_points.wkt}', skipping")
            continue
        if config.TESSELLATE_REMOVE_AFTER_TESSELLATION:
            _cells = tuple(shapely.difference(cell.buffer(0), _remove) for cell in _cells)
        _cells = itertools.chain.from_iterable(geom.polygons(split_geom.intersection(cell).buffer(0)) for cell in _cells)  # pyright: ignore [reportOptionalMemberAccess]
        points.extend(_points.geoms)  # pyright: ignore [reportAttributeAccessIssue]
        _cells = tuple(cell for cell in _cells if cell.area >= config.TESSELLATE_POLYGON_MIN_AREA)
        cells.extend(_cells)
        if split_gdf is None:
            _values = (None,) * len(_cells)
            for values in cell_fields:
                values.extend(_values)
            cell_areas.extend(_values)
        else:
            for field, values in zip(config.TESSELLATE_POLYGON_SPLIT_ADD_FIELDS, cell_fields):
                values.extend((split_gdf[field][split_index],) * len(_cells))
            split_area = 0.01 * split_gdf.geometry[split_index].area
            cell_areas.extend(cell.area / split_area for cell in _cells)
    data = dict(zip(config.TESSELLATE_POLYGON_SPLIT_ADD_FIELDS, cell_fields))
    data["split_area_percentile"] = cell_areas
    data["geometry"] = cells
    cells = tuple((cell, cell.bounds, {}) for cell in cells)
    tifs = tuple(path for path in config.OUTPUT_FILES_TO_ZIP if path.endswith(".tif"))
    total = len(cells) * len(tifs)
    suffix = f"of {total}"
    current = 0
    head = config.TESSELLATE_STATS + config.TESSELLATE_PERCENTILES + sorted(config.TESSELLATE_CALC)
    for path in tifs:
        name = os.path.basename(path).replace(".tif", "").replace(f"{config.name}_", "")
        value_percs = sorted(config.TESSELLATE_VALUE_PERCENTILES.get(name, ()))
        stats = []
        with rasterio.open(path) as raster:
            affine = raster.transform
            for cell_id, (cell, cell_bbox, cell_windows) in enumerate(cells):
                mezi_config.print_progress_bar(config, current, total, f"calculating statistics for '{name}' cell {cell_id}", suffix)
                if cell.is_empty:
                    stats.append((np.nan,) * (len(head) + len(value_percs)))
                    continue
                if not (window_out_mask := cell_windows.get(affine)):
                    window = raster.window(cell_bbox[0] - 1, cell_bbox[1] - 1, cell_bbox[2], cell_bbox[3]).round_lengths()
                    rows, cols = window.toslices()
                    out = np.zeros((rows.stop - rows.start, cols.stop - cols.start), raster.dtypes[0])
                    x, y = raster.xy(*zip(*itertools.product(range(rows.start, rows.stop), range(cols.start, cols.stop))))
                    mask = shapely.intersects_xy(cell, x, y).reshape(out.shape)
                    cell_windows[affine] = window_out_mask = window, out, mask
                window, out, mask = window_out_mask
                cell_data = raster.read(1, out, window=window, masked=False, boundless=True, fill_value=raster.nodata)
                cell_data = cell_data[mask & (cell_data != raster.nodata)]
                if len(cell_data):
                    stats.append(
                        (
                            *(getattr(cell_data, stat)() for stat in config.TESSELLATE_STATS),
                            *np.percentile(cell_data, config.TESSELLATE_PERCENTILES),
                            *(eval(calc, globals(), {"data": cell_data, "mask": mask, "raster": raster}) for _, calc in sorted(config.TESSELLATE_CALC.items())),  # noqa: S307
                            *(100 * (cell_data == value).sum() / cell_data.size for value in value_percs),
                        ),
                    )
                else:
                    stats.append((np.nan,) * (len(head) + len(value_percs)))
                current += 1
        data.update({f"{name}_{stat}{'_percentile' if isinstance(stat, int | float) else ''}": values for stat, *values in zip(head, *stats)})
        data.update({f"{name}_value_{value}_percentile": values for value, *values in zip(value_percs, *(stat[len(head) :] for stat in stats))})
    mezi_config.print_progress_bar(config, total, total, "all statistics calculated", suffix)
    splits = defaultdict(list)
    for index, (cell_value, cell_geom, cell_area, *split_key) in enumerate(
        zip(
            data[config.TESSELLATE_PRIORITY_OPTIMIZE_FIELD],
            data["geometry"],
            data["split_area_percentile"],
            *(data[field] for field in config.TESSELLATE_PRIORITY_SPLIT_KEY),
        ),
    ):
        splits[tuple(split_key)].append((cell_value * cell_area, cell_geom, cell_area, index))
    total = len(splits)
    suffix = f"of {total}"
    current = 0
    data_chops = []
    divs = config.TESSELLATE_PRIORITY_AREA_DIVISIONS
    for split_key, cells in splits.items():
        mezi_config.print_progress_bar(config, current, total, f"calculating chops for '{split_key}' split", suffix)
        cells = sorted(cells, key=lambda cell: cell[0], reverse=True)
        chops = tuple([] for _ in range(len(divs) + 1))
        chop_index = 0
        chop_area = 0
        chop_limit = divs[chop_index]
        chop = chops[chop_index]
        for cell_value, cell_geom, cell_area, index in cells:
            chop_area += cell_area
            if chop_area > chop_limit and chop_area != cell_area:
                chop_index += 1
                chop_area = cell_area
                chop_limit = divs[chop_index] if chop_index < len(divs) else np.inf
                chop = chops[chop_index]
            chop.append((cell_value, cell_geom, cell_area, index, chop_index))
        cells = tuple(itertools.chain.from_iterable(chops))
        final = {}
        chops = tuple(set() for _ in range(len(divs) + 1))
        neighbors = defaultdict(set)
        for offset, (cell_value, cell_geom, cell_area, index, chop_index) in enumerate(cells, 1):
            final[index] = [cell_value, cell_geom, cell_area, index, chop_index, len(divs)]
            chops[chop_index].add(index)
            for _, _cell_geom, _, _index, _ in cells[offset:]:
                if cell_geom is not _cell_geom and not cell_geom.intersection(_cell_geom).is_empty:
                    neighbors[index].add(_index)
                    neighbors[_index].add(index)
        for chop_index, chop_set in enumerate(chops[:-1]):
            clusters = _get_chop_clusters(chop_set, neighbors)
            demotion_candidates = _iter_demotion_candidates(chop_set, clusters, neighbors)
            candidates = []
            _candidate_limit = divs[chop_index]
            for candidate, demotion in demotion_candidates:
                _neighbors = set(itertools.chain.from_iterable(neighbors[index] for index in candidate))
                candidate_area = sum(final[index][2] for index in candidate)
                promotion_candidates = sorted(set(itertools.chain.from_iterable(chops[chop_index + 1 :])) - _neighbors)
                _candidates = []
                for index in promotion_candidates:
                    _promotion_candidates = set(promotion_candidates)
                    _candidate = set()
                    _candidate_area = candidate_area
                    while _promotion_candidates:
                        _candidate_area += final[index][2]
                        if _candidate_area > _candidate_limit:
                            break
                        _promotion_candidates.discard(index)
                        _candidate.add(index)
                        _promotion_candidates.difference_update(neighbors[index])
                        if _promotion_candidates:
                            index = min(_promotion_candidates)
                    if _candidate:
                        _candidates.append((sum(final[index][0] for index in _candidate), _candidate))
                promotion_value, promotion_candidate = max(_candidates, key=lambda candidate: candidate[0]) if _candidates else (0, set())
                candidates.append((sum(final[index][0] for index in candidate) + promotion_value, candidate, demotion, promotion_candidate))
            _, candidate, demotion, promotion = max(candidates, key=lambda candidate: candidate[0]) if candidates else (0, chop_set, set(), set())
            # for value, candidate, demotion, promotion in sorted(candidates, reverse=True, key=lambda candidate: candidate[0]):
            #     c_v = sum(final[index][0] for index in candidate)
            #     c_a = sum(final[index][2] for index in candidate)
            #     p_v = sum(final[index][0] for index in promotion)
            #     p_a = sum(final[index][2] for index in promotion)
            #     d_v = sum(final[index][0] for index in demotion)
            #     d_a = sum(final[index][2] for index in demotion)
            #     c_p = len(candidate & promotion)
            #     c_d = len(candidate & demotion)
            #     p_d = len(promotion & demotion)
            #     c_c = len(_get_chop_clusters(candidate, neighbors))
            #     p_c = len(_get_chop_clusters(promotion, neighbors))
            #     d_c = len(_get_chop_clusters(demotion, neighbors))
            #     c_p_c = len(_get_chop_clusters(candidate | promotion, neighbors))
            #     print(f"v={value} t={c_v + p_v} {c_a + p_a} {c_p_c} c={c_v} {c_a} {c_c} p={p_v} {p_a} {p_c} d={d_v} {d_a} {d_c} i={c_p} {c_d} {p_d}")
            for index in candidate:
                final[index][-1] = chop_index
            for index in demotion:
                final[index][-1] = len(divs)
                chops[chop_index].remove(index)
                chops[len(divs)].add(index)
            for index in promotion:
                final[index][-1] = chop_index
                chops[final[index][-2]].discard(index)
                chops[len(divs)].discard(index)
                chops[chop_index].add(index)
        data_chops.extend(final.values())
    mezi_config.print_progress_bar(config, total, total, "all chops calculated", suffix)
    data_chops = sorted(data_chops, key=lambda chop: chop[-3])
    data["initial_chop"] = [chop_index + 1 if (chop_index := chop[-2]) < len(divs) else 0 for chop in data_chops]
    data["final_chop"] = [chop_index + 1 if (chop_index := chop[-1]) < len(divs) else 0 for chop in data_chops]
    gdf = gpd.GeoDataFrame(data, geometry="geometry", crs="EPSG:3059")  # pyright: ignore [reportCallIssue]
    geom.write_file(config, gdf, output_path, layer=f"{config.TESSELLATE_GPKG_CACHE_LAYER}_cells", engine="pyogrio")
    gdf = gpd.GeoDataFrame({"geometry": points}, geometry="geometry", crs="EPSG:3059")  # pyright: ignore [reportCallIssue]
    geom.write_file(config, gdf, output_path, layer=f"{config.TESSELLATE_GPKG_CACHE_LAYER}_points", engine="pyogrio", unlink=False)
