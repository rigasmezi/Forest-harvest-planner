
# Author: LU, Rīgas Meži, SunGIS
# Created: 2024
# License: EUPL License

# Dependencies: environment.yml
# Python Version: 3.12+

import time


import dijkstra3d
import matplotlib.pyplot as plt
import numpy as np


def route(path, start, end, compass, connectivity):
    _time_start = time.time()
    data = np.pad(plt.imread(path).astype(np.float32), 1, constant_values=np.inf)
    data[data == 1] = np.inf
    data[data == 0] = 1
    _time_init = time.time()
    route = dijkstra3d.dijkstra(data, start, end, compass=compass, connectivity=connectivity)
    _time_loop = time.time()
    route = (tuple(route[::, 0]), tuple(route[::, 1]))
    _time_path = time.time()
    time_init = (_time_init - _time_start) * 1000
    time_loop = (_time_loop - _time_init) * 1000
    time_path = (_time_path - _time_loop) * 1000
    time_all = (_time_path - _time_start) * 1000
    print("init", time_init)
    print("loop", time_loop)
    print("path", time_path)
    print("all", time_all)
    return data, tuple(route)[::-1]


def show(maps, routes, start, end):
    ax = plt.subplots(1, len(maps))[1]
    for i in range(len(maps)):
        ax[i].imshow(maps[i])
        ax[i].plot(*start, "ro")
        ax[i].plot(*end, "go")
        ax[i].plot(*routes[i])
    plt.show()


path = "/home/zintis/git/govgis/lad-mezi/data/zmni/tif/8949719423249892623_zmni_warp.tif"
start = (10, 50)
end = (50, 270)
data = [
    route(path, start[::-1], end[::-1], True, 4),
    route(path, start[::-1], end[::-1], True, 8),
    route(path, start[::-1], end[::-1], False, 4),
    route(path, start[::-1], end[::-1], False, 8),
]
show(*zip(*data), start, end)
