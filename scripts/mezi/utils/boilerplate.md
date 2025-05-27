# How to use boilerplate AKA boilerplate boilerplate

Use without extra CLI args:

```python
#!/usr/bin/env python
import sys


import geopandas as gpd


from mezi.utils import config, geom, boilerplate


# define config cls with defaults
class Config(config.Config):
    TODO = TODO # default value for config variable, note that these must be compatible with json notation


# main function, executes the main logic
def extra_main(config=Config(), wkt=geom.DEFAULT_WKT, bbox=geom.DEFAULT_BBOX):
    gdf = gpd.read_file(config.TODO, wkt or bbox) # load and filter from wherever
    # gdf = geom.filter(gdf, wkt, bbox) # or load everything and filter after
    return 0 # don't forget return code


# build main entry point
parse, main = boilerplate.get_parse_main(Config, __name__, extra_main)


# and execute it with us
if __name__ == "__main__":
    sys.exit(main(**vars(parse())))
```

Use with extra CLI args:

```python
#!/usr/bin/env python
import sys


import geopandas as gpd


from mezi.utils import config, geom, boilerplate


# define config cls with defaults
class Config(config.Config):
    TODO = TODO # default value for config variable, note that these must be compatible with json notation


# extra arg default
_DEFAULT_TODO = TODO


# extra arg type, note that input is str
def _TODO_type(TODO):
    return TODO


# parse function, defines extra CLI args
def extra_parse(parser):
    parser.add_argument("-TODO", "--TODO", default=_DEFAULT_TODO, type=_TODO_type, help="TODO, defaults to %(default)s") # add extra arg


# main function, executes the main logic
def extra_main(config=Config(), wkt=geom.DEFAULT_WKT, bbox=geom.DEFAULT_BBOX, TODO=_DEFAULT_TODO):
    gdf = gpd.read_file(config.TODO, wkt or bbox) # load and filter from wherever
    # gdf = geom.filter(gdf, wkt, bbox) # or load everything and filter after
    print(TODO) # do something with the extra arg
    return 0 # don't forget return code


# build main entry point
parse, main = boilerplate.get_parse_main(Config, __name__, extra_main, extra_parse)


# and execute it with us
if __name__ == "__main__":
    sys.exit(main(**vars(parse())))

```
