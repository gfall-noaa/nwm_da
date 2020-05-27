#!/usr/bin/python3.6

import os
import datetime as dt
import sys
import pyproj
#import cartopy.crs as ccrs
from osgeo import gdal,osr,gdalconst
import numpy as np
from pyproj.utils import _convertback, _copytobuffer

"""
Functions for reading SNODAS climatology grids.
"""

def sample_grid_at_points(grid, row, col,
                          fill_value=None,
                          method='bilinear',
                          measure_wall_times=False):

    '''
    Sample grid value at a specified lat/lon location
    '''

    row_is_scalar = False
    if np.isscalar(row):
        row_arr = np.asarray(row)[None]
        row_is_scalar = True
    else:
        row_arr = np.asarray(row)

    col_is_scalar = False
    if np.isscalar(col):
        col_arr = np.asarray(col)[None]
        col_is_scalar = True
    else:
        col_arr = np.asarray(col)

    if fill_value is None:
        if np.issubdtype(grid.dtype, np.integer):
            fill_value = np.iinfo(grid.dtype).min
        elif np.issubdtype(grid.dtype, np.floating):
            fill_value = np.finfo(grid.dtype).min
        else:
            print('ERROR: Unsupported type "{}".'.format(grid.dtype),
                  file=sys.stderr)
            return None

    if method not in ['bilinear', 'neighbor']:
        print('ERROR: Method must be either "bilinear" or "neighbor".',
              file=sys.stderr)
        print('INFO: Method passed: ', method)
        return None

    if np.issubdtype(grid.dtype, np.integer) and \
       method == 'bilinear':
        print('WARNING: Bilinear sampling generates floating point ' +
              'results but input (as well as output) data are integers.',
              file=sys.stderr)

    num_rows, num_cols = grid.shape

    #if row.shape != col.shape:
    # if len(row) != len(col):
    #     print('ERROR: Grid row and col arrays must have the same shape.',
    #           file=sys.stderr)
    #     return None

    if measure_wall_times is True:
        time_start = time.time()
        full_time_start = time_start

    # Create "out" which is the same shape as col and row and the same
    # type as grid.
    # out = np.ma.masked_values(np.full_like(row_arr,
    #                                        dtype=grid.dtype, 
    #                                        fill_value=fill_value),
    #                           fill_value)

    out = np.full_like(row_arr,
                       dtype=grid.dtype,
                       fill_value=fill_value)
    out = np.ma.masked_where(out == fill_value, out)
    # out = np.ma.masked_all(row_arr.shape,
    #                        dtype=grid.dtype)
    # out[:] = np.ma.masked

    if measure_wall_times is True:
        time_finish = time.time()
        print('INFO: Created masked output array in {} seconds.'.
              format(time_finish - time_start))
        time_start = time.time()

    if method == 'bilinear':

        i1 = np.floor(col_arr).astype(int)
        i2 = i1 + 1
        j1 = np.floor(row_arr).astype(int)
        j2 = j1 + 1

        in_bounds = np.where((i1 >= 0) &
                             (i2 < num_cols) &
                             (j1 >= 0) &
                             (j2 < num_rows))

    else:

        i = np.asarray(np.round(col_arr).astype(int))
        j = np.asarray(np.round(row_arr).astype(int))

        in_bounds = np.where((i >= 0) &
                             (i < num_cols) &
                             (j >= 0) &
                             (j < num_rows))

    # print(row_is_scalar, col_is_scalar)
    # foo = np.ma.MaskedArray.squeeze(out)
    # print(type(foo))
    # print(foo)
    # print(foo.shape)

    count = len(in_bounds[0])
    if count == 0:
        if row_is_scalar and col_is_scalar:
            print('WARNING: Point is out of bounds.')
            # return(np.ma.MaskedArray.squeeze(out))
            return None
        else:
            print('WARNING: All grid row and col values are out of bounds.',
                  file=sys.stderr)
            return(out)

    if measure_wall_times is True:
        time_finish = time.time()
        print('INFO: Calculated in-bounds subset of input data for ' +
              '{} sampling '.format(method) +
              'in {} seconds.'.format(time_finish - time_start))
        time_start = time.time()

    if method == 'bilinear':

        gll = grid[j1[in_bounds], i1[in_bounds]]
        glr = grid[j1[in_bounds], i2[in_bounds]]
        gur = grid[j2[in_bounds], i2[in_bounds]]
        gul = grid[j2[in_bounds], i1[in_bounds]]

        # If any of gll, glr, gur, gul for any of [in_bounds] is
        # fill_value the corresonding values of out must be set to
        # fill_value .

        di_left = col[in_bounds] - i1[in_bounds]
        di_right = i2[in_bounds] - col[in_bounds]
        dj_bot = row[in_bounds] - j1[in_bounds]
        dj_top = j2[in_bounds] - row[in_bounds]

        out[in_bounds] = gll * di_right * dj_top + \
                         glr * di_left * dj_top + \
                         gur * di_left * dj_bot + \
                         gul * di_right * dj_bot

    else:

        out[in_bounds] = grid[j[in_bounds], i[in_bounds]]

    if measure_wall_times is True:
        time_finish = time.time()
        print('INFO: Performed {} sampling '.format(method) +
              'in {} seconds.'.format(time_finish - time_start))
        full_time_finish = time_finish
        print('INFO: Full sample_grid_at_points time ' +
              '{} seconds.'.format(full_time_finish - full_time_start))

    if row_is_scalar and col_is_scalar:
        return(out.item())
    return(out)


def at_loc(clim_dir,
           datetime,
           longitude,
           latitude,
           element='snow_depth',
           metric='median',
           sampling='neighbor'):
    """
    Retrieve SNODAS climatology at longitude/latitude location/s.

    Available elements: "snow_depth" (default), "swe"
    Available metrics: "median" (default), "iqr", "maximum"
    Available sampling methods: "neighbor" (default), "bilinear"
    """

    # Handle scalars or arrays.
    # lon_is_scalar = False
    if np.isscalar(longitude):
        lon_arr = np.asarray(longitude)[None]
        # lon_is_scalar = True
    else:
        lon_arr = np.asarray(longitude)

    # lat_is_scalar = False
    if np.isscalar(latitude):
        lat_arr = np.asarray(latitude)[None]
        # lat_is_scalar = True
    else:
        lat_arr = np.asarray(latitude)

    # lon = np.asarray(longitude)
    # lon_is_scalar = False
    # if lon.ndim == 0:
    #     lon = lon[None]
    #     lon_is_scalar = True

    # lat = np.asarray(latitude)
    # lat_is_scalar = False
    # if lat.ndim == 0:
    #     lat = lat[None]
    #     lat_is_scalar = True

    # lon_in, lon_is_float, lon_is_list, lon_is_tuple = \
    #     _copytobuffer(longitude)
    # lat_in, lat_is_float, lat_is_list, lat_is_tuple = \
    #     _copytobuffer(latitude)

    num_locs = lon_arr.size
    if num_locs == 0:
        print('ERROR: no locations given.',
              sys.stderr)
        return None
    if lat_arr.size != num_locs:
        print('ERROR: longitude and latitude arrays must have the ' +
              'same size.',
              file=sys.stderr)
        return None

    if not(os.path.isdir(clim_dir)):
        print('ERROR: {} directory not found',
              file=sys.stderr)
        return None

    date_mmdd = dt.datetime.strftime(datetime, '%m%d')

    clim_file = 'SNODAS_clim_{}_{}_{}.tif'. \
                format(element,
                       metric,
                       dt.datetime.strftime(datetime, '%m%d'))
    if not(os.path.exists(os.path.join(clim_dir,
                                       clim_file))):
        print('ERROR: file {} not found.'.
              format(os.path.join(clim_dir, clim_file)))
        return None

    # Read the climatology as a GDAL dataset.
    clim = gdal.Open(os.path.join(clim_dir, clim_file))

    # Get the geographic transform for the dataset. This is the "pixel
    # index to Cartesian" transform within the projected coordinate
    # system of the data. The components are:
    #   [0] Upper left x (edge) coordinate in projection coord. system
    #   [1] X resolution
    #   [2] 0.0
    #   [3] Upper left y (edge) coordinate in projection coord. system
    #   [4] 0.0
    #   [5] Y resolution (negative for north-up)
    clim_GeoTransform = clim.GetGeoTransform()

    # Transforming between longitude/latitude points and a geographic
    # grid is trivial, but this method should be applicable to other
    # projections as well.
    proj_geo = pyproj.Proj('epsg:4326')
    proj_clim = clim.GetProjection()

    # Calculate x/y values for longitude/latitude points.
    # x, y = pyproj.transform(proj_geo, proj_clim,
    #                         longitude, latitude)

    # pyproj.transform renders the whole scalar vs. array question
    # moot, because if lon_arr and lat_arr are one-element arrays, x and y
    # returned here will be scalars.
    x, y = pyproj.transform(proj_geo, proj_clim,
                            lon_arr, lat_arr)

    # x = _convertback(lon_is_float, lon_is_list, lon_is_tuple, x)
    # y = _convertback(lat_is_float, lat_is_list, lat_is_tuple, y)

    # Convert x/y values to col/row using the GeoTransform for the
    # dataset.
    x_res = clim.GetGeoTransform()[1]
    y_res = clim.GetGeoTransform()[5]
    x_corner_ctr = clim.GetGeoTransform()[0] + 0.5 * x_res
    y_corner_ctr = clim.GetGeoTransform()[3] + 0.5 * y_res
    col = (x - x_corner_ctr) / x_res
    row = (y - y_corner_ctr) / y_res

    #col = (x - x_corner_ctr) / x_res
    #row = (y - y_corner_ctr) / y_res

    # Sample the grid.
    clim_grid = clim.GetRasterBand(1).ReadAsArray()
    ndv = clim.GetRasterBand(1).GetNoDataValue()

    val = sample_grid_at_points(clim_grid, row, col,
                                fill_value=ndv,
                                method=sampling)

    if np.isscalar(val) or val is None:
        if val == ndv:
            return None
        else:
            return val
    else:
        val = np.ma.masked_where(val == ndv, val)
        return val


def main():

    clim_dir = '/net/lfs0data5/SNODAS_climatology/snow_depth'
    datetime = dt.datetime.strptime('2019021512', '%Y%m%d%H')

    lon = [-155.0, -100.0, -105.0, -110.19166666666667]
    lat = [42.0, 43.0, 44.0, 45.0]
    val = at_loc(clim_dir,
                 datetime,
                 lon,
                 lat,
                 element='snow_depth',
                 metric='median',
                 sampling='neighbor')
    print(type(val))
    print(val)
    print('xx')

    lon = [-155.0, 100.0, 105.0, 110.19166666666667]
    lat = [42.0, 43.0, 44.0, 45.0]
    val = at_loc(clim_dir,
                 datetime,
                 lon,
                 lat,
                 element='snow_depth',
                 metric='median',
                 sampling='neighbor')
    print(type(val))
    print(val)
    print('xx')

    lon = -155.0
    lat = 42.0
    val = at_loc(clim_dir,
                 datetime,
                 lon,
                 lat,
                 element='snow_depth',
                 metric='median',
                 sampling='neighbor')
    print(type(val))
    print(val)
    print('xx')


    lon = -105.0
    lat = 44.3
    val = at_loc(clim_dir,
                 datetime,
                 lon,
                 lat,
                 element='snow_depth',
                 metric='median',
                 sampling='neighbor')
    print(type(val))
    print(val)
    print('xx')


    # TENQ2
    print('TENQ2:')
    datetime = dt.datetime.strptime('2019120100', '%Y%m%d%H')
    lon = -122.93
    lat = 50.536
    val = at_loc(clim_dir,
                 datetime,
                 lon,
                 lat,
                 element='snow_depth',
                 metric='median',
                 sampling='neighbor')
    print(type(val))
    print(val)
    print('xx')







if __name__ == '__main__':
    main()
