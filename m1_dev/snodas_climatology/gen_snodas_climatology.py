#!/usr/bin/python3.6

import datetime as dt
import calendar
import os
import errno
import gzip
import numpy as np
import sys
from osgeo import gdal,osr,gdalconst
import cartopy.crs as ccrs
import matplotlib as mpl
import matplotlib.colors as mplcol
import matplotlib.pyplot as mplplt
import math
import cartopy.feature
from cartopy.feature import NaturalEarthFeature as cfNEF
from cartopy.feature import LAND, COASTLINE
import argparse


def zvalue_from_index(arr, ind):
    """
    Helper function from
    https://krstn.eu/np.nanpercentile()-there-has-to-be-a-faster-way
    See also
    https://stackoverflow.com/questions/2374640
    arr has to be a 3D array (num_z, num_rows, num_cols)
    ind has to be a 2D array (num_rows, num_cols)
    """
    # Get number of rows and columns.
    _,num_rows,num_cols = arr.shape

    # Get linear indices.
    idx = num_rows * num_cols * ind + \
        np.arange(num_rows*num_cols).reshape((num_rows,num_cols))

    # Extract elements with np.take().
    return np.take(arr, idx)


def ma_quantile(arr, quantile, ndv):
    """
    A faster version of numpy.nanquantile from
    https://krstn.eu/np.nanpercentile()-there-has-to-be-a-faster-way
    modified to work with masked arrays.
    arr has to be a 3D array (num_z, num_rows, num_cols)
    """
    # Count valid (non-masked) values along the first axis.
    num_valid = np.sum(np.invert(arr.mask), axis=0)

    # Identify locations where there are no valid data.
    no_valid = num_valid == 0

    # Replace masked values with the maximum of the flattened array.
    arr_copy = np.copy(np.ma.getdata(arr))
    arr_copy[arr.mask] = np.amax(arr)

    # Sort values along the z axis. Formerly masked values will be at the
    # end.
    arr_copy = np.sort(arr_copy, axis=0)

    # Loop over requested quantiles.
    if type(quantile) is list:
        quantiles = []
        quantiles.extend(quantile)
    else:
        quantiles = [quantile]

    # if len(quantiles) < 2:
    #     quant_arr = np.zeros(shape=(arr.shape[1], arr.shape[2]))
    # else:
    #     quant_arr = np.zeros(shape=(len(quantiles),
    #                                 arr.shape[1], arr.shape[2]))
    # quant_arr = np.ma.masked_where(quant_arr == 0.0, quant_arr)

    result = []
    # print('>>')
    for i in range(len(quantiles)):

        quant = quantiles[i]

        # Desired (floating point) position for each row/column as well
        # as floor and ceiling of it.
        k_arr = (num_valid - 1) * quant
        f_arr = np.floor(k_arr).astype(np.int32)
        c_arr = np.ceil(k_arr).astype(np.int32)

        # Identify locations where the desired quantile hit exactly.
        fc_equal_k_mask = f_arr == c_arr

        # Interpolate.
        floor_val = zvalue_from_index(arr=arr_copy, ind=f_arr) * (c_arr - k_arr)
        ceil_val = zvalue_from_index(arr=arr_copy, ind=c_arr) * (k_arr - f_arr)

        quant_arr = floor_val + ceil_val
        quant_arr[fc_equal_k_mask] = \
            zvalue_from_index(arr=arr_copy,
                              ind=k_arr.astype(np.int32))[fc_equal_k_mask]

        # Re-mask locations where there are no valid data.
        quant_arr[no_valid] = ndv
        quant_arr = np.ma.masked_where(quant_arr == ndv, quant_arr)

        result.append(quant_arr)
    #     print(quant_arr[0,0])
    #     print(result[i][0,0])
    #     print(np.ma.getdata(result[i])[0,0])

    # print('<<')
    return result


def nan_quantile(arr, quantile):
    """
    A faster version of numpy.nanquantile from
    https://krstn.eu/np.nanpercentile()-there-has-to-be-a-faster-way
    arr has to be a 3D array (num_z, num_rows, num_cols)
    """

    # Count valid (non-NaN) values along the first axis.
    num_valid = np.sum(np.isfinite(arr), axis=0)

    # Identify locations where there are no non-nan data.
    no_valid = num_valid == 0

    # Replace np.nan with the maximum of the flattened array.
    arr[np.isnan(arr)] = np.nanmax(arr)

    # Sort values. Former np.nan values will be at the end.
    arr = np.sort(arr, axis=0)

    # Loop over requested quantiles.
    if type(quantile) is list:
        quantiles = []
        quantiles.extend(quantile)
    else:
        quantiles = [quantile]

    # if len(quantiles) < 2:
    #     quant_arr = np.zeros(shape=(arr.shape[1], arr.shape[2]))
    # else:
    #     quant_arr = np.zeros(shape=(len(quantiles),
    #                                 arr.shape[1], arr.shape[2]))

    result = []
    for i in range(len(quantiles)):

        quant = quantiles[i]

        # Desired (floating point) position for each row/column as well
        # as floor and ceiling of it.
        k_arr = (num_valid - 1) * quant
        f_arr = np.floor(k_arr).astype(np.int32)
        c_arr = np.ceil(k_arr).astype(np.int32)

        # Identify locations where the desired quantile hit exactly.
        fc_equal_k_mask = f_arr == c_arr

        # Interpolate.
        floor_val = zvalue_from_index(arr=arr, ind=f_arr) * (c_arr - k_arr)
        ceil_val = zvalue_from_index(arr=arr, ind=c_arr) * (k_arr - f_arr)

        quant_arr = floor_val + ceil_val
        quant_arr[fc_equal_k_mask] = \
            zvalue_from_index(arr=arr,
                              ind=k_arr.astype(np.int32))[fc_equal_k_mask]
        quant_arr[no_valid] = np.nan

        result.append(quant_arr)

    return result


# class GeoRasterDS:
#     """
#     Geographic (lon/lat) raster dataset structure.
#     """
#     def __init__(self,
#                  gdal_ds,  # GDAL dataset
#                  ccrs,     # cartopy CRS
#                  x_ll_ctr, # x min (center) of raster in CRS (i.e., native)
#                  nx,       # x grid size (number of columns)
#                  dx,       # x grid spacing
#                  y_ll_ctr, # y min (center) of raster in CRS (i.e., native)
#                  ny,       # y grid size (number of rows)
#                  dy):      # y grid spacing
#         self.gdal_ds = gdal_ds
#         self.ccrs = ccrs
#         self.x_ll_ctr = x_ll_ctr
#         self.nx = nx
#         self.dx = dx
#         self.y_ll_ctr = y_ll_ctr
#         self.ny = ny
#         self.dy = dy


def GF_rcParams():
    """
    Set rcParams for matplotlib.
    """

    # Maybe this should be more of a stylesheet kind of thing. Will look
    # into it later.

    # Make colorbar tick labels smaller.
    #mpl.rcParams['xtick.labelsize']='small'
    mpl.rcParams['ytick.labelsize']='small'

    # Set the typeface for plots.
    # My favorite monospace
    #mpl.rcParams['font.family'] = 'monospace'
    #mpl.rcParams['font.monospace'] = 'Inconsolata'
    # My favorite sans
    # Use this to get a list of TrueType fonts on the system.
    #>>> import matplotlib.font_manager
    #>>> matplotlib.font_manager.findSystemFonts(fontpaths=None, fontext='ttf')
    mpl.rcParams['font.family'] = 'sans-serif'
    # mpl.rcParams['font.sans-serif'] = 'Calibri'

    # Eliminate the toolbar in plot windows.
    #mpl.rcParams['toolbar'] = 'None'

    # Some colors.
    mpl.rcParams['figure.facecolor'] = '#e8f2ffff'
    mpl.rcParams['text.color'] = 'k'
    mpl.rcParams['ytick.color'] = 'k'

    # Default margins for subplots.
    mpl.rcParams['figure.subplot.left'] = 0.025
    mpl.rcParams['figure.subplot.bottom'] = 0.025
    mpl.rcParams['figure.subplot.right'] = 0.975
    mpl.rcParams['figure.subplot.top'] = 0.900


def snow_colormap():
    """
    Define a color ramp to display snow water equivalent in mm or
    snow depth in cm.
    """
    # Define colors for snow depth.
    red = np.array([204, 163, 122, 83, 78, 86, 149, 207,
                    229, 245, 255, 255],
                   dtype=np.float64) / 255.0
    grn = np.array([249, 214, 157, 89, 48, 20, 36, 69,
                    106, 147, 187, 226],
                   dtype=np.float64) / 255.0
    blu = np.array([255, 245, 229, 207, 182, 153, 182, 199,
                    188, 185, 194, 221],
                   dtype=np.float64) / 255.0
    rgb = np.transpose(np.array([red,grn,blu]))

    colormap = mplcol.ListedColormap(rgb, name='SnowDepth')

    colormap.set_under('xkcd:light grey')
    colormap.set_over('xkcd:ocean blue')

    # Set color levels for mm of SWE or cm of depth.
    col_levels = [0.0, 1.0, 5.0, 10.0, 25.0, 50.0,
                  100.0, 150.0, 250.0, 500.0, 750.0,
                  1000.0, 2000.0]

    tick_levels = col_levels

    # Label color levels.
    tick_labels = ['0', '1', '5', '10', '25', '50', '100',
                   '150', '250', '500', '750', '1000', '2000']
    norm = mplcol.BoundaryNorm(col_levels, 12)

    snow_color_ramp = {'colormap': colormap,
                       'col_levels': col_levels,
                       'tick_levels': tick_levels,
                       'tick_labels': tick_labels,
                       'norm': norm,
                       'extend': 'both'}

    return snow_color_ramp


def geo_grid_map(crs, figure_x_size, figure_y_size, num, bbox,
                 x_ctr_data, y_ctr_data, dataset, raster_band_index, ndv,
                 title, color_ramp, color_ramp_label):
    """
    Generate a geographic map of single band from a GDAL raster dataset.

    crs: The cartopy.crs object describing the coordinate system.
    figure_x_size: The figure x size in inches.
    figure_y_size: The figure y size in inches.
    num: The num argument that will be provided to matplotlib.pyplot.subplots
         (and thereby provided to matplotlib.pyplot.figure) identifying the
         figure.
    bbox: The bounding box of the raster, in the coordinate system of the
          data.
    x_ctr_data: The list of x axis locations for the data (i.e. the center
                coordinates of each column).
    y_ctr_data: The list of y axis locations for the data (i.e. the center
                coordinates of each row). This should apply to the data in a
                north-up orientation, even though the grids in the GDAL
                dataset are generally oriented north-down.
    dataset: The GDAL raster dataset.
    raster_band_index: The raster band to show, indexed from 1.
    ndv: The no-data value.
    title: A string for the plot title.
    color_ramp: a dictionary describing the color ramp to use for the data:
                {'colormap': matplotlib.colors colormap object
                 'col_levels': colorbar axis levels bounding each color
                 'tick_levels': colorbar tick levels
                 'tick_labels': colorbar tick labels
                 'norm': colormap index from a call to
                         matplotlib.colors.BoundaryNorm
                 'extend': 'min', 'max', 'both', or 'neither'}
    color_ramp_label: Typically the units of the values on the colorbar
                      axis.
    """
    fig, ax = mplplt.subplots(subplot_kw=dict(projection=crs),
                              figsize=(figure_x_size, figure_y_size),
                              num=num,
                              clear=True)

    ax.set_extent(bbox, crs=crs)

    # If uncommented, tight_layout overrides rcParams elements (namely left,
    # bottom, right and top margin values) for a figure.
    #fig.tight_layout()

    # Extract the requested raster from the dataset.
    grid = dataset.GetRasterBand(raster_band_index).ReadAsArray()

    # Convert the grid to a masked array.
    grid_masked = np.ma.masked_equal(grid, ndv)

    # Draw the grid.
    cf = ax.contourf(x_ctr_data, y_ctr_data,
                     np.flipud(grid_masked),
                     color_ramp['col_levels'],
                     cmap=color_ramp['colormap'],
                     norm=color_ramp['norm'],
                     extend=color_ramp['extend'],
                     transform=crs)
 
    ax.set_title(title)
    cbar = fig.colorbar(cf,
                        orientation='vertical',
                        fraction=0.125,
                        shrink=0.7)
    cbar.set_ticks(color_ramp['tick_levels'])
    cbar.set_ticklabels(color_ramp['tick_labels'])
    cbar.ax.yaxis.set_tick_params(length=4)
    cbar.set_label(color_ramp_label, fontsize='medium',
                      color=mpl.rcParams['ytick.color'])

    # Draw U.S. states and national boundaries.
    # *** NEITHER OF THESE WORK WITH CARTOPY 0.13 / PYTHON 3.6 ***
    # ax.add_feature(cfNEF(category='cultural',
    #                      name='admin_1_states_provinces_lakes',
    #                      scale='50m',
    #                      edgecolor='gray',
    #                      facecolor='none',
    #                      linewidth=0.4))
    # ax.add_feature(cfNEF(category='cultural',
    #                      name='admin_0_countries_lakes',
    #                      scale='50m',
    #                      edgecolor='black',
    #                      facecolor='none',
    #                      linewidth=0.4))

    return(fig, ax)


def read_nsidc_arch_snow(archive_dir,
                         scratch_dir,
                         date_yyyymmdd,
                         product_group=1034,
                         unmasked=False):
    """
    Read snow depth (in mm) from a local copy of the NSIDC SNODAS
    archives.
    """

    # Verify input directory exists.
    if not os.path.isdir(archive_dir):
        raise FileNotFoundError(errno.ENOENT,
                                os.strerror(errno.ENOENT),
                                archive_dir)
        return None, None

    # Verify scratch directory exists.
    if not os.path.isdir(scratch_dir):
        raise FileNotFoundError(errno.ENOENT,
                                os.strerror(errno.ENOENT),
                                scratch_dir)
        return None, None

    if not unmasked:
        domain = 'masked'
        domain_file = 'us'
    else:
        domain = 'unmasked'
        domain_file = 'zz'

    #DEPTH
    #product_group = 1036 # snow depth

    file_dir = os.path.join(archive_dir,
                            domain,
                            '{}'.format(product_group),
                            date_yyyymmdd[0:4],
                            date_yyyymmdd[4:6])

    # Verify file directory exists.
    if not os.path.isdir(file_dir):
        raise FileNotFoundError(errno.ENOENT,
                                os.strerror(errno.ENOENT),
                                file_dir)
        return None, None

    if int(date_yyyymmdd) < 20161001:
        hdr_ext = 'Hdr'
    else:
        hdr_ext = 'txt'

    hdr_file = '{}_ssmv1{}tS__T0001TTNATS{}05HP001.{}.gz'. \
               format(domain_file,
                      product_group,
                      date_yyyymmdd,
                      hdr_ext)

    if not os.path.exists(os.path.join(file_dir,hdr_file)):
        print('No header found for {}'.format(date_yyyymmdd))
        return None, None

    # Read the GISRS raster header. ALL fields are returned as strings.
    gisrs_hdr = {}
    with gzip.open(os.path.join(file_dir,hdr_file), 
                   mode='rt') as nsidc_hdr:
        for line in nsidc_hdr:
            gisrs_hdr[line.split(':')[0]] = \
                line.split(':')[1].rstrip('\n').strip()
        #     print('"{}" = "{}"'.
        #           format(line.split(':')[0],
        #                  gisrs_hdr[line.split(':')[0]]))
        # data_units = gisrs_hdr['Data units']
        # data_slope = gisrs_hdr['Data slope']
        # data_intercept = gisrs_hdr['Data intercept']

    # Data units are always in mm.
    if not gisrs_hdr['Data units'].startswith('Meters / 1000.000'):
        print('Unsupported units "{}" in {}.'.
              format(gisrs_hdr['Data units'],
                     os.path.join(file_dir,hdr_file)),
              file=sys.stderr)
        return None, None

    if gisrs_hdr['Data type'] != 'integer':
        print('Unsupported data type "{}" in {}.'.
              format(gisrs_hdr['Data type'],
                     os.path.join(file_dir,hdr_file)),
              file=sys.stderr)
        return None, None

    if gisrs_hdr['Data bytes per pixel'] != '2':
        print('Unsupported bytes per pixel of "{}" in {}.'.
              format(gisrs_hdr['Data bytes per pixel'],
                     os.path.join(file_dir,hdr_file)),
              file=sys.stderr)
        return None, None

    dat_file = '{}_ssmv1{}tS__T0001TTNATS{}05HP001.dat.gz'. \
               format(domain_file,
                      product_group,
                      date_yyyymmdd)

    if not os.path.exists(os.path.join(file_dir,dat_file)):
        print('No data file found for {}'.format(date_yyyymmdd))
        return None, None

    # To read the binary grid we need to use the NumPy frombuffer
    # method, and must remember to flip the bytes on little endian
    # systems (such as Linux) because data are stored as big endian in
    # the SNODAS archives.
    with gzip.open(os.path.join(file_dir,dat_file),
                  mode='rb') as dat_file:
        dt = np.dtype('int16')
        # Data are stored as big endian in SNODAS archives. Linux is
        # little endian, so generally those bytes need to get swapped.
        if sys.byteorder == 'little':
            dt = dt.newbyteorder('>')
        grid = np.frombuffer(dat_file.read(), dtype=dt)
        grid = grid.reshape(int(gisrs_hdr['Number of rows']),
                            int(gisrs_hdr['Number of columns']))

    return gisrs_hdr, grid


def parse_args():

    """
    Parse command line arguments.
    """

    help_message = 'Generate a climatology of SNODAS snowpack states.'

    parser = argparse.ArgumentParser(description=help_message)

    parser.add_argument('-s', '--start_year',
                        type=int,
                        metavar='start water year',
                        nargs='?')
    parser.add_argument('-f', '--finish_year',
                        type=int,
                        metavar='finish water year',
                        nargs='?')
    parser.add_argument('-d', '--depth',
                        action='store_true',
                        help='Generate snow depth climatology ' +
                             '(SWE is the default).')
    parser.add_argument('-p', '--plot_results',
                        action='store_true',
                        help='Display plot of climatology for each day.')
    args = parser.parse_args()

    if not args.start_year:
        args.start_year = 2005
        print('No start year given. Using default of {}.'.
              format(args.start_year))
    if not args.finish_year:
        args.finish_year = 2019
        print('No finish year given. Using default of {}.'.
              format(args.finish_year))
    return args


def main():
    """
    Using daily archives of SNODAS snow water equivalent and snow depth
    available at NSIDC, generate a gridded climatology of those variables.
    """

    opt = parse_args()

    if opt.plot_results:
        # Prepare for plotting.
        mplplt.close('all')
        GF_rcParams()

    archive_dir = '/net/lfs0data5/NSIDC_archive'
    scratch_dir = '/net/scratch/{}'.format(os.getlogin())

    # opt.start_year and opt.finish_year are the END of the water
    # years. For example, if  opt.start_year = 2005, then the first year
    # of the climatology covers October 2004 - September 2005.
    clim_num_years = opt.finish_year - opt.start_year + 1

    repair_ds = None

    # Generate SNODAS climatology for a hypothetical leap year.
    for day_of_water_year in range(1, 367):
        # Generate climatology for current day_of_water_year.
        print('Day of water year {}.'.format(day_of_water_year))
        # Skip ahead to December 18.
        # if day_of_water_year < 79:
        # Skip ahead to June 11.
        # if day_of_water_year < 255:
        #   continue
        layers = []

        # Looping backward means that later years will establish
        # coordinates for the climatology. We do not want the outputs to
        # be anchored to the pre-shift (which occurred on 2016-10-01?)
        # coordinates.
        date_mmdd = None
        lon_lat_ds = None
        for year in range(opt.finish_year, opt.start_year - 1, -1):
            start_of_water_year = '{}1001'.format(year-1)
            start_of_water_year_datetime = \
              dt.datetime.strptime(start_of_water_year, '%Y%m%d')

            # Calculate the datetime associated with the current
            # day_of_water_year.
            if day_of_water_year < 152:
                # For dates up to and including Feburary 28, calculating
                # date_mmdd is simple.
                dowy_datetime = start_of_water_year_datetime + \
                                dt.timedelta(days=day_of_water_year-1)
                if date_mmdd is None:
                    date_mmdd = dowy_datetime.strftime('%m%d')
            else:
                if calendar.isleap(year):
                    # Calculating date_mmdd is unchanged for leap years.
                    dowy_datetime = start_of_water_year_datetime + \
                                    dt.timedelta(days=day_of_water_year-1)
                    if date_mmdd is None:
                        date_mmdd = dowy_datetime.strftime('%m%d')
                else:
                    # Subtract an extra day for non-leap years, which
                    # means that February 28 stands in for leap day when
                    # day_of_water_year = 152 (out of 366) and year is
                    # not a leap year.
                    dowy_datetime = start_of_water_year_datetime + \
                                    dt.timedelta(days=day_of_water_year-2)

            print('Reading data for {}.'.
                  format(dowy_datetime.strftime('%Y%m%d')))

            if (day_of_water_year == 152) and (not calendar.isleap(year)):
                dowy_datetime = start_of_water_year_datetime + \
                                dt.timedelta(days=day_of_water_year-1)
                print('  leap day in non-leap year: read data for {}.'.
                      format(dowy_datetime.strftime('%Y%m%d')))

            # Read "masked" SNODAS data.
            if opt.depth:
                product_group = 1036
                product_name = 'snow depth'
                product_file_string = 'snow_depth'
                product_title = 'Snow Depth'
                display_units = 'cm'
            else:
                product_group = 1034
                product_name = 'snow water equivalent'
                product_file_string = 'swe'
                product_title = 'SWE'
                display_units = 'mm'

            snow_hdr, snow_grid = \
                read_nsidc_arch_snow(archive_dir,
                                     scratch_dir,
                                     dowy_datetime.strftime('%Y%m%d'),
                                     product_group)
            if snow_grid is None:
                continue

            # Get grid geometry.
            this_num_rows = \
                np.int32(np.float64(snow_hdr['Number of rows']))
            this_num_cols = \
                np.int32(np.float64(snow_hdr['Number of columns']))
            this_min_lon = \
                np.float64(snow_hdr['Minimum x-axis coordinate'])
            this_max_lon = \
                np.float64(snow_hdr['Maximum x-axis coordinate'])
            this_min_lat = \
                np.float64(snow_hdr['Minimum y-axis coordinate'])
            this_max_lat = \
                np.float64(snow_hdr['Maximum y-axis coordinate'])
            this_lon_res = np.float64(snow_hdr['X-axis resolution'])
            this_lat_res = np.float64(snow_hdr['Y-axis resolution'])

            # Convert the snow_grid from a specifically big endian
            # integer to an ordinary integer for this system.
            snow_grid = snow_grid.astype(np.int16)

            # Convert the snow_grid to floating point.
            snow_grid = snow_grid.astype(np.float32)

            # Record the (floating point) no-data value.
            ndv = np.float32(snow_hdr['No data value'])

            # Convert the grid to a masked array.
            snow_grid = np.ma.masked_equal(snow_grid, ndv)
            # print(type(snow_grid))
            # print(snow_grid.data.dtype)

            # If this is the first snow_grid read for this date,
            # define the grid geometry, both "out" (output) and "ref"
            # (reference).
            if len(layers) == 0:
                num_rows_out = this_num_rows
                num_cols_out = this_num_cols
                min_lon_out = this_min_lon
                max_lon_out = this_max_lon
                min_lat_out = this_min_lat
                max_lat_out = this_max_lat
                lon_res_out = this_lon_res
                lat_res_out = this_lat_res
                num_rows_ref = this_num_rows
                num_cols_ref = this_num_cols
                min_lon_ref = this_min_lon
                max_lon_ref = this_max_lon
                min_lat_ref = this_min_lat
                max_lat_ref = this_max_lat
                lon_res_ref = this_lon_res
                lat_res_ref = this_lat_res

            # Verify grid shape matches grid geometry definition from the
            # raster header. Assume that if grid.shape is correct, then
            # we can use the other geometry parameters without a problem.
            if this_num_rows != snow_grid.shape[0]:
                print('ERROR: grid # rows mismatch in masked ("us") ' +
                      '{} for '.format(product_name) +
                      dowy_datetime.strftime('%Y-%m-%d') + '.',
                      file=sys.stderr)
                sys.exit(1)
            if this_num_cols != snow_grid.shape[1]:
                print('ERROR: grid # columns mismatch in masked ("us") ' +
                      '{} for '.format(product_name) +
                      dowy_datetime.strftime('%Y-%m-%d') + '.',
                      file=sys.stderr)
                sys.exit(1)

            # Verify grid shape against output geometry.
            if snow_grid.shape[0] != num_rows_out:
                print('ERROR: grid # rows inconsistency in masked ("us") ' +
                      '{} for '.format(product_name) +
                      dowy_datetime.strftime('%Y-%m-%d') + '.',
                      file=sys.stderr)
                sys.exit(1)
            if snow_grid.shape[1] != num_cols_out:
                print('ERROR: grid # columns inconsistency in masked ("us") ' +
                      '{} for '.format(product_name) +
                      dowy_datetime.strftime('%Y-%m-%d') + '.',
                      file=sys.stderr)
                sys.exit(1)

            # Make sure grid geometry does not differ significantly from
            # output geometry. We will tolerate differences of up to
            # 0.001 degrees, which is 3.6 arc sec--around 100
            # meters. This exercise is purely academic since no such
            # shift has ever happened, but it pays to be careful.
            shift = max(abs(this_min_lon - min_lon_out),
                        abs(this_max_lon - max_lon_out),
                        abs(this_min_lat - min_lat_out),
                        abs(this_max_lat - max_lat_out))
            if shift > 0.001:
                print('ERROR: unacceptably large coordinate shift at {}.'.
                      format(dowy_datetime.strftime('%Y%m%d')),
                      file=sys.stderr)
                sys.exit(1)

            # Give a notice if there is any significant change in
            # geometry. Since we are converting strings that were
            # generated from floats back into floats--and also because we
            # performed an intentional shift of the SNODAS grid in
            # 2012--this is expected, and not a problem, but is worth
            # noting when it occurs. The threshold for this check is
            # 1.0e-5 degrees--about 1 meter.
            shift = max(abs(this_min_lon - min_lon_ref),
                        abs(this_max_lon - max_lon_ref),
                        abs(this_min_lat - min_lat_ref),
                        abs(this_max_lat - max_lat_ref))
            if shift > 1.0e-5:
                print('NOTICE: minor coordinate shift at {}.'.
                      format(dowy_datetime.strftime('%Y%m%d')))
                num_rows_ref = this_num_rows
                num_cols_ref = this_num_cols
                min_lon_ref = this_min_lon
                max_lon_ref = this_max_lon
                min_lat_ref = this_min_lat
                max_lat_ref = this_max_lat
                lon_res_ref = this_lon_res
                lat_res_ref = this_lat_res

            if lon_lat_ds is None:

                # Generate a general purpose GDAL dataset for generating
                # graphics (including output GeoTIFF file).
                mem_driver = gdal.GetDriverByName('MEM')

                np_dtype_name = snow_grid.dtype.name
                gdal_data_type_num = gdal.GetDataTypeByName(np_dtype_name)
                gdal_data_type_name = gdal.GetDataTypeName(gdal_data_type_num)

                # Create a generic dataset that any grid for this
                # climatology can be dropped into.
                lon_lat_ds = mem_driver.Create('SNODAS climatology',
                                               xsize=snow_grid.shape[1],
                                               ysize=snow_grid.shape[0],
                                               bands=1,
                                               eType=eval('gdal.GDT_' +
                                                          gdal_data_type_name))
                # Define the "projection".
                srs = osr.SpatialReference()
                srs.ImportFromEPSG(4326)
                lon_lat_ds.SetProjection(srs.ExportToWkt())

                # Define the GeoTransform.
                lon_lat_ds.SetGeoTransform((min_lon_out, lon_res_out, 0.0,
                                            max_lat_out, 0.0, -lat_res_out))

                # Write snow_grid to GDAL dataset.
                if opt.depth:
                    # Convert snow depth to cm in the GDAL dataset so we
                    # can use the same color ramp for plots SWE and snow
                    # depth.
                    snow_grid_display = np.ma.copy(snow_grid) / 10.0
                    # Do not need to to this:
                    #snow_grid_display[snow_grid_display.mask == True] = ndv
                else:
                    snow_grid_display = np.ma.copy(snow_grid)

                lon_lat_ds.GetRasterBand(1).WriteArray(snow_grid_display)

                # Define variables needed for plotting.
                lon_lat_crs = ccrs.PlateCarree()
                bbox = [min_lon_out, max_lon_out, min_lat_out, max_lat_out]
                aspect = (max_lon_out - min_lon_out) / \
                         (max_lat_out - min_lat_out)
                ll_ctr_lon = min_lon_out + 0.5 * lon_res_out
                ll_ctr_lat = min_lat_out + 0.5 * lat_res_out
                xsize = 12.0
                ysize = math.ceil(2.0 * xsize / aspect) / 2.0
                lon_axis = np.linspace(min_lon_out + 0.5 * lon_res_out,
                                       max_lon_out - 0.5 * lon_res_out,
                                       num_cols_out)
                lat_axis = np.linspace(min_lat_out + 0.5 * lat_res_out,
                                       max_lat_out - 0.5 * lat_res_out,
                                       num_rows_out)

            if (dowy_datetime >= 
                dt.datetime.strptime('2014-10-09', '%Y-%m-%d') and
                dowy_datetime <=
                dt.datetime.strptime('2019-10-10', '%Y-%m-%d')):

                # Mask values that were "persistent zeroes" in SNODAS
                # from 2014-10-09 to 2019-10-10.
                if repair_ds is None:
                    # Read the repair mask. Values of 1 indicate cells
                    # that should have SWE/depth values of zero changed
                    # to missing/no-data.
                    repair_mask = 'SNODAS_Repair_Mask_October_2019.tif'
                    if not os.path.exists(repair_mask):
                        print('Did not find {}.'.format(repair_mask),
                              file=sys.stderr)
                        sys.exit(1)
                    full_repair_ds = gdal.Open(repair_mask)

                    # Create repair_ds to match the lon_lat_ds
                    # grid/coordinate system.
                    repair_ds = \
                        mem_driver.Create('SNODAS repair mask',
                                          xsize=snow_grid.shape[1],
                                          ysize=snow_grid.shape[0],
                                          bands=1,
                                          eType=eval('gdal.GDT_' +
                                                     gdal_data_type_name))
                    # Define the "projection".
                    srs = osr.SpatialReference()
                    srs.ImportFromEPSG(4326)
                    repair_ds.SetProjection(srs.ExportToWkt())

                    # Define the GeoTransform.
                    repair_ds.SetGeoTransform((min_lon_out, lon_res_out, 0.0,
                                               max_lat_out, 0.0, -lat_res_out))

                    # "Reproject" full_repair_ds data to the repair_ds
                    # coordinate system. Since both full_repair_ds and
                    # repair_ds are lon/lat grids, this does not actually
                    # reproject, it just subsets the data grid to match
                    # the "masked" SNODAS domain.
                    gdal.ReprojectImage(full_repair_ds,
                                        repair_ds,
                                        full_repair_ds.GetProjection(),
                                        repair_ds.GetProjection(),
                                        gdalconst.GRA_NearestNeighbour)

                    # tiff_name = 'us_SNODAS_Repair_Mask_October_2019.tif'
                    # gtiff_driver = gdal.GetDriverByName('GTiff')
                    # gtiff_driver.CreateCopy('tiff_name',
                    #                         repair_ds,
                    #                         strict=False,
                    #                         options=["COMPRESS=LZW"])

                    # Extract the data.
                    repair_grid = \
                        repair_ds.GetRasterBand(1).ReadAsArray()
                    # Identify cells that need to be set to no-data
                    # values and masked.
                    to_mask = np.where(repair_grid == 1)

                # Confirm that all to_mask values in the current
                # snow_grid are zeroes.
                if np.max(snow_grid[to_mask]) > 0.0:
                    print('ERROR: nonzero data found where persistent ' +
                          'zero values are expected.',
                          file=sys.stderr)
                    sys.exit(1)

                # Set to_mask values in the current snow_grid to ndv.
                snow_grid[to_mask] = ndv

                # Mask all to_mask values.
                snow_grid = np.ma.masked_where(repair_grid == 1, snow_grid)

            # Add the current grid to the layers list, for stacking to
            # come afterward.
            layers.append(snow_grid)

        if date_mmdd is None:
            print('ERROR: data did not include a leap year; ' +
                  'check programming.',
                  file=sys.stderr)
            exit(1)
        if lon_lat_ds is None:
            print('ERROR: no dataset created; check programming',
                  file=sys.stderr)
            exit(1)

        # Convert layers list into a stack.
        print('Stacking grids.')
        t1 = dt.datetime.utcnow()
        layers = np.ma.stack(layers, axis=0)
        t2 = dt.datetime.utcnow()
        elapsed_time = t2 - t1
        print('elapsed: {} seconds'.format(elapsed_time.total_seconds()))

        # Print sample data.
        # print('Shape of stacked layers: {}'.format(layers.shape))
        # sample_row_north_down = [2206, 1277, 798, 1143, 1033, 866,
        #                          1276, 1425, 1294, 692, 1202]
        # sample_col = [1626, 1263, 1280, 1075, 1238, 1964,
        #               4647, 1141, 2027, 6798, 5834]
        # print(layers[:,sample_row_north_down,sample_col])
        # for zc, sc in enumerate(sample_col):
        #   pixel_clim_series = \
        #       layers[:,sample_row_north_down[zc],sample_col[zc]]
        #   print('sample data at row {}, col {}:'.
        #       format(sample_row_north_down[zc], sample_col[zc]))
        #   print(pixel_clim_series)
        #   print('val = [' + ','.join(map(str, pixel_clim_series)) + ']')
        #   print('median: {}'.format(np.ma.median(pixel_clim_series)))
        # sys.exit(0)

        # Calculate the number of years of good data for each grid cell.
        num_years = layers.count(axis=0)

        # Make sure that sd_median has the value ndv where it is masked.
        # The default value for the result of np.ma.median, and therefore
        # the value that masked cells will carry, is zero. If we leave it
        # that way, masked values will appear like zeroes if/when the
        # data are plotted using matplotlib. It is matplotlib, what are
        # you gonna do?
        #
        # Notes on the following approaches that give the same result:
        #
        #   no_years = num_years == 0
        #   sd_median[no_years] = ndv
        #
        # vs.
        #
        #   no_years = np.where(num_years == 0)
        #   sd_median[no_years] = ndv
        #
        # From the numpy.where documentation:
        #
        #   "Note: When only /condition/ is provided, this function is a
        #    shorthand for np.asarray(condition).nonzero(). Using nonzero
        #    directly should be preferred, as it behaves correctly for
        #    subclasses."

        # Generate quantile/s.
        print('Computing quantiles.')
        t1 = dt.datetime.utcnow()
        [sd_mq25, sd_mq50, sd_mq75, sd_mq100] = \
            ma_quantile(layers, [0.25, 0.50, 0.75, 1.0], ndv)
        t2 = dt.datetime.utcnow()
        elapsed_time = t2 - t1
        print('elapsed: {} seconds'.format(elapsed_time.total_seconds()))

        print('Computing maximum.')
        t1 = dt.datetime.utcnow()
        sd_max = np.ma.max(layers, axis=0, fill_value=ndv)
        # The fill_value above does not work as expected (setting masked
        # values to ndv) so manually set values of masked cells to ndv.
        sd_max.data[sd_max.mask == True] = ndv
        t2 = dt.datetime.utcnow()
        elapsed_time = t2 - t1
        print('elapsed: {} seconds'.format(elapsed_time.total_seconds()))

        # Expected grid dimensions are 3351 rows, 6935 columns.

        # Print results for one cell where there are more than seven, but
        # fewer than 15, valid snow values in the stack.
        # Identify cells that are imperfect but have enough data to
        # calculate a result.
        min_years_for_clim = math.ceil(clim_num_years / 2)
        ind = np.where((num_years >= min_years_for_clim) &
                       (num_years < clim_num_years))
        print('There are {} "imperfect" pixels, '.format(len(ind[0])) +
              'with {}-{} '.format(min_years_for_clim, clim_num_years - 1) +
              'years of data.')

        # Count masked cells (debugging; not required).
        # mask_ind = np.where(np.ma.getmaskarray(sd_median) == True)
        # print('# masked cells before: {}'.format(len(mask_ind[0])))

        # Mask cells where we have data for less than half the years of
        # the climatology. For odd years use the ceiling.
        ind = np.where((num_years > 0) & 
                       (num_years < min_years_for_clim))

        # rc = ind[0][0]
        # cc = ind[1][0]
        # print('values for row {}, col {}:'.format(rc, cc))
        # print(layers[:,rc,cc])
        # # print('median: {}'.format(sd_median[rc,cc]))
        # # print('25% quantile: {}'.format(sd_q25[rc,cc]))
        # print('25% masked quantile: {}'.format(sd_mq25[rc,cc]))
        # print(sd_mq25[rc,cc])
        # print(np.ma.getdata(sd_mq25)[rc,cc])
        # # print('50% quantile: {}'.format(sd_q50[rc,cc]))
        # print('50% masked quantile: {}'.format(sd_mq50[rc,cc]))
        # # print('75% quantile: {}'.format(sd_q75[rc,cc]))
        # print('75% masked quantile: {}'.format(sd_mq75[rc,cc]))
        # print('# years: {}'.format(num_years[rc,cc]))
        # print('masking {} '.format(len(ind[0])) +
        #       'cells having data for less than {} '.
        #       format(math.ceil(clim_num_years / 2)) +
        #       'of the {}-year period.'.format(clim_num_years))

        # sd_median[ind] = ndv
        # sd_median.mask[ind] = True
        # print(np.ma.getdata(sd_mq25)[rc,cc])

        sd_mq25[ind] = ndv
        sd_mq25.mask[ind] = True
        sd_mq50[ind] = ndv
        sd_mq50.mask[ind] = True
        sd_mq75[ind] = ndv
        sd_mq75.mask[ind] = True
        sd_max[ind] = ndv
        sd_max.mask[ind] = True

        layers = None

        # --------------------------------------------------------------- 
        # Write the sd_median grid to the generic GDAL dataset lon_lat_ds.
        lon_lat_ds.GetRasterBand(1).WriteArray(sd_mq50)
        # Even though ndv is a 32-bit float, it is a numpy type, and for
        # an unknown reason it has to be cast to a regular Python float
        # for SetNoDataValue to accept it without errors.
        lon_lat_ds.GetRasterBand(1).SetNoDataValue(float(ndv))
        if day_of_water_year < 93:
            year_range = '{}-{}'.format(opt.start_year-1, opt.finish_year-1)
        else:
            year_range = '{}-{}'.format(opt.start_year, opt.finish_year)
        desc = 'Median SNODAS {} '.format(product_name) + \
               '(mm) for {} '. \
               format(calendar.month_name[int(date_mmdd[0:2])]) + \
               '{}, '.format(int(date_mmdd[2:])) + \
               year_range
        lon_lat_ds.GetRasterBand(1).SetDescription(desc)

        # Write the median to a GeoTIFF. See
        # https://gdal.org/drivers/raster/gtiff.html
        tiff_name = 'SNODAS_clim_{}_'.format(product_file_string) + \
                    'median_{}.tif'.format(date_mmdd)
        print('Creating GeoTIFF "{}".'.format(tiff_name))
        tiff_driver = gdal.GetDriverByName('GTiff')
        tiff_driver.CreateCopy(tiff_name,
                               lon_lat_ds,
                               False,
                               options=["COMPRESS=LZW"])

        if opt.plot_results:

            # Define the color ramp for snow depth.
            snow_color_ramp = snow_colormap()

            # Generate the figure.
            title = 'SNODAS Median ({}) '.format(year_range) + \
                    '{} for '.format(product_title) + \
                    dowy_datetime.strftime('%m-%d')
            fig, ax = geo_grid_map(lon_lat_crs,
                                   xsize,
                                   ysize,
                                   1,
                                   bbox,
                                   lon_axis,
                                   lat_axis,
                                   lon_lat_ds,
                                   1,
                                   ndv,
                                   title,
                                   snow_color_ramp,
                                   display_units)
            mplplt.show()

        # --------------------------------------------------------------- 
        # Calculate the IQR.
        sd_iqr = sd_mq75 - sd_mq25
        # print(np.ma.getdata(sd_iqr)[rc,cc])
        # print(sd_iqr[rc,cc])
        sd_iqr[sd_iqr.mask == True] = ndv
        # print(np.ma.getdata(sd_iqr)[rc,cc])
        # print(sd_iqr[rc,cc])
        # print('<><><><><><>')

        # --------------------------------------------------------------- 
        # Write the 25% quantile to a GeoTIFF.
        lon_lat_ds.GetRasterBand(1).WriteArray(sd_mq25)
        lon_lat_ds.GetRasterBand(1).SetNoDataValue(float(ndv))
        desc = '25% quantile in SNODAS {} '.format(product_name) + \
               '(mm) for {} '. \
               format(calendar.month_name[int(date_mmdd[0:2])]) + \
               '{}, '.format(int(date_mmdd[2:])) + \
               year_range
        lon_lat_ds.GetRasterBand(1).SetDescription(desc)

        tiff_name = 'SNODAS_clim_{}_'.format(product_file_string) + \
                    'mq25_{}.tif'.format(date_mmdd)
        print('Creating GeoTIFF "{}".'.format(tiff_name))
        tiff_driver.CreateCopy(tiff_name,
                               lon_lat_ds,
                               False,
                               options=["COMPRESS=LZW"])

        if opt.plot_results:

            # Define the color ramp for snow depth.
            snow_color_ramp = snow_colormap()

            # Generate the figure.
            title = 'SNODAS MQ25 ({}) '.format(year_range) + \
                    '{} for '.format(product_title) + \
                    dowy_datetime.strftime('%m-%d')
            fig, ax = geo_grid_map(lon_lat_crs,
                                   xsize,
                                   ysize,
                                   1,
                                   bbox,
                                   lon_axis,
                                   lat_axis,
                                   lon_lat_ds,
                                   1,
                                   ndv,
                                   title,
                                   snow_color_ramp,
                                   display_units)
            mplplt.show()

        # --------------------------------------------------------------- 
        # Write the 75% quantile to a GeoTIFF.
        lon_lat_ds.GetRasterBand(1).WriteArray(sd_mq75)
        lon_lat_ds.GetRasterBand(1).SetNoDataValue(float(ndv))
        desc = '75% quantile in SNODAS {} '.format(product_name) + \
               '(mm) for {} '. \
               format(calendar.month_name[int(date_mmdd[0:2])]) + \
               '{}, '.format(int(date_mmdd[2:])) + \
               year_range
        lon_lat_ds.GetRasterBand(1).SetDescription(desc)

        tiff_name = 'SNODAS_clim_{}_'.format(product_file_string) + \
                    'mq75_{}.tif'.format(date_mmdd)
        print('Creating GeoTIFF "{}".'.format(tiff_name))
        tiff_driver.CreateCopy(tiff_name,
                               lon_lat_ds,
                               False,
                               options=["COMPRESS=LZW"])

        if opt.plot_results:

            # Define the color ramp for snow depth.
            snow_color_ramp = snow_colormap()

            # Generate the figure.
            title = 'SNODAS MQ75 ({}) '.format(year_range) + \
                    '{} for '.format(product_title) + \
                    dowy_datetime.strftime('%m-%d')
            fig, ax = geo_grid_map(lon_lat_crs,
                                   xsize,
                                   ysize,
                                   1,
                                   bbox,
                                   lon_axis,
                                   lat_axis,
                                   lon_lat_ds,
                                   1,
                                   ndv,
                                   title,
                                   snow_color_ramp,
                                   display_units)
            mplplt.show()

        # --------------------------------------------------------------- 
        # Write the IQR to a GeoTIFF.
        lon_lat_ds.GetRasterBand(1).WriteArray(sd_iqr)
        lon_lat_ds.GetRasterBand(1).SetNoDataValue(float(ndv))
        desc = 'Interquartile range in SNODAS {} '.format(product_name) + \
               '(mm) for {} '. \
               format(calendar.month_name[int(date_mmdd[0:2])]) + \
               '{}, '.format(int(date_mmdd[2:])) + \
               year_range
        lon_lat_ds.GetRasterBand(1).SetDescription(desc)

        tiff_name = 'SNODAS_clim_{}_'.format(product_file_string) + \
                    'iqr_{}.tif'.format(date_mmdd)
        print('Creating GeoTIFF "{}".'.format(tiff_name))
        tiff_driver.CreateCopy(tiff_name,
                               lon_lat_ds,
                               False,
                               options=["COMPRESS=LZW"])

        if opt.plot_results:

            # Define the color ramp for snow depth.
            snow_color_ramp = snow_colormap()

            # Generate the figure.
            title = 'SNODAS IQR ({}) '.format(year_range) + \
                    '{} for '.format(product_title) + \
                    dowy_datetime.strftime('%m-%d')
            fig, ax = geo_grid_map(lon_lat_crs,
                                   xsize,
                                   ysize,
                                   1,
                                   bbox,
                                   lon_axis,
                                   lat_axis,
                                   lon_lat_ds,
                                   1,
                                   ndv,
                                   title,
                                   snow_color_ramp,
                                   display_units)
            mplplt.show()

        # --------------------------------------------------------------- 
        # Write the maximum to a GeoTIFF.
        lon_lat_ds.GetRasterBand(1).WriteArray(sd_max)
        lon_lat_ds.GetRasterBand(1).SetNoDataValue(float(ndv))
        desc = 'Maximum SNODAS {} '.format(product_name) + \
               '(mm) for {} '. \
               format(calendar.month_name[int(date_mmdd[0:2])]) + \
               '{}, '.format(int(date_mmdd[2:])) + \
               year_range
        lon_lat_ds.GetRasterBand(1).SetDescription(desc)

        tiff_name = 'SNODAS_clim_{}_'.format(product_file_string) + \
                    'max_{}.tif'.format(date_mmdd)
        print('Creating GeoTIFF "{}".'.format(tiff_name))
        tiff_driver.CreateCopy(tiff_name,
                               lon_lat_ds,
                               False,
                               options=["COMPRESS=LZW"])

        if opt.plot_results:

            # Define the color ramp for snow depth.
            snow_color_ramp = snow_colormap()

            # Generate the figure.
            title = 'SNODAS Maximum ({}) '.format(year_range) + \
                    '{} for '.format(product_title) + \
                    dowy_datetime.strftime('%m-%d')
            fig, ax = geo_grid_map(lon_lat_crs,
                                   xsize,
                                   ysize,
                                   1,
                                   bbox,
                                   lon_axis,
                                   lat_axis,
                                   lon_lat_ds,
                                   1,
                                   ndv,
                                   title,
                                   snow_color_ramp,
                                   display_units)
            mplplt.show()


if __name__ == '__main__':
    main()
