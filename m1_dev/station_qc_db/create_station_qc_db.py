#!/usr/bin/python3.6

"""
Create quality control database for observations in the NOHRSC web
database.
"""

import argparse
import datetime as dt
from netCDF4 import Dataset, date2num
import numpy as np
import pyproj
import sys
import os


class Opt:
    def __init__(self):
        self.start_datetime = []
        self.finish_datetime = []
        self.db_dir = []


def parse_args():
    """
    Parse command line arguments.
    """

    # Get the current date.
    system_datetime = dt.datetime.utcnow()

    help_message = 'Create a database for QC of station observations.'
    parser = argparse.ArgumentParser(description=help_message)
    parser.add_argument('-s', '--start_date',
                        type=str,
                        metavar='start date YYYYMMDDHH',
                        nargs='?',
                        help='Start date (UTC) in YYYYMMDDHH format.')
    parser.add_argument('-f', '--finish_date',
                        type=str,
                        metavar='finish date YYYYMMDDHH',
                        nargs='?',
                        help='Finish date (UTC) in YYYYMMDDHH format.')
    parser.add_argument('-d', '--db_dir',
                        type=str,
                        metavar='directory location of output database files',
                        nargs='?',
                        help='Directory in which output database files are ' +
                        'stored.')
    args = parser.parse_args()

    if args.start_date:
        if len(args.start_date) != 10:

            print('Start date must be 10 characters long.',
                  file=sys.stderr)
            exit(1)
        try:
            start_datetime = dt.datetime.strptime(args.start_date, '%Y%m%d%H')
        except:
            print('Invalid start date "{}".'.format(args.start_date),
                  file=sys.stderr)
            exit(1)
    else:
        # Use the first hour of the current calendar month.
        start_datetime = dt.datetime(year=system_datetime.year,
                                     month=system_datetime.month,
                                     day=1,
                                     hour=0)
        # start_date = start_datetime.strftime('%Y%m%d%H')
    Opt.start_datetime = start_datetime

    if args.finish_date:
        if len(args.finish_date) != 10:
            print('Finish date must be 10 characters long.',
                  file=sys.stderr)
            exit(1)
        try:
            finish_datetime = dt.datetime.strptime(args.finish_date,
                                                   '%Y%m%d%H')
        except:
            print('Invalid finish date "{}".'.format(args.finish_date),
                  file=sys.stderr)
            exit(1)
    else:
        # Use the last hour of the current calendar month.
        finish_datetime = system_datetime
        finish_datetime = \
            system_datetime.replace(month=finish_datetime.month % 12 + 1,
                                    day=1,
                                    hour=23,
                                    minute=0,
                                    second=0,
                                    microsecond=0) - \
            dt.timedelta(days=1)
        # finish_date = finish_datetime.strftime('%Y%m%d%H')
    Opt.finish_datetime = finish_datetime

    if args.db_dir:
        if not os.path.isdir(args.db_dir):
            raise FileNotFoundError(errno.ENOENT,
                                    os.strerror(errno.ENOENT),
                                    args.db_dir)
        Opt.db_dir = args.db_dir
    else:
        Opt.db_dir = os.getcwd()

    if not os.access(Opt.db_dir, os.W_OK):
        print('User cannot write to directory {}.'.format(Opt.db_dir),
              file=sys.stderr)
        exit(1)

    return Opt


def main():
    """
    Create a NetCDF database for storing quality control information for
    station observations from the NOHRSC web database.
    """

    # Read command line arguments.
    opt = parse_args()
    if opt is None:
        print('ERROR: Failed to parse command line.', file=sys.stderr)
        exit(1)

    # Define QC database.

    db_file = 'station_qc_db_' + \
              opt.start_datetime.strftime('%Y%m%d%H') + \
              '_to_' + \
              opt.finish_datetime.strftime('%Y%m%d%H') + \
              '.nc'
    nc_out = Dataset(os.path.join(opt.db_dir, db_file),
                     'w', format='NETCDF4', clobber=True)

    # Define the "last update datetime" attribute.
    nc_out.setncattr_string('last_datetime_updated',
                            '1970-01-01 00:00:00 UTC')

    # Define the "last station metadata update datetime" attribute.
    nc_out.setncattr_string('last_station_update_datetime',
                            '1970-01-01 00:00:00 UTC')

    # nc_out.setncattr('metadata_update_interval_hours', 12)

    # Define dimensions.

    time_length = opt.finish_datetime - opt.start_datetime
    num_hours = time_length.days * 24 + time_length.seconds // 3600 + 1

    dim_time = nc_out.createDimension('time', num_hours)
    dim_station = nc_out.createDimension('station', None)

    enum_ndv = {'float32 missing': np.finfo(np.float32).min,
                'float64 missing': np.finfo(np.float64).min,
                'int16 missing': np.iinfo(np.int16).min,
                'int32 missing': np.iinfo(np.int32).min,
                'byte missing': np.iinfo(np.byte).min}

    # Define the dimension/coordinate variable for time.
    var_time = nc_out.createVariable('time',
                                     'i4',
                                     ('time'),
                                     fill_value=enum_ndv['int32 missing'])
    var_time.units = 'hours since 1970-01-01 00:00:00 UTC'
    var_time.calendar = 'standard'

    time_axis = [opt.start_datetime + dt.timedelta(hours=h)
                 for h in range(0, num_hours)]
    var_time[:] = date2num(time_axis, units=var_time.units)


    # Define the variables on the station dimension (station
    # metadata). Some of these may not be needed for the QC database,
    # but unless they take up too much storage we may as well include
    # them.

    # Station object identifier. This is the only unique identifier
    # for stations.
    var_station_obj_id = \
        nc_out.createVariable('station_obj_identifier',
                              'i4',
                              ('station'),
                              fill_value=enum_ndv['int32 missing'])
    var_station_obj_id.setncattr_string('allstation_column_name',
                                        'obj_identifier')

    # Station ID. This is a text string generally taken from MADIS or
    # NWSLI metadata that identifies a site. These are nearly unique,
    # but not perfectly so.
    var_station_id = \
        nc_out.createVariable('station_id',
                              str,
                              ('station'))
    var_station_id._Encoding = 'UTF-8'
    var_station_id.setncattr_string('allstation_column_name',
                                    'station_id')

    var_station_name = \
        nc_out.createVariable('station_name',
                              str,
                              ('station'))
    var_station_name._Encoding = 'UTF-8'
    var_station_name.setncattr_string('allstation_column_name',
                                      'name')

    var_station_source = \
        nc_out.createVariable('station_source',
                              str,
                              ('station'))
    var_station_source._Encoding = 'UTF-8'
    var_station_source.setncattr_string('allstation_column_name',
                                        'source')

    var_station_type = \
        nc_out.createVariable('station_type',
                              str,
                              ('station'))
    var_station_type._Encoding = 'UTF-8'
    var_station_type.setncattr_string('allstation_column_name',
                                      'station_type')

    var_station_longitude = \
        nc_out.createVariable('station_longitude',
                              'f8',
                              ('station'),
                              fill_value=enum_ndv['float64 missing'])
    var_station_longitude.setncattr_string('allstation_column_name',
                                           'coordinates[0]')

    var_station_latitude = \
        nc_out.createVariable('station_latitude',
                              'f8',
                              ('station'),
                              fill_value=enum_ndv['float64 missing'])
    var_station_latitude.setncattr_string('allstation_column_name',
                                          'coordinates[1]')

    var_station_elevation = \
        nc_out.createVariable('station_elevation',
                              'i4',
                              ('station'),
                              fill_value=enum_ndv['int32 missing'])
    var_station_elevation.setncattr_string('allstation_column_name',
                                           'elevation')

    var_station_rec_elevation = \
        nc_out.createVariable('station_recorded_elevation',
                              'i4',
                              ('station'),
                              fill_value=enum_ndv['int32 missing'])
    var_station_rec_elevation.setncattr_string('allstation_column_name',
                                               'recorded_elevation')

    # var_station_details = \
    #     nc_out.createVariable('station_details',
    #                           str,
    #                           ('station'))
    # var_station_details._Encoding = 'UTF-8'
    # var_station_details.setncattr_string('allstation_column_name',
    #                                      'details')

    # var_station_vendor = \
    #     nc_out.createVariable('station_vendor',
    #                           str,
    #                           ('station'))
    # var_station_vendor._Encoding = 'UTF-8'
    # var_station_vendor.setncattr_string('allstation_column_name',
    #                                     'vendor')

    # var_station_vendor_date = \
    #     nc_out.createVariable('station_vendor_date',
    #                           str,
    #                           ('station'))
    # var_station_vendor_date._Encoding = 'UTF-8'
    # var_station_vendor_date.setncattr_string('allstation_column_name',
    #                                          'vendor_date')

    # var_station_use = \
    #     nc_out.createVariable('station_use',
    #                           'b',
    #                           ('station'),
    #                           fill_value=enum_ndv['byte missing'])

    # var_station_use.setncattr_string('allstation_column_name',
    #                                  'use')

    var_station_start_date = \
        nc_out.createVariable('station_start_date',
                              str,
                              ('station'))
    var_station_start_date._Encoding = 'UTF-8'
    var_station_start_date.setncattr_string('allstation_column_name',
                                            'start_date')

    var_station_stop_date = \
        nc_out.createVariable('station_stop_date',
                              str,
                              ('station'))
    var_station_stop_date._Encoding = 'UTF-8'
    var_station_stop_date.setncattr_string('allstation_column_name',
                                           'stop_date')

    var_station_added_date = \
        nc_out.createVariable('station_added_date',
                              str,
                              ('station'))
    var_station_added_date._Encoding = 'UTF-8'
    var_station_added_date.setncattr_string('allstation_column_name',
                                            'added_date')

    # var_station_nwm_grid_column = \
    #     nc_out.createVariable('station_nwm_grid_column',
    #                           'f8',
    #                           ('station'),
    #                           fill_value=enum_ndv['float64 missing'])

    # var_station_nwm_grid_row = \
    #     nc_out.createVariable('station_nwm_grid_row',
    #                           'f8',
    #                           ('station'),
    #                           fill_value=enum_ndv['float64 missing'])

    # The station_network variable is a comma-separated list of networks
    # that claim a station to be among their own. This claim is based on
    # a station being listed in the metadata file for a given network,
    # and is meant to confirm or augment the information in station_type
    # and station_vendor variables.
    # var_station_network = \
    #     nc_out.createVariable('station_network',
    #                           str,
    #                           ('station'))
    # var_station_network._Encoding = 'UTF-8'


    # Define bits (offsets) for snow depth QC flags.
    # These are, with references to tables in Durre (2010):
    # Basic integrity checks (Table 1)
    #  0 naught
    #    trace value check for snow depth
    #  1 world_record_exceedance
    #    world record exceedance check for snow depth
    #  2 world_record_increase_exceedance
    #    world record exceedance check for snow depth increase
    #  3 streak
    #    identical-value streak check for snow depth
    # Outlier checks (Table 2)
    #  4 excursion
    #    snow depth excursions to large values in time series
    #    (not in Durre 2010)
    #  5 rate
    #    snow depth increase between adjacent observations exceeds
    #    reasonable expectations
    #    (not in Durre 2010)
    #  6 gap
    #    snow depth gap check
    # Internal and temporal consistency checks (Table 3)
    #  7 temperature_consistency
    #    snow-temperature consistency check for snow depth
    #  8 snowfall_consistency
    #    snowfall-snow depth consistency check
    #  9 precip_consistency
    #    snow-precipitation consistency check "SNWD increase with 0 PRCP"
    # 10 depth_precip_ratio
    #    snow-precipitation consistency check "SNWD/PRCP ratio"
    # Spatial consistency checks (Table 4)
    # 11 spatial_temperature_consistency
    #    spatial snow-temperature consistency check for snow depth

    snow_depth_qc_bits = {'naught': 0,
                          'world_record_exceedance': 1,
                          'world_record_increase_exceedance': 2,
                          'streak': 3,
                          'anomaly': 4,
                          'rate': 5,
                          'gap': 6,
                          'temperature_consistency': 7,
                          'snowfall_consistency': 8,
                          'precip_consistency': 9,
                          'depth_precip_ratio': 10,
                          'spatial_temperature_consistency': 11}

    # Define QC variables.

    dims = ('station', 'time')
    station_chunk = 1
    time_chunk = min(1024, num_hours)
    chunk = (station_chunk, time_chunk)

    var_snow_depth_qc_checked = \
        nc_out.createVariable('snow_depth_qc_checked',
                              'u4',
                              dims,
                              fill_value=-1,
                              zlib=True,
                              chunksizes=chunk)

    var_snow_depth_qc_checked.setncattr_string('wdb0_table_name',
                                               'point.obs_snow_depth')
    var_snow_depth_qc_checked.setncattr('qc_test_names',
                                        list(snow_depth_qc_bits.keys()))
    var_snow_depth_qc_checked.setncattr('qc_test_bits',
                                        [np.uint8(i)
                                         for i in
                                         list(snow_depth_qc_bits.values())])

    var_snow_depth_qc = \
        nc_out.createVariable('snow_depth_qc',
                              'u4',
                              dims,
                              fill_value=-1,
                              zlib=True,
                              chunksizes=chunk)

    var_snow_depth_qc.setncattr_string('wdb0_table_name',
                                       'point.obs_snow_depth')
    var_snow_depth_qc.setncattr('qc_test_names',
                                list(snow_depth_qc_bits.keys()))
    var_snow_depth_qc.setncattr('qc_test_bits',
                                [np.uint8(i)
                                 for i in
                                 list(snow_depth_qc_bits.values())])

    nc_out.close()

    print('INFO: Created ' + os.path.join(opt.db_dir, db_file) + '.')

if __name__ == '__main__':
    main()
