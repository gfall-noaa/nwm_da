#!/usr/bin/python3.6
'''
  Update databases that created by the create program initially.
  The station information within the domain will be retrieved.
  The forcing and land variables and other states (from model) will
  be sampled at all stations and be written to the databases.

  There are two basic database options in updating. One is for
  operation and the other is for archiving.
'''

import argparse
import datetime as dt
import re
import sys
import os
import time
import errno
import shutil
import itertools
import sqlite3
import pyproj
import pandas as pd
import matplotlib.pyplot as plt
import psycopg2
import numpy as np
from netCDF4 import Dataset, num2date
import getpass
#from netCDF4 import Dataset, date2num, num2date
#import math

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'lib'))
import nwm_da_time as ndt
sys.path.append(os.path.join(os.path.dirname(__file__), 'lib'))
import nwm_da_sqlite_db as nds_db

#sqlite3.register_adapter(np.int32, lambda val: int(val))
sqlite3.register_adapter(np.int32, int)


def progress(count, total, status=''):
    """
    Progress bar:
    Copied from
    https://gist.github.com/vladignatyev/06860ec2040cb497f0f3
    Modified: 1/6/2020
    """
    bar_len = 50
    filled_len = int(round(bar_len * count / float(total)))

    percents = round(100.0 * count / float(total), 1)
    bar = '=' * filled_len + '-' * (bar_len - filled_len)

    #sys.stdout.write('\r[%s] %s%s ...%s\r' % (bar, percents, '%', status))
    sys.stdout.write('\r[%s] %s%s %s\r' % (bar, percents, '%', status))
    sys.stdout.flush()

def get_database_info(conn):
    '''
    Get other info from databases_info table.
    '''
    #Get the sampling method since now only one sampling method is used at a specific run
    sampling_method = \
        conn.execute("SELECT method FROM sampling_themes WHERE use='YES'").fetchone()[0]
        #because fetchone() gets a tuple

    #Obtain databbase names via databases_info table in the base database
    forcing_single_db_from_db = \
        conn.execute("SELECT forcing_single_db_name FROM databases_info").fetchone()[0]

    land_single_db_from_db = \
        conn.execute("SELECT land_single_db_name FROM databases_info").fetchone()[0]

    db_num_days_update = \
        conn.execute("SELECT num_days_update from databases_info").fetchone()[0]

    db_last_updated_datetime_ep = \
        conn.execute("SELECT last_updated_date FROM databases_info").fetchone()[0]

    station_update_time_ep = \
        conn.execute("SELECT last_update_datetime FROM station_control LIMIT 1"). \
                     fetchone()
    if station_update_time_ep is not None:
        last_station_update_datetime_ep = station_update_time_ep[0]
        print('\nINFO: Last station metadata update was performed at {} UTC.'
              .format(ndt.utc_epoch_to_string(last_station_update_datetime_ep)))
    else:
        station_last_datetime_dt = dt.datetime.strptime('1970-01-01 00:00:00',
                                                        '%Y-%m-%d %H:%M:%S')
        last_station_update_datetime_ep = \
            ndt.datetime_to_utc_epoch(station_last_datetime_dt)
        print('\nINFO: Last station metadata update was performed at {} '.
              format(ndt.utc_epoch_to_string(last_station_update_datetime_ep)) +
              '(i.e., it has never been updated).')

    nwm_archive_dir = \
        conn.execute("SELECT nwm_archive_dir FROM databases_info").fetchall()[0][0]

    if os.path.exists(nwm_archive_dir) and os.path.isdir(nwm_archive_dir):
        print('Archive directory is: ', nwm_archive_dir)
    else:
        print('Archive directory does not exist')
        sys.exit(1)

    #Determine if land_soil and land_snow layer databases need be there.
    land_layer = conn.execute("SELECT var_name, file_type FROM nwm_meta WHERE + \
                       file_type='land' and var_name LIKE '%_by_layer%'").fetchall()
    if len(land_layer) == 0:
        print('No land layer variables')

    return sampling_method, \
           land_layer, \
           forcing_single_db_from_db, \
           land_single_db_from_db, \
           db_num_days_update, \
           db_last_updated_datetime_ep, \
           last_station_update_datetime_ep, \
           nwm_archive_dir

def database_info_and_checks(conn,
                             db_dir,
                             db_file,
                             oper):
    '''Check database names and times are consistent between
       what's recorded in the database and what's in the files'''

    print('\nName and time consistency checking for {}...'.format(db_file))
    db_start_datetime_from_name_ep, \
    db_finish_datetime_from_name_ep = \
        nds_db.get_start_finish_date_from_name(db_file, oper)

    db_start_datetime_from_db_ep, \
    db_finish_datetime_from_db_ep = \
           nds_db.get_start_finish_date_from_db(conn)

    forcing_single_db_assumed = db_file.replace('_base', '_forcing_single')
    land_single_db_assumed = db_file.replace('_base', '_land_single')

    sampling_method, \
    land_layer, \
    forcing_single_db_from_db, \
    land_single_db_from_db, \
    db_num_days_update, \
    db_last_updated_datetime_ep, \
    last_station_update_datetime_ep, \
    nwm_archive_dir = \
        get_database_info(conn)

    #print(os.path.join(db_dir, forcing_single_db_from_db))
    #print(forcing_single_db_assumed)
    if forcing_single_db_from_db == forcing_single_db_assumed:
        print('  forcing_single_db file name is consistent')
        forcing_single_db = os.path.join(db_dir, forcing_single_db_assumed)
    if land_single_db_from_db == land_single_db_assumed:
        print('  land_single_db file name is consistent')
        land_single_db = os.path.join(db_dir, land_single_db_assumed)

    if db_start_datetime_from_db_ep != db_start_datetime_from_name_ep and \
       oper is False:
        print('WARNING: start datetime in database does not match ' +
              'filename "{}".'.format(db_file),
              file=sys.stderr)
    if db_finish_datetime_from_db_ep != db_finish_datetime_from_name_ep and \
       oper is False:
        print('WARNING: finish datetime in database does not match ' +
              'filename "{}".'.format(db_file),
              file=sys.stderr)

    print('  Number of days to update: ', db_num_days_update)
    print('  Last updated time: ',
          ndt.utc_epoch_to_string(db_last_updated_datetime_ep))
    if db_num_days_update == 0 and oper is False:
        print('\nUpdating archive databases ...')
        new_db_start_datetime_ep = db_start_datetime_from_db_ep
        new_db_finish_datetime_ep = db_finish_datetime_from_db_ep
        print('Start date (archive): {}'.
              format(ndt.utc_epoch_to_string(new_db_start_datetime_ep)))
        print('Finish date (archive): {}'.
              format(ndt.utc_epoch_to_string(new_db_finish_datetime_ep)))
    else:
        print('\nUpdating operational databases ...')
        this_update_datetime_ep = time.time()
        new_db_start_datetime_ep = this_update_datetime_ep - \
                                   db_num_days_update * 86400
                                   #db_num_days_update[0] * 86400
        #db_start_datetime_from_db_ep = new_db_start_datetime_ep
        new_db_finish_datetime_ep = this_update_datetime_ep

        print('New start date (oper): {}'.
              format(ndt.utc_epoch_to_string(new_db_start_datetime_ep)))
        print('New finish date (oper): {}'.
              format(ndt.utc_epoch_to_string(new_db_finish_datetime_ep)))

    return db_start_datetime_from_db_ep, \
           new_db_start_datetime_ep, \
           new_db_finish_datetime_ep, \
           sampling_method, \
           land_layer, \
           forcing_single_db, \
           land_single_db, \
           nwm_archive_dir, \
           last_station_update_datetime_ep

def zip_and_sort_nwm_files(nwm_file_names,
                           nwm_file_paths,
                           nwm_file_cycle_types,
                           nwm_file_time_minus_hours,
                           nwm_file_datetimes_ep,
                           nwm_file_cycle_datetimes_ep):
    '''Group sort NWM files'''
    # The NWM files are now sorted in chronological order, with files
    # having larger "tm" values first, whenever multiple files cover
    # the same datetime. This reflects the preference for larger "tm"
    # values (when the files for smaller "tm" values are encountered
    # they will be skipped since data with a higher "tm" value has
    # already been processed), and avoids processing data that would
    # just be overwritten immediately, which is wasteful.

    # Zip NWM file lists for group sorting.
    zipped = zip(nwm_file_names,
                 nwm_file_paths,
                 nwm_file_cycle_types,
                 nwm_file_time_minus_hours,
                 nwm_file_datetimes_ep,
                 nwm_file_cycle_datetimes_ep)

    # Sort NWM files in descending order of nwm_file_time_minus_hours.
    zipped = sorted(zipped, key=lambda nwm_file_list: nwm_file_list[3],
                    reverse=True)
    # Sort NWM files in ascending order of nwm_file_cycle_types.
    zipped = sorted(zipped, key=lambda nwm_file_list: nwm_file_list[2],
                    reverse=False)
    # Sort NWM files in order of nwm_file_datetimes_ep.
    zipped = sorted(zipped, key=lambda nwm_file_list: nwm_file_list[4],
                    reverse=False)
    # Unzip the sorted lists.
    nwm_file_names, \
    nwm_file_paths, \
    nwm_file_cycle_types, \
    nwm_file_time_minus_hours, \
    nwm_file_datetimes_ep, \
    nwm_file_cycle_datetimes_ep = zip(*zipped)

    return nwm_file_names, \
           nwm_file_paths, \
           nwm_file_cycle_types, \
           nwm_file_time_minus_hours, \
           nwm_file_datetimes_ep, \
           nwm_file_cycle_datetimes_ep


def form_web_query_string(db_column_names,
                          wdb_column_names,
                          data_column_types,
                          need_as=False):
    '''Form query string for web data selection'''

    wdb_selection_str_lst = list()
    for col_num, col_name in enumerate(db_column_names):
        if need_as:
            if data_column_types[col_num] == 'text':
                each_col_str = \
                               'TRIM(tstn.' + wdb_column_names[col_num] + \
                               ') AS ' + col_name
            else:
                each_col_str = \
                               'tstn.' + wdb_column_names[col_num] + \
                               ' AS ' + col_name
        else:
            each_col_str = 'tstn.' + wdb_column_names[col_num]
        wdb_selection_str_lst.append(each_col_str)
    #wdb_selection_str_lst = ['tstn.' + s for s in wdb_selection_str_lst]
    wdb_selection_str = \
        ','.join([str(elem) for elem in wdb_selection_str_lst])
    return wdb_selection_str

def subset_station_latlon_obj_ids(datetime_ep,
                                  db_grid_cols,
                                  db_grid_rows,
                                  obj_ids,
                                  db_start_dates_ep,
                                  db_stop_dates_ep):
    '''Subset station info when the datetime is between
       start and stop'''
    #print(len(db_grid_cols), len(db_grid_rows), len(obj_ids))

    jan_1_1900_epoch = -2208988800

    start_filter_list = \
      [dt[0] == jan_1_1900_epoch or dt[0] <= datetime_ep \
       for dt in db_start_dates_ep]
    stop_filter_list = \
      [dt[0] == jan_1_1900_epoch or dt[0] >= datetime_ep \
       for dt in db_stop_dates_ep]
    filter_com = np.logical_and(stop_filter_list, start_filter_list)
    # One could also use below to get the combined filter as list:
    #filter_com = [a and b for a, b in zip(stop_filter_list, start_filter_list)]
    db_grid_cols = list(itertools.compress(db_grid_cols, filter_com))
    db_grid_rows = list(itertools.compress(db_grid_rows, filter_com))
    obj_ids = list(itertools.compress(obj_ids, filter_com))
    num_stations = len(obj_ids)
    #print(len(db_grid_cols), len(db_grid_rows), len(obj_ids))

    return num_stations, db_grid_cols, db_grid_rows, obj_ids


def get_nwm_files_processed(conn):
    '''
    Get nwm files that have been processed from nwm_file_update_info.
    '''
    nwm_files_processed = \
        conn.execute('SELECT files_read from nwm_file_update_info').fetchall()
    # Convert list of tuples into list.
    #nwm_files_processed = [''.join(i) for i in nwm_files_processed]
    #nwm_files_processed = [file[0] for file in nwm_files_processed]
    #nwm_files_processed = list(sum(nwm_files_processed, ()))
    nwm_files_processed = list(map(''.join, nwm_files_processed))
    if len(nwm_files_processed) <= 0:
        print('\nINFO: No NWM file has been processed yet for this database.')
    else:
        print('\nINFO: {} NWM files have been processed for this database.'.
              format(len(nwm_files_processed)))
    return nwm_files_processed

def get_cycle_theme_info(conn, current_yyyymm):
    '''
    Get cycle types and file strings etc for regular and
    extended analysis from the table: cycle_type_themes.
    '''
    nwm_cycle_type_ext_ana = conn.execute("SELECT value FROM cycle_type_themes " + \
                        "WHERE type='extended analysis'").fetchone()[0]
    nwm_cycle_type_ana = conn.execute("SELECT value FROM cycle_type_themes " + \
                        "WHERE type='analysis'").fetchone()[0]
    # add other case below later

    ana_ext_str = conn.execute("SELECT file_str FROM cycle_type_themes " + \
                               "WHERE type='extended analysis'").fetchone()[0]
    ana_str = conn.execute("SELECT file_str FROM cycle_type_themes " + \
                           "WHERE type='analysis'").fetchone()[0]

    ana_ext_opt = conn.execute("SELECT option FROM cycle_type_themes " + \
                               "WHERE type='extended analysis'").fetchone()[0]
    # add other case below later

    if ana_ext_opt == 1:
        regex_str = '^nwm\.'+current_yyyymm+'[0-9]{2}\.t[0-9]{2}z\.' + \
                     ana_ext_str + '\.[^.]+\.tm[0-9]{2}\.conus\.nc$'
        ana_ext_pattern = re.compile(regex_str)
    else:
        regex_str = None

    ana_opt = conn.execute("SELECT option FROM cycle_type_themes " + \
                           "WHERE type='analysis'").fetchone()[0]
    if ana_opt == 1:
        ana_regex_str = '^nwm.' + current_yyyymm + '[0-9]{2}\.t[0-9]{2}z\.' + \
                         ana_str + '\.[^.]+\.tm[0-9]{2}\.conus\.nc$'
        ana_pattern = re.compile(ana_regex_str)
    else:
        ana_regex_str = None

    return nwm_cycle_type_ext_ana, \
           nwm_cycle_type_ana, \
           ana_ext_str, \
           ana_str, \
           ana_ext_pattern, \
           ana_pattern, \
           regex_str, \
           ana_regex_str

def get_nwm_files(nwm_archive_dir,
                  ana_ext_pattern,
                  ana_pattern,
                  db_start_datetime_ep,
                  db_finish_datetime_ep,
                  nwm_cycle_type_ext_ana,
                  nwm_cycle_type_ana,
                  nwm_files_processed,
                  oper, conn):
    '''
    Loop over all NWM files of supported types to
    determine which to include in the update.
    '''
    nwm_file_paths = []
    nwm_file_names = []
    nwm_file_time_minus_hours = []
    nwm_file_datetimes_ep = []
    nwm_file_cycle_datetimes_ep = []
    nwm_file_cycle_types_val = []


    # Loop over all NWM files of supported types; determine which
    # to include in this update.
    for root, dirs, files in os.walk(nwm_archive_dir, followlinks=True):

        files_not_processed = list(set(files).difference(nwm_files_processed))

        if len(files_not_processed) == 0 or len(files) == 0:
            continue

        for file_name in files_not_processed:

            # print('<<{}>> <<{}>>'.format(file_name, nwm_files_processed[0]))

            # Short debugging section, which uses the "index" method to
            # confirm, through exceptions, that file_name is NOT included in
            # nwm_files_processed.
            # foo = None
            # try:
            #     foo = nwm_files_processed.index(file_name)
            # except ValueError:
            #     pass
            # if foo is not None:
            #     print('BUG - FOUND {}'.format(nwm_files_processed[foo]))
            #     # print('nwm_files_processed:')
            #     # print(nwm_files_processed)
            #     sys.exit(1)

            if ana_ext_pattern.fullmatch(file_name) is None and \
               ana_pattern.fullmatch(file_name) is None:
                continue

            nwm_file_name = file_name
            nwm_file_path = os.path.join(root, file_name)

            nwm_file_cycle_type_str, \
            nwm_group, \
            nwm_file_time_minus_hour, \
            nwm_file_cycle_datetime_ep, \
            nwm_file_datetime_ep = \
                get_nwm_file_info(nwm_file_name)

            # If the nwm_file_datetime does not fit into the time
            # frame covered by the current database file, skip it.
            if (nwm_file_datetime_ep < db_start_datetime_ep) or \
               (nwm_file_datetime_ep > db_finish_datetime_ep):
                #print('reject {} - not in DB time frame'.format(nwm_file_name))
                continue

            # Check if nwm_file_names are in the nwm_file_processed.

            #NOTE: list(sum(nwm_files_processed,())) to convert list of tuples to list
            #if nwm_file_name in list(sum(nwm_files_processed, ())):
            #    #print("File {} is in the processed list.".format(nwm_file_name))
            #    continue

            if nwm_file_cycle_type_str == 'analysis_assim_extend':
                nwm_file_cycle_type_val = nwm_cycle_type_ext_ana
            elif nwm_file_cycle_type_str == 'analysis_assim':
                nwm_file_cycle_type_val = nwm_cycle_type_ana
                ## NOTE: Values for other type of cycle_types can be retreated from
                ##       the table: cycle_type_themes. Processing other types of data
                ##       file has not been implemented yet.
            else:
                print('WARNING: Unsupported cycle type ' +
                      '"{}" '.format(nwm_file_cycle_type_str) +
                      ' in NWM file {}.'.format(nwm_file_name),
                      file=sys.stderr)
                continue

            # For archive databases, if existing data in the same
            # nwm_group_db for the same nwm_file_datetime_db is a
            # better reference than the NWM file, do not process.
            if oper is False and nwm_file_time_minus_hour != 0:

                ref_nwm_file_name, ref_nwm_cycle_datetime_db_ep, \
                    ref_time_minus_hours_db, ref_cycle_type_db = \
                    ref_nwm_file_update_info(conn,
                                             nwm_file_datetime_ep,
                                             nwm_group)
                if ref_nwm_file_name is not None:
                    if (nwm_file_cycle_type_val > ref_cycle_type_db) or \
                       ((nwm_file_cycle_type_val == ref_cycle_type_db) and \
                        (nwm_file_time_minus_hour < ref_time_minus_hours_db)):
                        # Better is_reference_db = 1 data already exists.
                        #print('reject {} '.format(nwm_file_name) +
                        #      '- worse than {}'.format(ref_nwm_file_name))
                        continue
                    # else:
                    #     print('!file {} is going to be the new reference data '.format(nwm_file_name) +
                    #           'as it is better than {}'.format(ref_nwm_file_name))


            # print('*** will include {}'.format(nwm_file_name))

            # Combine files to be processed
            nwm_file_paths.append(nwm_file_path)
            nwm_file_names.append(nwm_file_name)
            nwm_file_time_minus_hours.append(nwm_file_time_minus_hour)
            #nwm_file_time_minus_hours.append(time_minus_hours)
            nwm_file_datetimes_ep.append(nwm_file_datetime_ep)
            nwm_file_cycle_datetimes_ep.append(nwm_file_cycle_datetime_ep)
            nwm_file_cycle_types_val.append(nwm_file_cycle_type_val)

    return nwm_file_paths, \
           nwm_file_names, \
           nwm_file_time_minus_hours, \
           nwm_file_datetimes_ep, \
           nwm_file_cycle_datetimes_ep, \
           nwm_file_cycle_types_val

def get_nwm_file_info(a_nwm_file_name):
    '''
    Get needed information based on nwm file name.
    '''
    # Identify whether this file is a "land" or "forcing" file
    # based on the filename.
    # Truth: used this filename-parsing approach before
    # discovering the "model_output_type" global attribute,
    # which is a far better way to get the information. Keeping
    # the filename-based approach for reference. It does no harm
    # since it now just verifies the "model_output_type" used
    # after the NWM file is opened (below).

    # Determine the cycle type from the filename.
    nwm_file_cycle_type_str = \
        re.findall('^nwm\.[0-9]{8}\.t[0-9]{2}z\.[^.]+\.',
                   a_nwm_file_name)[0][18:-1]
    nwm_group = re.findall('\.[^.]+\.tm[0-9]{2}\.conus\.nc$',
                           a_nwm_file_name)[0][1:-14]

    # Get time information as interpreted from the NWM file name.
    cycle_yyyymmdd = re.findall('[0-9]{8}', a_nwm_file_name)[0]
    cycle_hh = re.findall('\.t[0-9]{2}z\.', a_nwm_file_name)[0][2:-2]
    nwm_file_time_minus_hours = int(re.findall('\.tm[0-9]{2}\.',
                               a_nwm_file_name)[0][3:-1])
    nwm_file_cycle_datetime_dt = \
        dt.datetime.strptime(cycle_yyyymmdd + cycle_hh,
                             '%Y%m%d%H')
    nwm_file_cycle_datetime_ep = \
        ndt.datetime_to_utc_epoch(nwm_file_cycle_datetime_dt)
    nwm_file_datetime_dt = nwm_file_cycle_datetime_dt - \
                           dt.timedelta(hours=nwm_file_time_minus_hours)
    nwm_file_datetime_ep = ndt.datetime_to_utc_epoch(nwm_file_datetime_dt)


    return nwm_file_cycle_type_str, \
           nwm_group, \
           nwm_file_time_minus_hours, \
           nwm_file_cycle_datetime_ep, \
           nwm_file_datetime_ep

def check_nwm_attributes(nwm, nwm_group, a_nwm_file_name):
    '''
    Check "model_output_type", "time" and "units" attributes
    of Dataset of a nwm file to be processed.
    '''
    # Get the "model_output_type" attribute, which must match
    # the name of a group in the database file.
    try:
        nwm_group_ = nwm.getncattr('model_output_type')
        if nwm_group_ != nwm_group:
            print('WARNING: Group "{}"'.format(nwm_group) +
                  'indicated by file name ' +
                  '{} '.format(a_nwm_file_name) +
                  'does not match ' +
                  '"model_output_type" attribute ' +
                  '"{}".'.format(nwm_group_),
                  file=sys.stderr)
            nwm_group = nwm_group_
    except:
        print('ERROR: No "model_output_type" attribute ' +
              'in NWM file {}.'.format(a_nwm_file_name),
              file=sys.stderr)
        nwm.close()
        return 0
    # Get time variables from the database file for this
    # group.

    # Get time information from the NWM file.
    try:
        nwm_var_time = nwm.variables['time']
    except:
        print('ERROR: NWM file ' +
              '{} '.format(a_nwm_file_name) +
              'has no "time" variable.',
              file=sys.stderr)
        nwm.close()
        return 0

    # Make sure the "time" variable in the NWM file varies along
    # only one dimension, and that the dimension is "time".
    if (len(nwm_var_time.dimensions) != 1 or
            nwm_var_time.dimensions[0] != 'time'):
        print('ERROR: NWM file ' +
              '{} '.format(a_nwm_file_name) +
              '"time" variable has unexpected structure; ' +
              'expecting a single dimension "time".',
              file=sys.stderr)
        nwm.close()
        return 0

    # Make sure the size of the "time" dimension in the NWM file
    # is exactly 1.
    if nwm.dimensions['time'].size != 1:
        print('ERROR: Time dimension in ' +
              '{} '.format(a_nwm_file_name) +
              'has size {}; '.format(nwm.dimensions['time'].size) +
              'expecting 1.',
              file=sys.stderr)
        nwm.close()
        return 0

    try:
        nwm_var_time_units = nwm_var_time.getncattr('units')
    except:
        print('ERROR: NWM file {} '.format(a_nwm_file_name) +
              '"time" variable has no "units" attribute.',
              file=sys.stderr)
        nwm.close()
        return 0
    try:
        nwm_datetime_dt = num2date(nwm_var_time[:][0],
                                units=nwm_var_time_units)
        nwm_datetime_ep = ndt.datetime_to_utc_epoch(nwm_datetime_dt)
    except:
        print('ERROR: Unable to convert time ' +
              '{} '.format(nwm_var_time[:][0]) +
              'to a datetime using units ' +
              '"{}".'.format(nwm_var_time_units),
              file=sys.stderr)
        nwm.close()
        return 0

    cycle_yyyymmdd = re.findall('[0-9]{8}', a_nwm_file_name)[0]
    cycle_hh = re.findall('\.t[0-9]{2}z\.', a_nwm_file_name)[0][2:-2]
    nwm_file_time_minus_hours = int(re.findall('\.tm[0-9]{2}\.',
                               a_nwm_file_name)[0][3:-1])
    nwm_file_cycle_datetime_dt = \
        dt.datetime.strptime(cycle_yyyymmdd + cycle_hh,
                             '%Y%m%d%H')
    nwm_file_datetime_dt = nwm_file_cycle_datetime_dt - \
                           dt.timedelta(hours=nwm_file_time_minus_hours)
    nwm_file_datetime_ep = ndt.datetime_to_utc_epoch(nwm_file_datetime_dt)
    if nwm_datetime_ep != nwm_file_datetime_ep:
        print('ERROR: NWM file {} '.format(a_nwm_file_name) +
              'refers to date/time {}; '.
               format(ndt.utc_epoch_to_string(nwm_datetime_ep)) +
              '; filename indicates ' + 'date/time {}.'.
               format(ndt.utc_epoch_to_string(nwm_file_datetime_ep)),
              file=sys.stderr)
        nwm.close()
        return 0

class File_info:
    def __init__(self, filename):
        self.filename = filename

    @staticmethod
    def cycle_yyyymmdd(filename):
        return re.findall('[0-9]{8}', filename)[0]
    def cycle_hh(filename):
        return re.findall('\.t[0-9]{2}z\.', filename)[0][2:-2]
    def time_minus_hours(filename):
        return int(re.findall('\.tm[0-9]{2}\.', filename)[0][3:-1])
    def datetime_dt(filename):
        cycle_yyyymmdd = File_info.cycle_yyyymmdd(filename)
        cycle_hh = File_info.cycle_hh(filename)
        return dt.datetime.strptime(cycle_yyyymmdd + cycle_hh, '%Y%m%d%H')

def create_temp_var_table(conn, nwm_var_col_name):
    '''
    Create a temporary table to hold data for a variable for one time step for
    all stations.
    '''
    #Create a temp table to hold values for each variable
    conn.execute("DROP TABLE IF EXISTS temp_var_val")
    try:
        conn.execute('CREATE TABLE temp_var_val (station_obj_identifier integer,' + \
                             nwm_var_col_name + ' real)')
    except:
        print('Error in creating table temp_var_val')

def check_var_units(conn, nwm_var, db_nwm_var_name):
    '''
    Check if units match between what's given in the database and
    what has in the file.
    '''
    try:
        nwm_var_db_unit = \
            conn.execute("SELECT units from nwm_meta WHERE nwm_var_name='" + \
                          db_nwm_var_name+ "'").fetchone()[0]
    except:
        print('WARNING: Variable "{}" '.
              format(db_nwm_var_name) +
              'in database has no "units" attribute. Skipping.',
              file=sys.stderr)
        return False
    try:
        nwm_var_unit = nwm_var.getncattr('units')
    except:
        print('WARNING: Variable "{}" '.
              format(nwm_var.name) +
              'in NWM file has no "units" attribute. Skipping.',
              file=sys.stderr)
        return False

    if nwm_var_unit != nwm_var_db_unit:
        print('WARNING: Variable "{}" '.
              format(nwm_var.name) +
              'in NWM file has units "{}" '.format(nwm_var_unit) +
              '; expecting "{}". '.format(nwm_var_db_unit) +
              'Skipping.',
              file=sys.stderr)
        return False
    else:
        return True

def identify_dim_pos_in_var(a_nwm_file_name,
                            db_var_dims,
                            nwm,
                            nwm_var,
                            nwm_var_name,
                            nwm_grid_num_rows,
                            nwm_grid_num_columns):
    '''
    Identify positions of dimensions in the database file variable.
    Identify positions of known dimensions in the NWM file variable.

    Verify that if the database file variable has a "layer"
    dimension, the NWM variable does as well, and vice
    versa, and that they are the same size.

    Make sure the set of NWM file dimension indices are
    like 0,1,2,... when sorted.
    '''
    db_num_z_dims, \
    db_z_dim_loc, \
    db_z_dim_name, \
    db_num_unknown_dims, \
    db_station_dim_loc, \
    db_time_dim_loc = \
       identify_dim_pos_in_db_var(db_var_dims, nwm_var_name)

    if db_num_z_dims > 1:
        print('ERROR: Variable "{}" '.format(nwm_var_name) +
              'in database file has too many "layer" dimensions.',
              file=sys.stderr)
        return 0
    if db_num_unknown_dims > 0:
        return 0
    if db_station_dim_loc is None:
        print('ERROR: Variable "{}" '.format(nwm_var_name) +
              'in database file does not use the "station" dimension.',
              file=sys.stderr)
        return 0
    if db_time_dim_loc is None:
        print('ERROR: Variable "{}" '.format(nwm_var_name) +
              'in database file does not use the "time" dimension.',
              file=sys.stderr)
        return 0

    nwm_num_z_dims, \
    nwm_z_dim_loc, \
    nwm_z_dim_name, \
    nwm_num_unknown_dims, \
    nwm_y_dim_loc, \
    nwm_x_dim_loc, \
    nwm_time_dim_loc = \
        identify_dim_pos_in_nwm_var(nwm,
                                    nwm_var,
                                    nwm_grid_num_rows,
                                    nwm_grid_num_columns,
                                    db_var_dims)

    if nwm_num_z_dims > 1:
        print('ERROR: Variable "{}" '.format(nwm_var.name) +
              'in NWM file {} '.format(a_nwm_file_name) +
              'has too many "layer" dimensions.')
        return 0
    if nwm_num_unknown_dims > 0:
        return 0
    if nwm_y_dim_loc is None:
        print('ERROR: Variable "{}" '.format(nwm_var.name) +
              'in NWM file {} '.format(a_nwm_file_name) +
              'does not use the "y" dimension.',
              file=sys.stderr)
        return 0
    if nwm_x_dim_loc is None:
        print('ERROR: Variable "{}" '.format(nwm_var.name) +
              'in NWM file {} '.format(a_nwm_file_name) +
              'does not use the "x" dimension.',
              file=sys.stderr)
        return 0
    if nwm_time_dim_loc is None:
        print('ERROR: Variable "{}" '.format(nwm_var.name) +
              'in NWM file {} '.format(a_nwm_file_name) +
              'does not use the "time" dimension.',
              file=sys.stderr)
        return 0

    ## Verify that if the database file variable has a "layer"
    ## dimension, the NWM variable does as well, and vice
    ## versa, and that they are the same size.
    if (db_z_dim_loc is not None) and \
       (nwm_z_dim_loc is None):
        print('ERROR: Variable "{}" '.format(nwm_var_name) +
              'in database ' +
              'has layer dimension "{}" '.format(db_z_dim_name) +
              'with no counterpart for corresponding ' +
              'variable "{}" '.format(nwm_var.name) +
              'in NWM file {}.'.format(a_nwm_file_name),
              file=sys.stderr)
              #'in database file {} '.format(temp_db_file) +
        return 0
    if (db_z_dim_loc is None) and \
       (nwm_z_dim_loc is not None):
        print('ERROR: Variable "{}" '.format(nwm_var.name) +
              'in NWM file {}.'.format(a_nwm_file_name) +
              'has layer dimension "{}" '.format(nwm_z_dim_name) +
              'with no counterpart for corresponding ' +
              'variable "{}" '.format(nwm_var_name) +
              'in database',
              file=sys.stderr)
              #'in database file {} '.format(temp_db_file),
        return 0

    nwm_dim_locs = [nwm_y_dim_loc,
                    nwm_x_dim_loc,
                    nwm_time_dim_loc]
    if nwm_z_dim_loc is not None:
        nwm_dim_locs.append(nwm_z_dim_loc)

    # Make sure the set of NWM file dimension indices are
    # like 0,1,2,... when sorted.
    if sorted(nwm_dim_locs) != list(range(len(nwm_dim_locs))):
        print('ERROR: Variable "{}" '.format(nwm_var.name) +
              'in NWM file {} '.format(a_nwm_file_name) +
              'has unaccounted-for dimensions.',
              file=sys.stderr)
        return 0



    return 1

def identify_dim_pos_in_db_var(db_var_dims,
                               nwm_var_name):
    '''
    Identify positions of dimensions in the database file variable.
    '''
    db_num_z_dims = 0
    db_z_dim_loc = None
    db_z_dim_name = None
    #db_z_dim_size = 0
    db_num_unknown_dims = 0
    db_station_dim_loc = None
    db_time_dim_loc = None
    #db_ensemble_dim_loc = None
    #db_sampling_dim_loc = None
    #for ndi, dim in enumerate(ncdb_var.dimensions):
    ndi = 0
    for dim in db_var_dims:
        ndi += 1
        if dim == 'station':
            db_station_dim_loc = ndi
        elif dim == 'time':
            db_time_dim_loc = ndi
        #elif dim == 'ensemble':
        #    db_ensemble_dim_loc = ndi
        #elif dim == 'sampling':
        #    db_sampling_dim_loc = ndi
        else:
            if "layer" in dim:
                db_z_dim_loc = ndi
                db_z_dim_name = dim
                #db_z_dim_size = db_var_dims.size
                #db_z_dim_size = len(db_var_dims)
                db_num_z_dims += 1
            else:
                db_num_unknown_dims += 1
                print('ERROR: Variable "{}" '.
                      format(nwm_var_name) +
                      'in database file has unsupported dimension "{}".'.
                      format(dim),
                      file=sys.stderr)
                      #'{} '.format(temp_db_file) +
    return db_num_z_dims, \
           db_z_dim_loc, \
           db_z_dim_name, \
           db_num_unknown_dims,\
           db_station_dim_loc, \
           db_time_dim_loc

def get_nwm_dim_time_loc(nwm_var):
    '''
    Get dimension locations info
    '''
    nwm_z_dim_loc = None
    nwm_y_dim_loc = None
    nwm_x_dim_loc = None
    for ndi, dim in enumerate(nwm_var.dimensions):
        if dim == 'y':
            nwm_y_dim_loc = ndi
        elif dim == 'x':
            nwm_x_dim_loc = ndi
        elif dim == 'time':
            nwm_time_dim_loc = ndi
        else:
            if "layer" in dim:
                nwm_z_dim_loc = ndi
    nwm_dim_time_locs = [nwm_x_dim_loc,
                         nwm_y_dim_loc,
                         nwm_z_dim_loc,
                         nwm_time_dim_loc]
    return nwm_dim_time_locs

def identify_dim_pos_in_nwm_var(nwm,
                                nwm_var,
                                nwm_grid_num_rows,
                                nwm_grid_num_columns,
                                db_var_dims):
    '''
    Identify positions of known dimensions in the NWM file variable.

    '''

    nwm_num_z_dims = 0
    nwm_z_dim_loc = None
    nwm_z_dim_name = None
    nwm_num_unknown_dims = 0
    nwm_y_dim_loc = None
    nwm_x_dim_loc = None
    nwm_time_dim_loc = None
    for ndi, dim in enumerate(nwm_var.dimensions):
        if dim == 'y':
            nwm_y_dim_loc = ndi
            if nwm.dimensions[dim].size != nwm_grid_num_rows:
                print('ERROR: Variable "{}" '.
                      format(nwm_var.name) +
                      'in NWM file has dimension "{}" '.format(dim) +
                      'with size {}; '.
                      format(nwm.dimensions[dim].size) +
                      'expecting {}.'.format(nwm_grid_num_rows),
                      file=sys.stderr)
                      #'in NWM file {} '.
                      #format(a_nwm_file_name) +
                #nwm_y_dim_ok = False
        elif dim == 'x':
            nwm_x_dim_loc = ndi
            if nwm.dimensions[dim].size != nwm_grid_num_columns:
                print('ERROR: Variable "{}" '.
                      format(nwm_var.name) +
                      'in NWM file has dimension "{}" '.format(dim) +
                      'with size {}; '.
                      format(nwm.dimensions[dim].size) +
                      'expecting {}.'.format(nwm_grid_num_columns),
                      file=sys.stderr)
                #nwm_x_dim_ok = False
        elif dim == 'time':
            nwm_time_dim_loc = ndi
            if nwm.dimensions[dim].size != 1:
                print('ERROR: Variable "{}" '.
                      format(nwm_var.name) +
                      'in NWM file has dimension "{}" '.format(dim) +
                      'with size {}; '.
                      format(nwm.dimensions[dim].size) +
                      'expecting 1.',
                      file=sys.stderr)
                #nwm_time_dim_ok = False
        else:
            if "layer" in dim:
                nwm_z_dim_loc = ndi
                nwm_z_dim_name = dim
                nwm_num_z_dims += 1
                # GF begin
                # 1. Verify ONE "_layer" dimension exists in
                #    db_var_dims
                # 2. Extract word "snow" or "soil" from "_layer"
                #    dimension listed in db_var_dims.
                # 3. Verify the same word "snow" or "soil" is
                #    part of dim.
                # GF end
                if "soil_layers" in dim:
                    if "soil_layer" not in db_var_dims:
                        print('ERROR: Variable "{}" '.
                              format(nwm_var.name) +
                              'in database file has no dimension soil_layer',
                              file=sys.stderr)
                              #format(temp_db_file) +
                        sys.exit(1)
                elif "snow_layers" in dim:
                    if "snow_layer" not in db_var_dims:
                        print('ERROR: Variable "{}" '.
                              format(nwm_var.name) +
                              'in database has no dimension snow_layer',
                              file=sys.stderr)
                              #format(temp_db_file) +
                        sys.exit(1)
                else:
                    print('ERROR: Variable "{}" '.
                          format(nwm_var.name) +
                          'in NWM file has unknown layer')
                    sys.exit(1)

            else:
                nwm_num_unknown_dims += 1
                print('ERROR: Variable "{}" '.
                      format(nwm_var.name) +
                      'in NWM file has unsupported dimension "{}".'.
                      format(dim),
                      file=sys.stderr)

    return nwm_num_z_dims, \
           nwm_z_dim_loc, \
           nwm_z_dim_name, \
           nwm_num_unknown_dims, \
           nwm_y_dim_loc, \
           nwm_x_dim_loc, \
           nwm_time_dim_loc

def new_nwm_grid_for_zc(zc, nwm_dim_time_locs,
                        nwm_var):
    '''Modified nwm_grid for each layer zc'''

    nwm_x_dim_loc = nwm_dim_time_locs[0]
    nwm_y_dim_loc = nwm_dim_time_locs[1]
    nwm_z_dim_loc = nwm_dim_time_locs[2]
    nwm_time_dim_loc = nwm_dim_time_locs[3]

    nwm_dim_locs = [nwm_y_dim_loc,
                    nwm_x_dim_loc,
                    nwm_time_dim_loc]
    if nwm_z_dim_loc is not None:
        nwm_dim_locs.append(nwm_z_dim_loc)

    # The nwm_slice_indices must match the order
    # of dimensions in the nwm_dim_locs list.
    nwm_slice_indices = [slice(None),
                         slice(None),
                         0]
    if nwm_z_dim_loc is not None:
        nwm_slice_indices.append(zc)

    # Zip, sort, and unzip the NWM lists. Note
    # that this will return tuples, not lists,
    # which is what we want for taking slices.
    zipped = zip(nwm_dim_locs, nwm_slice_indices)
    zipped = sorted(zipped, key=lambda nwm_lists:
                    nwm_lists[0], reverse=False)
    nwm_dim_locs, nwm_slice_indices = zip(*zipped)

    # Slice the grid out of the NWM file.
    # Note: 2019-05-09 this is the slowest step
    # by far of the updating process.
    #time_start = time.time()
    nwm_grid = nwm_var[nwm_slice_indices]
    ndv = nwm_var.getncattr('_FillValue')

    if nwm_var.name == 'ISNOW' and \
       np.all(nwm_var.valid_range == [0, 10]):

        # Override the default set_auto_mask behavior
        # of netCDF4 for this variable. The valid_range
        # value for ISNOW in NWM v2.0 files is [0,10],
        # but the correct valid_range is [-3,0].
        # By default, netCDF4 will mask any elements of
        # ISNOW that are outside the (incorrect)
        # valid_range, which will include -3, -2, and
        # -1. This section repairs that mask.

        bad_mask_ind = np.where((nwm_grid.mask is True) &
                                (nwm_grid.data != ndv))
        weird_mask_ind = \
            np.where((nwm_grid.mask is True) &
                     (nwm_grid.data != ndv) &
                     ((nwm_grid.data >=
                       nwm_var.valid_range[0]) &
                      (nwm_grid.data <=
                       nwm_var.valid_range[1])))

        if len(weird_mask_ind[0]) > 0:
            print('WARNING: unexpected masked data ' +
                  'in "{}" '.format(nwm_var.name),
                  file=sys.stderr)
                  #format(a_nwm_file_name),

        nwm_grid.mask[bad_mask_ind] = False

    # Transpose the nwm_grid if the y dimension
    # is not first.
    if nwm_dim_locs.index(nwm_y_dim_loc) > \
       nwm_dim_locs.index(nwm_x_dim_loc):
        nwm_grid = nwm_grid.transpose()

    return nwm_grid

def write_each_var_vals_to_temp_table(conn,
                                      df_obj_ids,
                                      df_time_ep,
                                      df_cycle_datetime_ep,
                                      df_cycle_type,
                                      df_result,
                                      df_zc,
                                      nwm_group,
                                      nwm_var_col_name,
                                      land_snow_layer_var_counter,
                                      land_soil_layer_var_counter,
                                      land_single_layer_var_counter,
                                      forcing_single_layer_var_counter):
    '''
    Write each variable's values (plus corresponding obj_identifiers)
    to the temp table which then will be attached later to form a temp
    table that contains all variable data for one time step/one nwm file.
    '''
    #write each variable data to a temp table for all stations
    statement_insert_var_val = "INSERT INTO temp_var_val VALUES (?,?)"
    conn.executemany(statement_insert_var_val,
                     (pd.concat((df_obj_ids, df_result),
                                axis=1)).values.tolist())

    if nwm_group == 'land':
        if ('_snow_' in nwm_var_col_name) and  ('_by_layer' in nwm_var_col_name):
            if land_snow_layer_var_counter == 1:
                insert_statement_layers = "INSERT INTO " + \
                                          "temp_land_snow_layer " + \
                                          " VALUES (?,?,?,?,?)"
                conn.executemany(insert_statement_layers,
                                 (pd.concat((df_obj_ids, df_time_ep,
                                             df_cycle_datetime_ep,
                                             df_cycle_type,
                                             df_zc), axis=1)).
                                             values.tolist())
        elif ('_soil_' in nwm_var_col_name) and  ('_by_layer' in nwm_var_col_name):
            if land_soil_layer_var_counter == 1:
                insert_statement_layers = "INSERT INTO " + \
                                          "temp_land_soil_layer " + \
                                          " VALUES (?,?,?,?,?)"
                conn.executemany(insert_statement_layers,
                                 (pd.concat((df_obj_ids, df_time_ep,
                                             df_cycle_datetime_ep,
                                             df_cycle_type,
                                             df_zc), axis=1)).
                                             values.tolist())
        else:
            if land_single_layer_var_counter == 1:
                insert_statement_layers = "INSERT INTO " + \
                                          "temp_land_single_layer " + \
                                          " VALUES (?,?,?,?)"
                conn.executemany(insert_statement_layers,
                                 (pd.concat((df_obj_ids, df_time_ep,
                                             df_cycle_datetime_ep,
                                             df_cycle_type),
                                             axis=1)).values.tolist())
    elif nwm_group == 'forcing':
        if forcing_single_layer_var_counter == 1:
            insert_statement_layers = "INSERT INTO " + \
                                      "temp_forcing_single_layer " + \
                                      " VALUES (?,?,?,?)"
            conn.executemany(insert_statement_layers,
                             (pd.concat((df_obj_ids, df_time_ep,
                                         df_cycle_datetime_ep,
                                         df_cycle_type), axis=1)).
                                         values.tolist())
    else:
        print('nwm_group {} has not been implemented.')
        sys.exit(1)

def attach_each_var_vals_to_temp_table(conn,
                                       nwm_group,
                                       nwm_var_col_name):
    '''
    Append each variable's values as new column to the temp table
    Common heading columns (ids, times, etc) have already been ready
    '''

    if nwm_group == 'land':
        if ('_snow_' in nwm_var_col_name) and  ('_by_layer' in nwm_var_col_name):
            conn.execute('ALTER TABLE temp_land_snow_layer ADD COLUMN '+ \
                          nwm_var_col_name + ' real')

            #Need to check row numbers are the same before updating
            #?????????  Actaully not. reshape earlier makes sure they are the same
            conn.execute("UPDATE temp_land_snow_layer SET " + nwm_var_col_name +\
                         "=(SELECT " + nwm_var_col_name + " FROM " +\
                         "temp_var_val WHERE temp_land_snow_layer.ROWID=" +\
                         "temp_var_val.ROWID)")
        elif ('_soil_' in nwm_var_col_name) and  ('_by_layer' in nwm_var_col_name):
            conn.execute('ALTER TABLE temp_land_soil_layer ADD COLUMN '+ \
                          nwm_var_col_name + ' real')

            #Need to check row numbers are the same before updating
            #?????????
            conn.execute("UPDATE temp_land_soil_layer SET " + nwm_var_col_name +\
                         "=(SELECT " + nwm_var_col_name + " FROM " +\
                         "temp_var_val WHERE temp_land_soil_layer.ROWID=" +\
                         "temp_var_val.ROWID)")
        else:
            conn.execute('ALTER TABLE temp_land_single_layer ADD COLUMN '+ \
                          nwm_var_col_name + ' real')

            #Need to check row numbers are the same before updating
            #?????????
            conn.execute("UPDATE temp_land_single_layer SET " + nwm_var_col_name +\
                         "=(SELECT " + nwm_var_col_name + " FROM " +\
                         "temp_var_val WHERE temp_land_single_layer.ROWID=" +\
                         "temp_var_val.ROWID)")
    elif nwm_group == 'forcing':
        conn.execute('ALTER TABLE temp_forcing_single_layer ADD COLUMN '+ \
                     nwm_var_col_name + ' real')
        #Need to check row numbers are the same before updating
        #?????????
        conn.execute("UPDATE temp_forcing_single_layer SET " + nwm_var_col_name +\
                     "=(SELECT " + nwm_var_col_name + " FROM " +\
                     "temp_var_val WHERE temp_forcing_single_layer.ROWID=" +\
                     "temp_var_val.ROWID)")
    else:
        print('The group {} has not been implemented yet.'.format(nwm_group))
        sys.exit(1)

def print_sample_station_loc(nwm_grid,
                             sample_ind,
                             db_grid_cols,
                             db_grid_rows):
    '''print out location info for a sample station'''

    #if nwm_var.name == 'ISNOW':
    #    print(sampling_method)
    #    print(np.amin(nwm_grid))
    #    print(np.amax(nwm_grid))

    #    print('{} - '.format(nwm_var_name) +
    #          '{} - '.format(sampling_method) +
    #          '@ "{}" '.format(sample_id) +
    #          '[{}]: '.format(sample_ind),
    #          result[sample_ind])

    col = db_grid_cols[:][sample_ind]
    row = db_grid_rows[:][sample_ind]
    i1 = np.floor(col).astype(int)
    i2 = i1 + 1
    j1 = np.floor(row).astype(int)
    j2 = j1 + 1
    gll = nwm_grid[j1, i1]
    glr = nwm_grid[j1, i2]
    gur = nwm_grid[j2, i2]
    gul = nwm_grid[j2, i1]
    di_left = col - i1
    di_right = i2 - col
    dj_bot = row - j1
    dj_top = j2 - row
    print(col, row)
    print(gll, glr, gur, gul)
    print(gll * di_right * dj_top +
          glr * di_left * dj_top +
          gur * di_left * dj_bot +
          gul * di_right * dj_bot)


def write_temp_data_to_database(conn,
                                nwm_group,
                                land_layer,
                                land_single_layer_col_names,
                                forcing_single_layer_col_names,
                                land_snow_layer_col_names=''):
                                #land_soil_layer_col_names='',
    '''
    write all variable data in one time step to final database.
    '''

    if nwm_group == 'land':
        if len(land_layer) != 0:
            #conn.execute("INSERT INTO land_soil.nwm_land_soil_layers (" + \
            #              land_soil_layer_col_names + \
            #             ") SELECT " + land_soil_layer_col_names + \
            #             " FROM temp_land_soil_layer")
            conn.execute("INSERT INTO land_snow.nwm_land_snow_layers (" + \
                          land_snow_layer_col_names + \
                         ") SELECT " + land_snow_layer_col_names + \
                         " FROM temp_land_snow_layer")
        conn.execute("INSERT INTO land_single.nwm_land_single_layer (" + \
                      land_single_layer_col_names + \
                     ") SELECT " + land_single_layer_col_names + \
                     " FROM temp_land_single_layer")
#        temp_db_conn.commit()
    elif nwm_group == 'forcing':
        conn.execute("INSERT INTO forcing_single.nwm_forcing_single_layer (" + \
                      forcing_single_layer_col_names + \
                     ") SELECT " + forcing_single_layer_col_names + \
                     " FROM temp_forcing_single_layer")
#        temp_db_conn.commit()
    else:
        print('This group {} has not been implemented.'.format(nwm_group))
        sys.exit(1)

def update_nwm_file_update_info(conn,
                                nf_name,
                                nwm_cycle_type_ext_ana,
                                nwm_cycle_type_ana,
                                oper):
    '''
    Update nwm_file_update_info table and delete some old
    and not 'best' data.
    '''

    # Abbreviations: "nf" = "NWM file"
    #                "ep" = "epoch" datetime
    #                "dt" = "datetime" type
    nf_cycle_type_str, \
        nf_group, \
        nf_time_minus_hours, \
        nf_cycle_datetime_ep, \
        nf_datetime_ep = \
        get_nwm_file_info(nf_name)

    if nf_cycle_type_str == 'analysis_assim_extend':
        nf_cycle_type = nwm_cycle_type_ext_ana
    elif nf_cycle_type_str == 'analysis_assim':
        nf_cycle_type = nwm_cycle_type_ana

    # Identify reference data for the current nf_datetime_ep and nf_group.
    ref_nwm_file_name, ref_nwm_cycle_datetime_db_ep, \
        ref_time_minus_hours_db, ref_cycle_type_db = \
        ref_nwm_file_update_info(conn,
                                 nf_datetime_ep,
                                 nf_group)

    sql_insert = "INSERT INTO nwm_file_update_info " + \
                 "VALUES (?, ?, ?, ?, ?, ?, ?)"

    #for the current nwm_file_datetime that have the same 'land' or 'forcing' category
    #and have is_reference = 1

    if ref_nwm_file_name is None:

        # No reference files have been defined for the current
        # nf_datetime_ep, for the current nf_group.
        is_reference = 1
        # print('INFO: {} provides initial group "{}" reference data for {}.'.
        #       format(nf_name,
        #              nf_group,
        #              ndt.utc_epoch_to_string(nf_datetime_ep)))
        try:
            conn.execute(sql_insert, (nf_name,
                                      nf_datetime_ep,
                                      nf_cycle_datetime_ep,
                                      nf_time_minus_hours,
                                      nf_cycle_type,
                                      nf_group,
                                      is_reference))
        except:
            print('ERROR: Error in updating nwm_file_update_info table',
                  file=sys.stderr)
            sys.exit(1)

    else:

        # Reference data has been defined for the current nf_datetime_ep,
        # for the current nf_group.
        # print('Further Update for {}'.
        #       format(ndt.utc_epoch_to_string(nf_datetime_ep)))
        # print('INFO: {} provides additional group "{}" data for {}.'.
        #       format(nf_name,
        #              nf_group,
        #              ndt.utc_epoch_to_string(nf_datetime_ep)))
        if (nf_cycle_type < ref_cycle_type_db) or \
           ((nf_cycle_type == ref_cycle_type_db) and \
            (nf_time_minus_hours > ref_time_minus_hours_db)):

            # New data is a better reference than existing data.
            # print('Case 1 - inserting')
            print('INFO: {} provides improved reference for {}'.
                  format(nf_name,
                         ndt.utc_epoch_to_string(nf_datetime_ep)))
            is_reference = 1
            conn.execute(sql_insert, (nf_name,
                                      nf_datetime_ep,
                                      nf_cycle_datetime_ep,
                                      nf_time_minus_hours,
                                      nf_cycle_type,
                                      nf_group,
                                      is_reference))

            # Set is_reference = 0 for previous is_reference = 1 entry in
            # nwm_file_update_info for this nf_datetime_ep and this
            # nf_group.
            # GF - do we need to add the nf_group to this? Seems like it will
            # wipe out records for two files with this arrangement, instead of
            # one.
            # print('Case 1 - updating')
            sql_update = "UPDATE nwm_file_update_info " + \
                         "SET is_reference=0 WHERE " + \
                         "cycle_datetime=" + \
                         str(ref_nwm_cycle_datetime_db_ep) + " " + \
                         "AND time_minus_hours=" + \
                         str(ref_time_minus_hours_db) + " " + \
                         "AND cycle_type=" + \
                         str(ref_cycle_type_db) + " " + \
                         "AND nwm_group='" + \
                         str(nf_group) + "' " + \
                         "AND is_reference=1"
            print(sql_update)
            conn.execute(sql_update)

            if oper is False and ref_time_minus_hours_db != 0:

                # Remove now-obsolete is_reference = 1 data from data tables.
                print('Need to delete some old <is_reference = 1> data')
                if nf_group == 'forcing':
                    table_name = 'forcing_single.nwm_forcing_single_layer'
                elif nf_group == 'land':
                    table_name = 'land_single.nwm_land_single_layer'
                else:
                    print('ERROR: Group "{}" is not supported at this time.'.
                          format(nf_group),
                          file=sys.stderr)
                    sys.exit(1)
                sql_count = "SELECT COUNT(*) FROM " + table_name + " " + \
                            "WHERE cycle_datetime=" + \
                            str(ref_nwm_cycle_datetime_db_ep) + " " + \
                            "AND datetime=" + \
                            str(nf_datetime_ep) + " " + \
                            "AND cycle_type=" + str(ref_cycle_type_db)
                sql_del = "DELETE from " + table_name + " " + \
                          "WHERE cycle_datetime=" + \
                          str(ref_nwm_cycle_datetime_db_ep) + " " + \
                          "AND datetime=" + \
                          str(nf_datetime_ep) + " " + \
                          "AND cycle_type=" + str(ref_cycle_type_db)
                db_output = conn.execute(sql_count).fetchone()
                print('Before deleting: ----{}----entries'.format(db_output[0]))
                #time.sleep(10)

                # Delete former is_reference = 1 data from archive database.
                conn.execute(sql_del)

                db_output = conn.execute(sql_count).fetchone()
                print('After deleting: ----{}----entries'.format(db_output[0]))

        else:

            # Existing data is a better reference than new data.
            #if (oper is True) or (nwm_time_minus_hour == 0):
            # print('INFO: {} is non-reference for {} but qualifies for addition'.
            #       format(nf_name,
            #              ndt.utc_epoch_to_string(nf_datetime_ep)))
            if (oper is True) or (nf_time_minus_hours == 0):
                is_reference = 0
                conn.execute(sql_insert, (nf_name,
                                          nf_datetime_ep,
                                          nf_cycle_datetime_ep,
                                          nf_time_minus_hours,
                                          nf_cycle_type,
                                          nf_group,
                                          is_reference))
            else:
                print("WARNING: we should never have processed something that has " +
                      "is_reference = 0 and " +
                      "time_minus_hours = {} ".format(nf_time_minus_hours) +
                      "to an 'archive' type of database",
                      file=sys.stderr)

def update_databases_info(conn,
                          new_db_start_datetime_ep,
                          new_db_finish_datetime_ep):
    '''Update databases_info table with new start/finish datetimes'''
    # Update the time stamps for the table 'databases_info'
    conn.execute("UPDATE databases_info SET start_date=(?)",
                  (new_db_start_datetime_ep, ))

    conn.execute("UPDATE databases_info SET finish_date=(?)",
                  (new_db_finish_datetime_ep, ))
    #if oper is True:
    #    new_db_start_datetime_str = new_db_start_datetime.strftime('%s')
    #    #new_db_start_datetime_str = new_db_start_datetime.strftime('%Y-%m-%d %H:00:00')
    #    temp_db_conn.execute("UPDATE databases_info SET start_date=(?)",
    #                         (new_db_start_datetime_str, ))

    #    new_db_finish_datetime_str = new_db_finish_datetime.strftime('%s')
    #    temp_db_conn.execute("UPDATE databases_info SET finish_date=(?)",
    #                         (new_db_finish_datetime_str, ))

    latest_time_ep = time.time()
    conn.execute("UPDATE databases_info SET last_updated_date=(?)",
                  (latest_time_ep, ))


def copy_temporary_databases(db_path,
                             db_file,
                             db_dir,
                             suffix,
                             forcing_single_db,
                             land_single_db,
                             oper):
    '''
    Make temporary databases by coping original databases
    and then open and attach them.
    '''
    # Create a copy of the database file.

    temp_db_file = db_file + '.' + suffix
    temp_db_path = os.path.join(db_dir, temp_db_file)
    #temp_db_path = os.path.join(opt.safe_dir, temp_db_file)
    try:
        shutil.copy(db_path, temp_db_path)
    except:
        print('ERROR: Failed to make temporary copy of ' +
              '{} '.format(db_path) +
              ' as {}.'.format(temp_db_path),
              file=sys.stderr)
        sys.exit(1)

    print('\nINFO: Modifying copy of {} '.format(os.path.split(db_path)[1]) +
          'as {}.'.format(os.path.split(temp_db_path)[1]))
    # Open the copy.
    try:
        temp_db_conn = sqlite3.connect(temp_db_path,
           detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
        temp_db_cur = temp_db_conn.cursor()
    except sqlite3.OperationalError:
        print('Error in open database file', temp_db_path)

    #To attach and operate on temp databases
    # database existance has been checked earlier
    temp_forcing_single_db = forcing_single_db + '.' + suffix
    temp_forcing_single_db_path = os.path.join(db_dir, temp_forcing_single_db)
    shutil.copy(forcing_single_db, temp_forcing_single_db_path)
    temp_db_conn.execute('ATTACH DATABASE "' + temp_forcing_single_db_path + \
                         '" AS forcing_single')

    temp_land_single_db = land_single_db + '.' + suffix
    temp_land_single_db_path = os.path.join(db_dir, temp_land_single_db)
    shutil.copy(land_single_db, temp_land_single_db_path)
    temp_db_conn.execute('ATTACH DATABASE "' + temp_land_single_db_path + \
                         '" AS land_single')

    ## BELOW ARE COMMENTED FOR NOW, NEED MODIFY THEM IF layer INCLUDED
    ## NEED PASS: land_soil_db, land_snow_db, safe_dir
    #if len(land_layer) != 0:
    #    temp_land_soil_db = land_soil_db + '.' + suffix
    #    temp_land_soil_db_path = os.path.join(safe_dir, temp_land_soil_db)
    #    shutil.copy(land_soil_db, temp_land_soil_db_path)
    #    temp_db_conn.execute('ATTACH DATABASE "' + temp_land_soil_db_path + \
    #                         '" AS land_soil')

    #    temp_land_snow_db = land_snow_db + '.' + suffix
    #    temp_land_snow_db_path = os.path.join(safe_dir, temp_land_snow_db)
    #    shutil.copy(land_snow_db_path, temp_land_snow_db_path)
    #    temp_db_conn.execute('ATTACH DATABASE "' + temp_land_snow_db_path + \
    #                         '" AS land_snow')
    ## ABOVE ARE COMMENTED FOR NOW, NEED MODIFY THEM IF layer INCLUDED
    ## NEED PASS: land_soil_db, land_snow_db, safe_dir

    #This may help to speed up the execution of the code but
    #data could be lost if power failure or system crash occurs
    temp_db_conn.execute('PRAGMA synchronous=OFF')

    return temp_db_conn, temp_db_cur

def rename_temp_databases(db_dir,
                          base_db_file,
                          forcing_single_db,
                          land_single_db,
                          suffix):
                          #land_soil_db='',
                          #land_snow_db=''):
    '''Rename temp databases to regular database file names'''

    temp_db_file = base_db_file + '.' + suffix
    temp_db_path = os.path.join(opt.db_dir, temp_db_file)
    base_db_path = os.path.join(opt.db_dir, base_db_file)
    #print('\nINFO: Renaming {} '.format(temp_db_path) +
    #      ' as {}.'.format(base_db_path))
    print('\nINFO: Renaming {} '.format(temp_db_file) +
          'as {}.'.format(base_db_file))
    shutil.move(temp_db_path, base_db_path)

    temp_forcing_single_db = forcing_single_db + '.' + suffix
    temp_forcing_single_db_path = os.path.join(db_dir,
                                               temp_forcing_single_db)
    print('\nINFO: Renaming {} '.format(os.path.split(temp_forcing_single_db)[1]) +
          'as {}.'.format(os.path.split(forcing_single_db)[1]))
    shutil.move(temp_forcing_single_db_path, forcing_single_db)

    temp_land_single_db = land_single_db + '.' + suffix
    temp_land_single_db_path = os.path.join(db_dir,
                                            temp_land_single_db)
    print('\nINFO: Renaming {} '.format(os.path.split(temp_land_single_db)[1]) +
          'as {}.'.format(os.path.split(land_single_db)[1]))
    shutil.move(temp_land_single_db_path, land_single_db)
    #if len(land_layer) != 0:
    #    print('\nINFO: Renaming {} '.format(temp_land_soil_db_path) +
    #          ' as {}.'.format(land_soil_db))
    #    shutil.move(temp_land_soil_db_path, land_soil_db)
    #    print('\nINFO: Renaming {} '.format(temp_land_snow_db_path) +
    #          ' as {}.'.format(land_snow_db))
    #    shutil.move(temp_land_snow_db_path, land_snow_db)


def change_datetime_to_readable_string(datetime_df,
                                       column_name):
    '''
    Change time column of a dataframe in epoch seconds
    to readable time string in yyyy-mm-dd hh:00
    '''
    str_list = []
    for each_v in datetime_df:
        each_str = ndt.utc_epoch_to_string(each_v, '%Y-%m-%d %H:%M')
        str_list.append(each_str)
    str_series = pd.Series(str_list)
    df_str = str_series.to_frame(column_name)
    datetime_df = df_str
    return datetime_df


def get_station_latlon_obj_ids_times(conn, db_file):
    '''
    Get station lat/lon and obj_identifier info from stations
    '''
    # Read NWM grid row/column of stations.
    try:
        db_grid_cols = conn.execute('SELECT nwm_grid_column FROM stations').fetchall()
        db_grid_rows = conn.execute('SELECT nwm_grid_row FROM stations').fetchall()
    except:
        print('ERROR: Failing to get grid col/row data from ' +
              db_file + '.', file=sys.stderr)

    # Get obj_identifier of all stations.
    try:
        obj_ids = conn.execute("SELECT obj_identifier FROM stations").fetchall()
        #print('GF03 ', type(obj_ids[0]))

    except:
        print('ERROR: Failing to get obj_identifier data from ' +
              db_file + '.', file=sys.stderr)

    # Get stop_datetime and start_datetime for each station
    try:
        db_start_dates_ep = conn.execute("SELECT start_date" + \
                                         " FROM stations").fetchall()
        db_stop_dates_ep = conn.execute("SELECT stop_date" + \
                                         " FROM stations").fetchall()
    except:
        print('ERROR: Failing to get start/stop dates from ' +
              db_file + '.', file=sys.stderr)

    return db_grid_cols, db_grid_rows, obj_ids, \
           db_start_dates_ep, db_stop_dates_ep


def get_sample_station_info(conn, sample_id):
    '''
    Get sample station lat/lon and obj_identifier info from stations
    '''
    try:
        sample_grid_col_row = \
            conn.execute("SELECT nwm_grid_column, " + \
                         "nwm_grid_row FROM stations " + \
                         "WHERE id='" + sample_id + "'").fetchone()
    except:
        print('No sample_grid_col_row obtained')
    try:
        sample_obj_id = conn.execute("SELECT obj_identifier FROM stations " + \
                            "WHERE id='" + sample_id + "'").fetchone()
    except:
        print('No sample_obj_id obtained')
    try:
        sample_ind = \
            conn.execute("SELECT rowid FROM stations " + \
                         "WHERE id='" + sample_id + "'").fetchone()[0] - 1
        #sample_ind = temp_db_cur.fetchone()[0] - 1  # because rowid starts at 1
    except:
        print('\nNo sample_ind obtained.')

    #print('Sample station obj_id and col/row:', sample_obj_id, sample_grid_col_row)
    if sample_grid_col_row is None:
        sample_ind = None

    return sample_grid_col_row, sample_obj_id, sample_ind

def delete_older_data(conn, new_db_start_datetime_ep):
    '''
    Delete those data/files that are older than new_db_start_datetime
    '''
    conn.execute("DELETE FROM forcing_single.nwm_forcing_single_layer " + \
                 "WHERE datetime < " + str(new_db_start_datetime_ep))
    conn.execute("DELETE FROM land_single.nwm_land_single_layer " + \
                 "WHERE datetime < " + str(new_db_start_datetime_ep))
    # Also delete file list from nwm_file_update_info
    conn.execute("DELETE FROM nwm_file_update_info " + \
                 "WHERE datetime < " + str(new_db_start_datetime_ep))



def write_dataframe_to_database(sqlite_conn,
                                table_name,
                                column_names,
                                num_cols,
                                num_rows,
                                data_df):

    '''
    Write array of pandas dataframe data to sqlite
    database. Specifically write station metadata to
    the stations table in the base database
    '''
    num_stations_added = 0
    num_out_of_bounds = 0
    for data_row in data_df.itertuples():
        progress(data_row.Index, len(data_df),
                 status='')
        if data_row.nwm_grid_column < 0 or \
           data_row.nwm_grid_column >= \
           num_cols or \
           data_row.nwm_grid_row < 0 or \
           data_row.nwm_grid_row >= \
           num_rows:
            num_out_of_bounds += 1
            continue
        data_values = list()
        for col_name in column_names:
            data_col_val = eval('data_row.{}'.format(col_name))
            #if isinstance(data_col_val, str):
            #    data_col_val = data_col_val.strip()
            #NOTE: EXTRA SPACES HAVE BEEN STRIPPED DURING THE QUERY

            if 'date' in col_name:
                #print('debug:', type(data_col_val), data_col_val, col_name)
                #if isinstance(data_col_val, int):
                #    data_values.append(data_col_val)
                #else:
                #    dv = data_col_val.strftime('%Y-%m-%d %H:%M:%S')
                #    data_values.append(ndt.string_to_utc_epoch(dv))
                dv = data_col_val.strftime('%Y-%m-%d %H:%M:%S')
                data_values.append(ndt.string_to_utc_epoch(dv))
            else:
                data_values.append(data_col_val)

        wildcards = ','.join(['?'] * len(column_names))
        #print('debug:',len(data_values))
        #print('debug:',data_values)
        sqlite_conn.execute("INSERT INTO " + table_name + \
                           " VALUES ("+ wildcards +")", data_values)
        num_stations_added += 1

    #print('Number of stations skipped: ', num_out_of_bounds)
    sqlite_conn.commit()

    return num_stations_added, num_out_of_bounds


def sample_grid_at_points(grid, rowlist, collist,
                          fill_value=None,
                          method='bilinear',
                          measure_wall_times=False):

    '''
    Sample grid value at a specified lat/lon location
    '''
    if fill_value is None:
        if np.issubdtype(grid.dtype, np.integer):
            fill_value = np.iinfo(grid.dtype).min
        elif np.issubdtype(grid.dtype, np.floating):
            fill_value = np.finfo(grid.dtype).min
        else:
            print('ERROR: Unsupported type "{}".'.format(grid.dtype),
                  file=sys.stderr)
            return None

    #print('Actual sampling_method: <{}>'.format(method))
    #print(type(rowlist),type(collist))
    row = np.asarray(rowlist, dtype=np.float32)
    col = np.asarray(collist, dtype=np.float32)
    #print(type(row),type(col))
    #print(rowlist[0:5], row[0:5])
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
    if len(row) != len(col):
        print('ERROR: Grid row and col arrays must have the same shape.',
              file=sys.stderr)
        return None

    if measure_wall_times is True:
        time_start = time.time()
        full_time_start = time_start

    # Create "out" which is the same shape as col and row and the same
    # type as grid.
    # out = np.full_like(row, dtype=grid.dtype, fill_value=fill_value)
    #out[:] = np.ma.masked
    out = np.ma.masked_values(np.full_like(row,
                                           dtype=grid.dtype,
                                           fill_value=fill_value),
                              fill_value)

    if measure_wall_times is True:
        time_finish = time.time()
        print('INFO: Created masked output array in {} seconds.'.
              format(time_finish - time_start))
        time_start = time.time()

    if method == 'bilinear':

        i1 = np.floor(col).astype(int)
        i2 = i1 + 1
        j1 = np.floor(row).astype(int)
        j2 = j1 + 1

        in_bounds = np.where((i1 >= 0) &
                             (i2 < num_cols) &
                             (j1 >= 0) &
                             (j2 < num_rows))

    else:

        i = np.round(col).astype(int)
        j = np.round(row).astype(int)

        in_bounds = np.where((i >= 0) &
                             (i < num_cols) &
                             (j >= 0) &
                             (j < num_rows))

    count = len(in_bounds[0])
    if count == 0:
        print('WARNING: All grid row and col values are out of bounds.',
              file=sys.stderr)
        return None

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

        #print(type(in_bounds), len(in_bounds), in_bounds[0])
        di_left = col[in_bounds] - i1[in_bounds]
        di_right = i2[in_bounds] - col[in_bounds]
        dj_bot = row[in_bounds] - j1[in_bounds]
        dj_top = j2[in_bounds] - row[in_bounds]

        # print('xxx', out[5917])
        # print('xxx', out.mask[5917])
        out[in_bounds] = gll * di_right * dj_top + \
                         glr * di_left * dj_top + \
                         gur * di_left * dj_bot + \
                         gul * di_right * dj_bot

        # k = np.where(in_bounds[0] == 5917)
        # k = k[0][0]
        # print('xxx', gll[k], glr[k], gur[k], gul[k])
        # print('xxx', out[5917])
        # print('xxx', out.mask[5917])

    else:

        out[in_bounds] = grid[j[in_bounds], i[in_bounds]]

    if measure_wall_times is True:
        time_finish = time.time()
        print('INFO: Performed {} sampling '.format(method) +
              'in {} seconds.'.format(time_finish - time_start))
        full_time_finish = time_finish
        print('INFO: Full sample_grid_at_points time ' +
              '{} seconds.'.format(full_time_finish - full_time_start))

    return out


def update_nwm_db_allstation(db_file,
                             db_start_datetime_ep,
                             db_end_datetime_ep,
                             sqldb_conn):

    """
    Update the allstation information (from the webdb on wfs0) in a
    NWM database file.
    """

    # Prepare to update the station information in the database file.
    print('INFO: Updating allstation information for {}'.
           format(os.path.split(db_file)[1]))

    # Read the station dimension from the database file.
    try:
        sqldb_cur = sqldb_conn.cursor()
        statement = 'SELECT COUNT(DISTINCT obj_identifier) FROM stations'
        sqldb_cur.execute(statement)
    except:
        print('ERROR: Database file {} '.format(db_file) +
              'has no information regarding the obj_identifier.',
              file=sys.stderr)
        return None
    db_num_stations_start = sqldb_cur.fetchone()[0]
    if db_num_stations_start is None:
        db_num_stations_start = 0
    print('\nINFO: station database currently has {} stations'.
          format(db_num_stations_start))


    # get bounding box info and other values from the coordinate_system table
    result = sqldb_conn.execute("PRAGMA table_info('coordinate_system')").fetchall()
    column_names = list(zip(*result))[1]
    default_value = list(zip(*result))[4]
    for col in range(0, len(result)):
        if column_names[col] == 'standard_parallel_lat1':
            lat_sec_1 = float(default_value[col])
        if column_names[col] == 'standard_parallel_lat2':
            lat_sec_2 = float(default_value[col])
        if column_names[col] == 'latitude_of_projection_origin':
            lat_d = float(default_value[col])
        if column_names[col] == 'longitude_of_central_meridian':
            lon_v = float(default_value[col])
        if column_names[col] == 'earth_radius':
            earth_radius_m = float(default_value[col])
        if column_names[col] == 'false_easting':
            false_easting = float(default_value[col])
        if column_names[col] == 'false_northing':
            false_northing = float(default_value[col])
        if column_names[col] == 'x_resolution_meters':
            dx = float(default_value[col])
        if column_names[col] == 'y_resolution_meters':
            dy = float(default_value[col])
        if column_names[col] == 'x_left_center':
            x_left_center = float(default_value[col])
        if column_names[col] == 'y_bottom_center':
            y_bottom_center = float(default_value[col])
        if column_names[col] == 'number_of_rows':
            number_of_rows = int(default_value[col])
        if column_names[col] == 'number_of_columns':
            number_of_columns = int(default_value[col])
        if column_names[col] == 'bounding_box_minimum_longitude':
            bb_min_lon = float(default_value[col])
        if column_names[col] == 'bounding_box_maximum_longitude':
            bb_max_lon = float(default_value[col])
        if column_names[col] == 'bounding_box_minimum_latitude':
            bb_min_lat = float(default_value[col])
        if column_names[col] == 'bounding_box_maximum_latitude':
            bb_max_lat = float(default_value[col])


    # Define the geodetic system and the NWM grid as pyproj objects. These
    # will be used to transform station locations in longitude/latitude to
    # x/y (and NWM column/row) values.

    proj_geo = pyproj.Proj(proj='longlat',
                           R=earth_radius_m)

    proj_nwm = pyproj.Proj(proj='lcc',
                           R=earth_radius_m,
                           lon_0=lon_v,
                           lat_0=lat_d,
                           lat_1=lat_sec_1,
                           lat_2=lat_sec_2,
                           x_0=false_easting,
                           y_0=false_northing)

    if db_num_stations_start > 0:
        sqldb_cur.execute('SELECT obj_identifier from stations')
        sqldb_obj_ids = sqldb_cur.fetchall()
        max_obj_id = (max(sqldb_obj_ids))[0]   # from tuple to a value
    else:
        # By convention the obj_identifier is always > 0.
        max_obj_id = -1
    print('max_obj_id =', max_obj_id)

    # Open the web database.
    #web_conn_string = "host='wdb0' dbname='web_data'"
    web_conn_string = "host='wdb0.dmz.nohrsc.noaa.gov' dbname='web_data'"
    web_conn = psycopg2.connect(web_conn_string)
    web_conn.set_client_encoding("utf-8")
    web_cursor = web_conn.cursor()

    # Generate a list of point.allstation columns to retrieve, and
    # corresponding column names for the resulting pandas dataframe
    # that will be created.

    # Generate separate but corresponding column name lists for the sqlite
    # database "station" table and the web database "allstation" table.
    name_list = sqldb_conn.execute('SELECT * FROM station_meta'). \
                                            fetchall()
    db_column_names = map(lambda x: x[0], name_list)
    sqldb_column_names = list(db_column_names)
    wdb_column_names = map(lambda x: x[1], name_list)
    wdb_column_names = list(wdb_column_names)
    data_column_types = map(lambda x: x[2], name_list)
    data_column_types = list(data_column_types)

    wdb_selection_str = \
        form_web_query_string(sqldb_column_names,
                              wdb_column_names,
                              data_column_types,
                              need_as=True)
    group_str = \
        form_web_query_string(sqldb_column_names,
                              wdb_column_names,
                              data_column_types)

    snow_start_datetime_ep = db_end_datetime_ep - 365*24*3600
    snow_start_datetime_ep = min(snow_start_datetime_ep,
                                 db_start_datetime_ep)
    snow_end_datetime_ep = db_end_datetime_ep
    #print(wdb_selection_str_new)
    sql_meta_with_snow = \
        "SELECT " + wdb_selection_str + \
        " FROM " + \
        "(" + \
            "SELECT obj_identifier, date, value " + \
            "FROM point.obs_snow_depth " + \
            "WHERE date >= '" + \
             ndt.utc_epoch_to_string(snow_start_datetime_ep) + "' " + \
            "AND date <= '" + \
             ndt.utc_epoch_to_string(snow_end_datetime_ep) +"' " + \
            "AND value IS NOT NULL " + \
        ") AS tsd " + \
        "FULL JOIN" + \
        "(" + \
            "SELECT obj_identifier, date, value " + \
            "FROM point.obs_swe " + \
            "WHERE date >= '" + \
             ndt.utc_epoch_to_string(snow_start_datetime_ep) + "' " + \
            "AND date <= '" + \
             ndt.utc_epoch_to_string(snow_end_datetime_ep) +"' " + \
            "AND value IS NOT NULL " + \
        ") AS tswe USING (date, obj_identifier) " + \
        "JOIN point.allstation AS tstn USING (obj_identifier) " + \
        "WHERE tstn.coordinates[0]" + \
             " >= {} ".format(bb_min_lon) + \
             "AND tstn.coordinates[0]" + \
             " <= {} ".format(bb_max_lon) + \
             "AND tstn.coordinates[1]" + \
             " >= {} ".format(bb_min_lat) + \
             "AND tstn.coordinates[1]" + \
             " <= {} ".format(bb_max_lat) + \
             "AND (tstn.stop_date = '1900-01-01 00:00:00' " + \
             "OR tstn.stop_date >= '" + \
             ndt.utc_epoch_to_string(db_start_datetime_ep) + \
             "') " + \
             "AND (tstn.start_date = '1900-01-01 00:00:00' " + \
             "OR tstn.start_date <= '" + \
              ndt.utc_epoch_to_string(db_end_datetime_ep) + \
             "') " + \
             "AND tstn.use = TRUE " + \
        "GROUP BY " + group_str + \
        " HAVING COUNT(*) > 0 " + \
        "ORDER BY tstn.obj_identifier;"

    print('\nINFO: psql command "{}"'.format(sql_meta_with_snow))

    # Set this_station_update_datetime to the current system time.
    # This should be done just before reading the allstation table.
    #this_station_update_datetime = dt.datetime.utcnow()
    this_station_update_datetime_ep = time.time()

    web_cursor.execute(sql_meta_with_snow)
    # allstation is just a huge list of tuples.
    allstation = web_cursor.fetchall()
    wdb_df = pd.DataFrame(allstation, columns=sqldb_column_names)
    finish_station_update_datetime_ep = time.time()
    print('\nINFO: Time spending on query is {} minutes.'.format
          (round((finish_station_update_datetime_ep-this_station_update_datetime_ep)/
            60), 2))


    print('\nThe dataframe from web database wdb_df has a dimension of', wdb_df.shape)
    print('INFO: Total snow stations obtained from the web database is {}.'.format(len(wdb_df)))

    print('\nSnow station selection was base on the period: \nfrom {} to {}.'.format(
          ndt.utc_epoch_to_string(snow_start_datetime_ep),
          ndt.utc_epoch_to_string(snow_end_datetime_ep)))


    # Calculate NWM grid row/column for each station and attach them
    # to the queried dataframe.
    # The to_numpy method converts pandas series to numpy arrays.
    lon = wdb_df['longitude'].to_numpy()
    lat = wdb_df['latitude'].to_numpy()
    x, y = pyproj.transform(proj_geo, proj_nwm, lon, lat)
    db_grid_col = (x - x_left_center) / dx
    db_grid_row = (y - y_bottom_center) / dy
    wdb_df['nwm_grid_column'] = db_grid_col
    wdb_df['nwm_grid_row'] = db_grid_row


    print('\nUpdating station meta data if necessary')

    #print('wdb_df:', wdb_df['vendor_date'][0:100])

    num_stations_added = 0
    #num_skipped = 0
    num_out_of_bounds = 0

    sqldb_column_names.append('nwm_grid_column')
    sqldb_column_names.append('nwm_grid_row')
    if db_num_stations_start <= 0:
        print('Process the station metadata for the first time.\n')
        total_stations, num_out_of_bounds = \
                         write_dataframe_to_database(sqldb_conn,
                                                     'stations',
                                                     sqldb_column_names,
                                                     number_of_columns,
                                                     number_of_rows,
                                                     wdb_df)

    else:
        print('Update the station metadata ...\n')
        #Update the station meta data with all from wdb_df plus what's left
        #in sqlite database (those in sqldb but not in wdb_df)
        ##########################
        print('Comparing data from web and the sqlite databases...')
        sqldb_cur.execute('SELECT * from stations')
        sqldb_lst = sqldb_cur.fetchall()
        #sqldb_column_names.append('nwm_grid_column')
        #sqldb_column_names.append('nwm_grid_row')
        sqldb_df = pd.DataFrame(sqldb_lst, columns=sqldb_column_names)
        filter_sql_in_wdb = sqldb_df['obj_identifier'].isin(wdb_df['obj_identifier'])
        filter_extra_in_sqldb = ~filter_sql_in_wdb

        #id_filter = ~np.isin(wdb_df['obj_identifier'].values, sqldb_obj_ids[:])
        #ids_in_sqldb_filter = np.isin(sqldb_obj_ids[:], wdb_df['obj_identifier'].values)
        #ids_left_in_sqldb_filter = ~ids_in_sqldb_filter

        combined_df = pd.concat([sqldb_df[filter_extra_in_sqldb],
                                 wdb_df])
        combined_df = combined_df.sort_values('obj_identifier')

        sqldb_cur.execute('DELETE from stations')
        ##########################

        print('Writing station meta data to the database ...\n')
        total_stations, num_out_of_bounds = \
                         write_dataframe_to_database(sqldb_conn,
                                                     'stations',
                                                     sqldb_column_names,
                                                     number_of_columns,
                                                     number_of_rows,
                                                     combined_df)


    #total_stations = num_stations_added
    print('\n\nStation metadata has been updated.\n')


    #Update the last_update_datetime for station_control
    sqldb_cur.execute('SELECT count(*) from station_control')
    num_record = sqldb_cur.fetchone()[0]
    print('INFO: number of records in station_control table:', num_record)
    insert_statement = 'INSERT INTO station_control VALUES (?)'
    update_statement = 'UPDATE station_control SET last_update_datetime=(?)'
    #time_str = this_station_update_datetime.strftime('%s')
    time_ep = this_station_update_datetime_ep

    if num_record == 0:
        sqldb_cur.execute(insert_statement, (time_ep,))
    elif num_record == 1:
        sqldb_cur.execute(update_statement, (time_ep,))
    else:
        print("Multiple records exist in the station_control table")
        sys.exit(1)
    sqldb_conn.commit()

    print('\nINFO: Database file had {} stations.'.
          format(db_num_stations_start) +
          '\n      wdb0 allstation query returned {}'.
          format(len(wdb_df)) +
          '\n      Number out of bounds: ({})'.format(num_out_of_bounds) +
          '\n      Final number: {}'.
          format(len(wdb_df) - num_out_of_bounds) +
          '\n      Number added: {}'.format(num_stations_added))
    # print('\nINFO: wdb0 allstation query returned {} '.
    #        format(len(wdb_df)) +
    #       'stations;\n database file had {} stations. '
    #       .format(db_num_stations_start) +
    #       '\n number of out-of-bounds stations was {}.'
    #       .format(num_out_of_bounds) +
    #       '\n actual final number of stations now is {}.'
    #       .format(len(wdb_df) - num_out_of_bounds))
    #       '\n total number of added stations was {}.'.format
    #        (num_stations_added) +

    #close web-database connection
    web_cursor.close()
    web_conn.close()

    #return db_ind
    return total_stations



def get_nwm_file_update_info(conn,
                             nwm_file_datetime_ep,
                             nwm_group,
                             is_ref_target):
    """
    Get entry/entries from nwm_file_update_info in an open m3 database.
    """

    sql_select = "SELECT files_read, " + \
                 "cycle_datetime, " + \
                 "time_minus_hours, " + \
                 "cycle_type " + \
                 "FROM nwm_file_update_info " + \
                 "WHERE datetime=" + \
                 str(nwm_file_datetime_ep) + \
                 " AND nwm_group='" + nwm_group + \
                 "' AND is_reference={}".format(is_ref_target)
                 #nwm_file_datetime.strftime('%s') + \

    #print('debug:', sql_select)
    values_from_db = conn.execute(sql_select).fetchall()

    # Extract values into list.
    nwm_files_read = list(map(lambda x: x[0], values_from_db))
    nwm_cycle_datetime_db_ep = list(map(lambda x: x[1], values_from_db))
    time_minus_hours_db = list(map(lambda x: x[2], values_from_db))
    cycle_type_db = list(map(lambda x: x[3], values_from_db))

    return \
        nwm_files_read, \
        nwm_cycle_datetime_db_ep, \
        time_minus_hours_db, \
        cycle_type_db


def ref_nwm_file_update_info(conn,
                             nwm_file_datetime_ep,
                             nwm_group):
    """
    Get is_reference_db = 1 entry from nwm_file_update_info in an open
    m3 database.
    """

    # Get is_reference_db = 1 data.
    nwm_files_read, \
    nwm_cycle_datetime_db_ep, \
    time_minus_hours_db, \
    cycle_type_db = \
        get_nwm_file_update_info(conn,
                                 nwm_file_datetime_ep,
                                 nwm_group,
                                 1)

    #print('f_read {} for {}.'.format(nwm_files_read,
    #                                 ndt.utc_epoch_to_string(nwm_file_datetime_ep)))
    if len(nwm_files_read) == 0:
        return None, None, None, None

    # Confirm only one item in returned lists.
    if len(nwm_files_read) > 1:
        print('ERROR: multiple is_reference_db = 1 ' +
              'results in nwm_file_update_info for ' +
              ndt.utc_epoch_to_string(nwm_file_datetime_ep) +
              ' and nwm_group_db = "' + nwm_group + '".',
              file=sys.stderr)
              #nwm_file_datetime.strftime('%Y-%m-%d %H:%M:%S') +
        conn.close()
        sys.exit(1)

    nwm_files_read = nwm_files_read[0]
    nwm_cycle_datetime_db_ep = nwm_cycle_datetime_db_ep[0]
    time_minus_hours_db = time_minus_hours_db[0]
    cycle_type_db = cycle_type_db[0]

    return \
        nwm_files_read, \
        nwm_cycle_datetime_db_ep, \
        time_minus_hours_db, \
        cycle_type_db


class opt:
    def __init__(self):
        self.max_num_nwm_files = []
        self.db_dir = []
        self.safe_dir = []
        self.base_name = []
        self.station_update_interval_hours = []

def parse_args():

    """
    Parse command line arguments.
    """

    # Get the current date for setting default update end datetime.
    #system_datetime = dt.datetime.utcnow()

    help_message = 'Update the NWM analysis station database.'
    parser = argparse.ArgumentParser(description=help_message)
    parser.add_argument('-x', '--max_num_nwm_files',
                        type=int,
                        metavar='# of files',
                        nargs='?',
                        default='-1',
                        help='Maximum number of NWM files to read.')

    parser.add_argument('-d', '--db_dir',
                        type=str,
                        metavar='directory path',
                        nargs='?',
                        help='Directory in which database files are ' +
                        'stored.')

    #parser.add_argument('-b', '--db_base_name',
    parser.add_argument('db_base_name',
                        type=str,
                        metavar='database file base name',
                        nargs='?',
                        help='The base name for database files. ' +
                        'Time strings in the name can be omitted.')

    #parser.add_argument('-p', '--oper_base_name',
    #                    type=str,
    #                    metavar='operation database file name for the base',
    #                    nargs='?',
    #                    help='The base database file name for operation. ' +
    #                    'Should contain oper in the name.')

    parser.add_argument('-u', '--station_update_interval_hours',
                        type=int,
                        metavar='# hours',
                        nargs='?',
                        default='24',
                        help='Station metadata update interval hours; ' +
                        'default=24.')

    parser.add_argument('-s', '--safe_dir',
                        type=str,
                        metavar='directory path',
                        nargs='?',
                        help='Directory in which to store a temporary ' +
                        'copy of each database file during modification; ' +
                        'default=database location.')


    # The way --safe_dir works is as follows: each database file is
    # copied to that directory prior to its modification, and that
    # copy, rather than the original, is modified by this program.
    # The copy is moved to args.db_dir only after all modifications are
    # completed successfully.
    # If safe_dir is not defined, then the current working directory is
    # used. Even in that case a temporary copy of each database file is
    # created during modifications, and the original is replaced by the
    # copy only after all modifications have been performed.
    # If this program is called by a script, it might be useful to have
    # that script create a temporary, empty safe_dir. If the safe_dir
    # is not empty after the program runs, its contents may be assumed
    # to be copies of database files that were under modification when
    # this program was interrupted or hit an exception.

    args = parser.parse_args()

    opt.max_num_nwm_files = args.max_num_nwm_files

    if args.db_dir is not None:
        if not os.path.isdir(args.db_dir):
            raise FileNotFoundError(errno.ENOENT,
                                    os.strerror(errno.ENOENT),
                                    args.db_dir)
        opt.db_dir = args.db_dir
    else:
        opt.db_dir = os.getcwd()

    if not os.access(opt.db_dir, os.W_OK):
        print('User cannot write to directory {}.'.format(opt.db_dir),
              file=sys.stderr)
        sys.exit(1)

    if args.safe_dir is not None:
        if not os.path.isdir(args.safe_dir):
            raise FileNotFoundError(errno.ENOENT,
                                    os.strerror(errno.ENOENT),
                                    args.safe_dir)
        opt.safe_dir = args.safe_dir
    else:
        opt.safe_dir = opt.db_dir

    if not os.access(opt.safe_dir, os.W_OK):
        print('User cannot write to directory {}.'.format(opt.safe_dir),
              file=sys.stderr)
        sys.exit(1)

    if args.db_base_name is not None:
        opt.base_name = args.db_base_name

    #if args.archive_base_name is not None:
    #    opt.base_name = args.archive_base_name

    #if args.oper_base_name is not None:
    #    opt.base_name = args.oper_base_name

    if args.station_update_interval_hours is not None:
        opt.station_update_interval_hours = args.station_update_interval_hours

    return opt



def main():

    '''
    Update SQLite3 databases for NWM land surface grids sampled at
    observing stations.
    '''
    time_beginning = os.times()

    # Read command line arguments.
    opt = parse_args()
    if opt is None:
        print('ERROR: Failed to parse command line.', file=sys.stderr)
        sys.exit(1)


    # Set allstation_update_interval_hours to -1 to always update.
    #allstation_update_interval_hours = 720
    allstation_update_interval_hours = opt.station_update_interval_hours

    # Locate and verify the base database files to be updated.
    oper, db_paths = nds_db.verify_base_database(opt.base_name, opt.db_dir)

    num_nwm_files_read = 0

    # Loop over database/s (only one if operational, possibly multiple if archive).
    for db_path in db_paths:

        db_dir, db_file = os.path.split(db_path)

        # Open SQLite3 database file and do the checking
        try:
            sqldb_conn = \
                sqlite3.connect(db_path,
                                detect_types=\
                                sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
            #sqldb_cur = sqldb_conn.cursor()
        except sqlite3.OperationalError:
            print('ERROR: Failed to open database file "{}".'.format(db_path),
                  file=sys.stderr)
            sys.exit(1)

        #Get database info and check file name and time consistency
        db_start_datetime_from_db_ep, \
        new_db_start_datetime_ep, \
        new_db_finish_datetime_ep, \
        sampling_method, \
        land_layer, \
        forcing_single_db, \
        land_single_db, \
        nwm_archive_dir, \
        last_station_update_datetime_ep = \
            database_info_and_checks(sqldb_conn,
                                     db_dir,
                                     db_file,
                                     oper)

        #Now check if other companion databases are exist and then attach them
        nds_db.attach_databases(sqldb_conn,
                                forcing_single_db,
                                land_single_db)

        print('INFO: Companion databases are now attached!')

        #Suffix for temp databases
        suffix = dt.datetime.utcnow().strftime('%Y%m%d%H%M%S') + \
                 '.{}'.format(os.getpid())


        #Get row/column info from the coordinate_system table
        result = sqldb_conn.execute(
                 "PRAGMA table_info('coordinate_system')").fetchall()
        column_names = list(zip(*result))[1]
        default_value = list(zip(*result))[4]
        for col in range(0, len(result)):
            if column_names[col] == 'number_of_rows':
                nwm_grid_num_rows = int(default_value[col])
            if column_names[col] == 'number_of_columns':
                nwm_grid_num_columns = int(default_value[col])

        #Get column names in the original order
        forcing_single_layer_col_names, \
        land_single_layer_col_names, \
        land_snow_layer_col_names = \
            nds_db.get_data_column_names(sqldb_conn, land_layer)


        ## A few common used info
        #statement_insert_var_val = "INSERT INTO temp_var_val VALUES (?,?)"


        # Get cycle type info from the table: cycle_type_themes
        current_yyyymm = \
            dt.datetime.strftime(ndt.utc_epoch_to_datetime(new_db_start_datetime_ep),
                                                       '%Y%m')
        # ***** Above will not work for more than a month period *******

        current_yyyymm = '[0-9][0-9][0-9][0-9][0-9][0-9]'
        nwm_cycle_type_ext_ana, \
        nwm_cycle_type_ana, \
        ana_ext_str, \
        ana_str, \
        ana_ext_pattern, \
        ana_pattern, \
        regex_str, \
        ana_regex_str = \
            get_cycle_theme_info(sqldb_conn, current_yyyymm)

        # Get file names from the table nwm_file_update_info of the database.
        # Skip those that have been processed already.
        nwm_files_processed = get_nwm_files_processed(sqldb_conn)

        # Search for NWM data to sample for this database file.

        # determine whether there are any new NWM files to sample for
        # the database file. If there is no new NWM data to add, then
        # station metadata will not be updated.

        # Set this_update_datetime to the current system time. This
        # should be done just before searching for NWM files, since this
        # datetime will be used as the "last_update_datetime" the next
        # time this program operates on the current ncdb_file.
        #this_update_datetime = dt.datetime.utcnow()

        nwm_file_paths, \
        nwm_file_names, \
        nwm_file_time_minus_hours, \
        nwm_file_datetimes_ep, \
        nwm_file_cycle_datetimes_ep, \
        nwm_file_cycle_types = \
            get_nwm_files(nwm_archive_dir,
                          ana_ext_pattern,
                          ana_pattern,
                          new_db_start_datetime_ep,
                          new_db_finish_datetime_ep,
                          nwm_cycle_type_ext_ana,
                          nwm_cycle_type_ana,
                          nwm_files_processed,
                          oper, sqldb_conn)

        if len(nwm_file_names) == 0:
            print('INFO: No new NWM files ' +
                  'fit into {}'.format(db_file))
            sqldb_conn.close()
            continue

        # Inventory files expeced to be processed for this database.
        if num_nwm_files_read > 0:
            print('INFO: Have processed {} NWM files so far.'.
                  format(num_nwm_files_read))
        if opt.max_num_nwm_files > 0:
            print('INFO: {} unprocessed NWM files '.
                  format(len(nwm_file_names)) + 
                  'fit into this database; {} will be sampled.'
                  .format(min(len(nwm_file_names),
                              opt.max_num_nwm_files - num_nwm_files_read)))
        else:
            print('INFO: {} unprocessed NWM files found to be sampled:'
                  .format(len(nwm_file_names)))
        # Files in nwm_file_names will all be sampled/processed below

        # Close the database file.
        sqldb_conn.close()

        # Sort files in chronological order, with files having larger values
        # of nwm_file_time_minus_hours first, and files having smaller values
        # of nwm_file_cycle_types first.
        nwm_file_names, \
            nwm_file_paths, \
            nwm_file_cycle_types, \
            nwm_file_time_minus_hours, \
            nwm_file_datetimes_ep, \
            nwm_file_cycle_datetimes_ep = \
            zip_and_sort_nwm_files(nwm_file_names,
                                   nwm_file_paths,
                                   nwm_file_cycle_types,
                                   nwm_file_time_minus_hours,
                                   nwm_file_datetimes_ep,
                                   nwm_file_cycle_datetimes_ep)

        # print('--')
        # for nwm_file_name in nwm_file_names:
        #     print(nwm_file_name)

        # sys.exit(1)

        # Update database file/s with NWM data.

        # For a given variable (e.g., land/snow_water_equivalent),
        # all the data stored for a given time, or nwm_file_datetime
        # (i.e., the data for all stations for that time), comes from a
        # single NWM file. However, if both regular and extended analysis
        # data are considered, either 4 or 5 NWM files will cover a given
        # time. Therefore, rules must be established to determine if a
        # file representing a given nwm_file_datatime should be allowed
        # to overwrite existing data for the same time.
        #
        # The following rules apply to updates, consistent with the
        # sorting of NWM files above (first in descending order of
        # nwm_file_time_minus_hours (a.k.a. "tm"), then in ascending
        # order of nwm_file_datetime). For a given time, or
        # nwm_file_datetime:
        #
        # 1. Smaller values of the cycle_type "flag_values" are always
        #    preferred over larger ones; i.e., extended analysis data
        #    will always overwrite regular analysis data, regardless
        #    of the value of "tm".
        #
        # 2. Within a given cycle_type, larger values of "tm", or
        #    nwm_file_time_minus_hours, are always preferred over
        #    smaller ones. For example, within regular analysis
        #    data, data from a "tm02" file should always overwrite
        #    data from a "tm00" or "tm01" file covering the same
        #    nwm_file_datetime. Within extended analysis, a "tm26" file
        #    will overwrite a "tm02" data from the extended analysis
        #    of the previous day, but not vice versa.

        # Create a temporary copy of the database file.
        temp_db_conn, temp_db_cur = \
            copy_temporary_databases(db_path,
                                     db_file,
                                     opt.db_dir,
                                     suffix,
                                     forcing_single_db,
                                     land_single_db,
                                     oper)

        # Decide whether to update station metadata in the database file.
        time_since_update_ep = time.time() - last_station_update_datetime_ep
        hours_since_metadata_update = int(time_since_update_ep / 3600)
        station_start = time.time()
        temp_db_file = db_file + '.' + suffix
        temp_db_path = os.path.join(opt.db_dir, temp_db_file)
        if hours_since_metadata_update >= allstation_update_interval_hours:
            num_stations_all = update_nwm_db_allstation(temp_db_path,
                                                    new_db_start_datetime_ep,
                                                    new_db_finish_datetime_ep,
                                                    temp_db_conn)
            if num_stations_all is None:
                print('Unable to update station metadata in' +
                      temp_db_file + '.',
                      file=sys.stderr)
                temp_db_conn.close()
            print('\nTemporary database {} has {} stations'
                  .format(temp_db_file, num_stations_all))
        else:
            temp_db_cur.execute('SELECT count(*) FROM stations')
            num_stations_all = temp_db_cur.fetchone()[0]
            print('\nINFO: Database contains {} stations.'
                  .format(num_stations_all))
            if hours_since_metadata_update == 1:
                hourhours = 'hour'
            else:
                hourhours = 'hours'
            print('INFO: {} '.format(hours_since_metadata_update) +
                  hourhours +
                  ' (<{}) '.format(allstation_update_interval_hours) +
                  'since previous allstation update; skip updating this time.')

        station_end = time.time()
        # nwm_file_names, \
        # nwm_file_paths, \
        # nwm_file_cycle_types, \
        # nwm_file_time_minus_hours, \
        # nwm_file_datetimes_ep, \
        # nwm_file_cycle_datetimes_ep = \
        # zip_and_sort_nwm_files(nwm_file_names,
        #                        nwm_file_paths,
        #                        nwm_file_cycle_types,
        #                        nwm_file_time_minus_hours,
        #                        nwm_file_datetimes_ep,
        #                        nwm_file_cycle_datetimes_ep)

        # Read NWM grid row/column of stations.
        # TODO: add start_datetimes, stop_datetimes
        db_grid_cols_all, db_grid_rows_all, obj_ids_all,\
        db_start_dates_ep_all, db_stop_dates_ep_all = \
            get_station_latlon_obj_ids_times(temp_db_conn, temp_db_file)


        # Get row/column and rowid for a sample site.
        sample_id = 'MN-HN-149'  # obj_identifier=347082
        sample_grid_col_row, sample_obj_id, sample_ind = \
            get_sample_station_info(temp_db_conn, sample_id)


        # For operation database, we need to first delete those data that
        # are older than new_db_start_datetime

        if oper is True:
            #if new_db_start_datetime_ep > db_start_datetime_from_db_ep[0]:
            if new_db_start_datetime_ep > db_start_datetime_from_db_ep:
                print('\nDeleting records that older than {} ...'
                      .format(ndt.utc_epoch_to_string(new_db_start_datetime_ep)))
                delete_older_data(temp_db_conn, new_db_start_datetime_ep)
            else:
                print('No records to be deleted')

        ## List all files to process.
        # if opt.max_num_nwm_files > 0:
        #     print('\nTotal new nwm files to be processed is {} out of {}.'
        #           .format(opt.max_num_nwm_files, len(nwm_file_names)))
        # else:
        #     print('\nTotal new nwm files to be processed is: ', len(nwm_file_names))

	#below as tmp ref
        '''
        delete from tableToDeleteFrom where tablePK in (select tablePK
        from tableToDeleteFrom where someThresholdDate <= @someThresholdDate)
        '''

        # Loop over all NWM files whose data fit into the current
        # databases.
        # Abbrevation "nf" = "NWM file"

        jan_1_1900_epoch = -2208988800
        prev_nf_datetime_ep = jan_1_1900_epoch
        prev_num_stations = -1

        for nfi, nf_name in enumerate(nwm_file_names):

            # If the earlier call to get_nwm_files returned the "group" (land
            # vs. forcing) for each file, then this step would be fully
            # redundant with information we already have. The "BUG1"
            # etc. checks below confirm this.
            nf_cycle_type_str, \
                nf_group, \
                nf_time_minus_hours, \
                nf_cycle_datetime_ep, \
                nf_datetime_ep = \
                get_nwm_file_info(nf_name)

            if nwm_file_cycle_datetimes_ep[nfi] != nf_cycle_datetime_ep:
                print('BUG1')
                sys.exit(1)
            if nwm_file_datetimes_ep[nfi] != nf_datetime_ep:
                print('BUG2')
                sys.exit(1)
            if nwm_file_time_minus_hours[nfi] != nf_time_minus_hours:
                print('BUG3')
                sys.exit(1)

            if nf_cycle_type_str == 'analysis_assim_extend':
                nf_cycle_type = nwm_cycle_type_ext_ana
            elif nf_cycle_type_str == 'analysis_assim':
                nf_cycle_type = nwm_cycle_type_ana
            else:
                print('WARNING: Unsupported cycle type ' +
                      '"{}" '.format(nf_cycle_type_str) +
                      ' in NWM file {}.'.format(nf_name),
                      file=sys.stderr)
                continue
                ## NOTE: Values for other cycle_types can be retrieved from
                ##       the table "cycle_type_themes". Processing other types
                ##       of data file has not been implemented yet.

            if nwm_file_cycle_types[nfi] != nf_cycle_type:
                print('BUG4')
                sys.exit(1)

            # Identify subset of db_grid_cols, db_grid_rows, obj_ids where
            # stop_datetimes == '1900-01-01 00:00:00'
            # or stop_datetimes >= nf_datetime_ep
            # and start_datetimes == '1900-01-01 00:00:00'
            # or start_datetimes <= nf_datetime_ep
            if nf_datetime_ep != prev_nf_datetime_ep:
                num_stations, db_grid_cols, db_grid_rows, obj_ids = \
                    subset_station_latlon_obj_ids(nf_datetime_ep,
                                                  db_grid_cols_all,
                                                  db_grid_rows_all,
                                                  obj_ids_all,
                                                  db_start_dates_ep_all,
                                                  db_stop_dates_ep_all)
                if num_stations != prev_num_stations:
                    print('INFO: New # of stations: {}.'.format(num_stations))
                prev_num_stations = num_stations
            prev_nf_datetime_ep = nf_datetime_ep

            # TODO:
            # If the database/s are of the "archive" type and
            # nwm_time_minus_hours != 0:
            # For all entries in nwm_file_update_info for the current
            # nwm_file_datetime and nf_group having is_reference_db = 1
            # (there should be at most ONE such entry), get
            # time_minus_hours_db and cycle_type_db.
            # if (nf_cycle_type > cycle_type_db) or \
            #    ((nf_cycle_type == cycle_type_db) and \
            #     (nwm_time_minus_hours < time_minus_hours_db)):
            #     Existing data is better and we should skip this file.

            if oper is False and nf_time_minus_hours != 0:

                ref_nwm_file_name, ref_nwm_cycle_datetime_db_ep, \
                    ref_time_minus_hours_db, ref_cycle_type_db = \
                        ref_nwm_file_update_info(temp_db_conn,
                                                 nf_datetime_ep,
                                                 nf_group)
                if ref_nwm_file_name is not None:
                    if (nf_cycle_type > ref_cycle_type_db) or \
                       ((nf_cycle_type == ref_cycle_type_db) and \
                        (nf_time_minus_hours < ref_time_minus_hours_db)):
                        print('INFO: Skipping {} for {}.'.
                              format(nf_name,
                                     ndt.utc_epoch_to_string(nf_datetime_ep)))
                        continue

            # Get some of the colums of dynamic tables ready
            cycle_type_list = [nf_cycle_type]*num_stations
            time_minus_hours_list = [nf_time_minus_hours]*num_stations
            cycle_datetime_list_ep = [nf_cycle_datetime_ep]*num_stations
            time_list_ep = [nf_datetime_ep]*num_stations
            #cycle_datetime_list = [nwm_file_cycle_datetimes[nfi]]*num_stations
            #time_list = [nwm_file_datetimes[nfi]]*num_stations
            #cycle_datetime_list = [nwm_file_cycle_datetime.strftime('%Y-%m-%d %H:00')]*num_stations
            #time_list = [nwm_file_dt]*num_stations
            #time_list = [nwm_file_datetime.strftime('%Y-%m-%d %H:00')]*num_stations
            #See ref: https://www.sqlite.org/lang_datefunc.html for time string formats that sqlite accepts.

            # nwm_start = time.time()

            # Open the NWM file.
            try:
                nwm = Dataset(nwm_file_paths[nfi], 'r')
            except:
                print('ERROR: Failed to open NWM file {}.'.
                      format(nf_name),
                      file=sys.stderr)
                continue

            checked = check_nwm_attributes(nwm, nf_group, nf_name)
            if checked == 0:
                continue


            #Get the variable names to be processed
            temp_db_cur.execute("SELECT nwm_var_name from nwm_meta WHERE file_type='" + nf_group + "'")
            nwm_var_names = temp_db_cur.fetchall()

            #print(type(nwm_var_names),nwm_var_names,len(nwm_var_names))   #list of variables  names

            num_vars_sampled = 0


            # Create temp tables for forcing/land single_layer, soil_layers and snow_layers tables
            common_cols_str = 'station_obj_identifier integer ' + \
                      ', datetime integer' + \
                      ', cycle_datetime integer ' + \
                      ', cycle_type integer'
                      #', sampling text'
                      #', ensemble integer'
            layer_str = 'vertical_layer integer'

            if nf_group == 'forcing':
                try:
                    temp_db_cur.execute('CREATE TABLE temp_forcing_single_layer (' + \
                                         common_cols_str + ')')
                except:
                    print('Error in creating table: temp_forcing_single_layer')
                    sys.exit(1)
            elif nf_group == 'land':
                try:
                    temp_db_cur.execute('CREATE TABLE temp_land_single_layer (' +\
                                        common_cols_str + ')')
                    if len(land_layer) != 0:
                        temp_db_cur.execute('CREATE TABLE temp_land_snow_layer (' +\
                                            common_cols_str+','+ layer_str + ')')
                        temp_db_cur.execute('CREATE TABLE temp_land_soil_layer (' +\
                                            common_cols_str+','+ layer_str + ')')
                except:
                    print('Error in creating land related tables')
                    sys.exit(1)

            #for nwm_var in nwm_vars:
            forcing_single_layer_var_counter = 0
            land_single_layer_var_counter = 0
            land_soil_layer_var_counter = 0
            land_snow_layer_var_counter = 0
            #if len(land_layer) != 0:
            #    land_soil_layer_var_counter = 0
            #    land_snow_layer_var_counter = 0

            #print('There will be {} nwm_var_names'.format(len(nwm_var_names)))
            #print(nwm_var_names)
            for nwm_var_name in nwm_var_names:
                nwm_var = nwm.variables[nwm_var_name[0]]

                ##Get the table names for each variable.
                temp_db_cur.execute("SELECT var_name from nwm_meta WHERE nwm_var_name='" + \
                                     nwm_var_name[0]+ "'")
                nwm_var_col_name = temp_db_cur.fetchone()[0]

                if nf_group == 'forcing':
                    forcing_single_layer_var_counter += 1
                elif nf_group == 'land':
                    if ('_snow_' in nwm_var_col_name) and  ('_by_layer' in nwm_var_col_name):
                        land_snow_layer_var_counter += 1
                    elif ('_soil_' in nwm_var_col_name) and  ('_by_layer' in nwm_var_col_name):
                        land_soil_layer_var_counter += 1
                    else:
                        land_single_layer_var_counter += 1
                else:
                    print('This nf_group {} has not been implemented.')
                    sys.exit(1)

                #Create a temp table to hold values for each variable
                create_temp_var_table(temp_db_conn, nwm_var_col_name)

                # Make sure the units match.
                checked_ok = check_var_units(temp_db_conn, nwm_var, nwm_var_name[0])
                if checked_ok == False:
                    continue


                #Get the dimensions for each variable from the database
                temp_db_cur.execute("SELECT dims from nwm_meta WHERE " + \
                                    "nwm_var_name='" + nwm_var_name[0]+ "'")
                db_var_dims_str = temp_db_cur.fetchone()[0]
                db_var_dims = db_var_dims_str.split(',')
                #num_dims = len(db_var_dims)


                ## Identify positions of dimensions in the database file variable.
                pos = identify_dim_pos_in_var(nf_name,
                                              db_var_dims,
                                              nwm,
                                              nwm_var,
                                              nwm_var_name[0],
                                              nwm_grid_num_rows,
                                              nwm_grid_num_columns)
                if pos == 0:
                    continue

                nwm_dim_time_locs = get_nwm_dim_time_loc(nwm_var)
                # nwm_x_dim_loc = nwm_dim_time_locs[0]
                # nwm_y_dim_loc = nwm_dim_time_locs[1]
                nwm_z_dim_loc = nwm_dim_time_locs[2]
                # nwm_time_dim_loc = nwm_dim_time_locs[3]

                num_z = 1
                if  nwm_z_dim_loc is not None:
                    num_z = nwm_var.shape[nwm_z_dim_loc]
                    ##num_z = nwm_var.dimensions[nwm_z_dim_name].size
                    ##num_z = nwm.dimensions[nwm_z_dim_name].size

                for zc in range(num_z):

                    if (num_z > 1) and \
                           (sampling_method != 'neighbor'):
                        print('NOTICE: Skipping "{}" '.
                              format(sampling_method) +
                              'sampling of multi-layer variable ' +
                              '"{}".'.format(nwm_var.name))
                        continue

                    nwm_grid = \
                        new_nwm_grid_for_zc(zc, nwm_dim_time_locs,
                                            nwm_var)


                    # Programming check.
                    if nwm_grid.shape[0] != nwm_grid_num_rows:
                        print('ERROR: Programming mistake in NWM ' +
                              'file slice (rows).',
                              file=sys.stderr)
                        sys.exit(1)
                    if nwm_grid.shape[1] != nwm_grid_num_columns:
                        print('ERROR: Programming mistake in NWM ' +
                              'file slice (columns).',
                              file=sys.stderr)
                        sys.exit(1)

                    zc_list = [zc]*num_stations

                    ndv = nwm_var.getncattr('_FillValue')
                    result = \
                        sample_grid_at_points(nwm_grid,
                                              db_grid_rows,
                                              db_grid_cols,
                                              fill_value=ndv,
                                              method=sampling_method,
                                              measure_wall_times=False)


                    #write data to sqlite database

                    if len(result) != num_stations:
                        print('Error: Result dimension is different from # of stations')
                        print(len(result), len(db_grid_rows), num_stations)
                        sys.exit(1)

                    #result_list = result[:, 0].tolist()

                    '''
                    #print('\nINFO: Updating {} for {}.'.format(nwm_var_table,
                    print('\nINFO: Updating {} for {}.'.format(nwm_var_col_name,
                                                               nwm_file_datetimes[nfi]))
                    print('  Time: ', time_list[0])
                    print('    Cycle time: ', cycle_datetime_list[0])
                    print('      Time Minus Hour: ', time_minus_hours_list[0])
                    print('        Cycle type: ', cycle_type_list[0])
                    #print('          Sampling: ', sc_list[0])
                    print('              Layer: ', zc_list[0])
                    '''
                    df_obj_ids = pd.DataFrame(obj_ids)
                    df_result = pd.DataFrame(result, columns=list(nwm_var_name))
                    df_time_ep = pd.DataFrame(time_list_ep)
                    df_cycle_datetime_ep = pd.DataFrame(cycle_datetime_list_ep)
                    df_cycle_type = pd.DataFrame(cycle_type_list)
                    df_zc = pd.DataFrame(zc_list)
                    #df_time_ep = df_time_ep.astype('str')
                    #df_cycle_datetime_ep = df_cycle_datetime_ep.astype('str')
                    #df_sc = pd.DataFrame(sc_list)

                    #write each variable data to a temp table for all stations
                    write_each_var_vals_to_temp_table(temp_db_conn,
                                                      df_obj_ids,
                                                      df_time_ep,
                                                      df_cycle_datetime_ep,
                                                      df_cycle_type,
                                                      df_result,
                                                      df_zc,
                                                      nf_group,
                                                      nwm_var_col_name,
                                                      land_snow_layer_var_counter,
                                                      land_soil_layer_var_counter,
                                                      land_single_layer_var_counter,
                                                      forcing_single_layer_var_counter)


#                    temp_db_conn.commit()

                    #Manually mute the following part for sample station
                    sample_prt = 0  # print sample info when =1
                    if (sample_id is not None) and (sample_prt == 1):
                        print_sample_station_loc(nwm_grid,
                                                 sample_ind,
                                                 db_grid_cols,
                                                 db_grid_rows)

                    #time_start = time.time()

                num_vars_sampled += 1

                # Still in the variable loop here
                # Append each variable's values as new column to the temp table
                attach_each_var_vals_to_temp_table(temp_db_conn,
                                                   nf_group,
                                                   nwm_var_col_name)
                temp_db_cur.execute("DROP TABLE IF EXISTS temp_var_val")
#                temp_db_conn.commit()

            ## write data to final perspective tables for each nwm file [nfi] processed.
            write_temp_data_to_database(temp_db_conn,
                                        nf_group,
                                        land_layer,
                                        land_single_layer_col_names,
                                        forcing_single_layer_col_names,
                                        land_snow_layer_col_names)
                                        #land_soil_layer_col_names,

            nwm.close()

            # Recorded processed nwm file and other info to nwm_file_update_info
            print('INFO: Updating nwm_file_update_info table ' +
                  'with {} for {}.'.
                  format(nf_name,
                         ndt.utc_epoch_to_string(nf_datetime_ep)))

            #To update is_reference and delete un-necessary data from archive

            update_nwm_file_update_info(temp_db_conn,
                                        nf_name,
                                        nwm_cycle_type_ext_ana,
                                        nwm_cycle_type_ana,
                                        oper)





            temp_db_cur.execute("DROP TABLE IF EXISTS temp_land_single_layer")
            temp_db_cur.execute("DROP TABLE IF EXISTS temp_forcing_single_layer")
            if len(land_layer) != 0:
                temp_db_cur.execute("DROP TABLE IF EXISTS temp_land_soil_layer")
                temp_db_cur.execute("DROP TABLE IF EXISTS temp_land_snow_layer")
            temp_db_conn.commit()



            #nwm_finish = time.time()
            #print('\nINFO: Update file {} '.format(a_nwm_file_name) +
            #      'took {} seconds.'.format(nwm_finish - nwm_start))

            if num_vars_sampled != len(nwm_var_names):
                print('WARNING: Some variables were not processed. ' +
                      'No update to "cycle_type" and ' +
                      '"cycle_datetime_minus_hours" variables.',
                      file=sys.stderr)
                continue

            num_nwm_files_read += 1


            if (opt.max_num_nwm_files > 0) and \
               (num_nwm_files_read >= opt.max_num_nwm_files):
                break

        ## Only update the last_update_datetime if ALL qualifying NWM
        ## files were considered (i.e., do not update if we just broke out
        ## of the nwm_file_names loop).
        #if (opt.max_num_nwm_files <= 0) or \
        #   (num_nwm_files_read < opt.max_num_nwm_files):
        #    ncdb.setncattr_string('last_update_datetime',
        #                          this_update_datetime.
        #                          strftime('%Y-%m-%d %H:%M:%S UTC'))

        update_databases_info(temp_db_conn,
                              new_db_start_datetime_ep,
                              new_db_finish_datetime_ep)

        if hours_since_metadata_update >= allstation_update_interval_hours:
            print('\nINFO: Station metadata update took {} minutes.'
                  .format(round((station_end - station_start) / 60.0, 2)))

        #if opt.max_num_nwm_files > 0:
        #    print('Time for updating {} NWM files took {} minutes.'
        #          .format(opt.max_num_nwm_files, (nwm_finish-station_end)/60.0))
        #else:
        #    print('Time for updating {} NWM files took {} minutes.'
        #          .format(len(nwm_file_names), (nwm_finish-station_end)/60.0))

        temp_db_conn.execute('COMMIT')
        temp_db_conn.close()

        #Rename temp database files back
        rename_temp_databases(opt.db_dir,
                              db_file,
                              forcing_single_db,
                              land_single_db,
                              suffix)
                              #land_soil_db,
                              #land_snow_db)

        print('\nINFO: Completed updates to {}'.format(db_file))

        if (opt.max_num_nwm_files > 0) and \
           (num_nwm_files_read >= opt.max_num_nwm_files):
            break


    time_end = os.times()
    total_time_spent = (time_end.elapsed - time_beginning.elapsed)/60.0
    print('Total time spent for this run is {} minutes.\n'.
          format(round(total_time_spent), 2))


    # To get reference values for each variable for a given station

    # extract data for a specified station
    '''
    print('Extracting and plotting data for a specified station...')
    station_obj_id = 74501
    station_obj_id = 888
    station_obj_id = 1959
    var_names = [('land', 'nwm_snow_water_equivalent'),
                 ('forcing', 'nwm_precip_rate')]
    try:
        base_db = os.path.join(opt.db_dir, db_file)
    except:
        print('\nThe database may not exist.')
        print('Check your db_base_name option.')
        sys.exit(1)
    extract_data_for_station(station_obj_id,
                             var_names,
                             oper,
                             base_db,
                             land_single_db,
                             forcing_single_db)
    '''


if __name__ == '__main__':
    main()
