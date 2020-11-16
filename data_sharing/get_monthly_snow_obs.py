#!/usr/bin/python
'''
Collect monthly SWE and snow depth data for a specific period of months.
For 00Z on the first of each month, get all SWE and snow depth data, as nearly
concurrent as possible, using a window of some hours before and after the
target time to maximize the amount of data collected. Also use some criterion
for "concurrent" that is more flexible than requiring the data occur at
exactly the same time.
'''
import os
import datetime as dt
import pickle as pkl
import psycopg2
import sys
import logging
import pandas as pd
import numpy as np
import csv

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'lib'))
import wdb0
import local_logger
import nwm_da_time as ndt

def get_snow_obs_for_target(target_datetime,
                            window_hrs_prev,
                            window_hrs_post,
                            max_sep_hrs_for_concurrence,
                            bounding_box=None,
                            scratch_dir=None):

    '''
    Get snow water equivalent (SWE) and snow depth observations from the
    "web_data" database on wdb0, targeting a specific target_datetime, but
    allowing for a window that covers window_hrs_prev to window_hrs_post. If
    possible, concurrent snow depth observations are collected, allowing for a
    time difference up to max_sep_hrs_for_concurrence.

    Example:
    - target_datetime = 2019-12-01 00Z
    - window_hrs_prev = 12
    - window_hrs_post = 12
    - max_sep_hrs_for_concurrence = 6
    For this case, SWE observations from 2019-11-30 12Z to 2019-12-01 12Z will
    be accepted. For any given site only one observation as close as
    possible to the target_datetime will be returned, and others will be
    discared. For each SWE observation, a snow depth observation up to 6 hours
    away will be returned as a "concurrent" snow depth.
    This logic implies that getting SWE close to the target_datetime is more
    important than getting perfectly concurrent SWE and snow depth.
    '''

    logger = logging.getLogger()
    
    # Only use .pkl files if there is no bounding box.
    pkl_file_path = None
    if bounding_box is None:
        pkl_file_name = 'wdb0_obs_swe_snwd_' + \
            'target_{}_'.format(target_datetime.strftime('%Y%m%d%H')) + \
            'p{}h_m{}h_'.format(window_hrs_prev, window_hrs_post) + \
            'max_sep_{}h'.format(max_sep_hrs_for_concurrence) + \
            '.pkl'
        if scratch_dir is not None:
            output_dir = scratch_dir
        else:
            output_dir = os.path.dirname(__file__)
            logger.debug('No scratch area idenfitied; ' +
                         'Using {} '.format(output_dir) +
                         'as output directory.')

        pkl_file_path = os.path.join(scratch_dir, pkl_file_name)
            
        if os.path.isfile(pkl_file_path):
            # Retrieve data from pkl file and return.
            file_obj = open(pkl_file_path, 'rb')
            obs_snow = pkl.load(file_obj)
            file_obj.close()
            return(obs_snow)
        else:
            logger.debug('Cache file {} not found.'.
                         format(pkl_file_path))

    begin_datetime = target_datetime - dt.timedelta(hours=window_hrs_prev)
    end_datetime = target_datetime + dt.timedelta(hours=window_hrs_post)
    time_range = end_datetime - begin_datetime
    num_hours = time_range.days * 24 + time_range.seconds // 3600 + 1

    # Open the web database.
    conn_string = "host='wdb0.dmz.nohrsc.noaa.gov' dbname='web_data'"
    conn = psycopg2.connect(conn_string)
    conn.set_client_encoding('utf-8')
    cursor = conn.cursor()

    # Define the SQL statement for getting SWE.
    sql_cmd = 'SELECT ' + \
        't1.obj_identifier, ' + \
        'TRIM(t1.station_id), ' + \
        'TRIM(t1.name), ' + \
        'TRIM(t1.station_type), ' + \
        't1.coordinates[0] AS lon, ' + \
        't1.coordinates[1] AS lat, ' + \
        't1.elevation, ' + \
        't1.recorded_elevation, ' + \
        't2.date, ' + \
        't2.value * 1000.0 AS obs_swe_mm, ' + \
        't3.value * 1000.0 AS obs_snwd_mm ' + \
        'FROM ' + \
        'point.allstation AS t1, ' + \
        'point.obs_swe AS t2, ' + \
        'point.obs_snow_depth AS t3 ' + \
        'WHERE ' + \
        't2.date >= \'' + \
        begin_datetime.strftime('%Y-%m-%d %H:%M:%S') + \
        '\' ' + \
        'AND t2.date <= \'' + \
        end_datetime.strftime('%Y-%m-%d %H:%M:%S') + \
        '\' ' + \
        'AND t3.date = t2.date ' + \
        'AND t2.obj_identifier = t1.obj_identifier ' + \
        'AND t3.obj_identifier = t2.obj_identifier ' + \
        'AND (t3.value IS NOT NULL OR t2.value IS NOT NULL) '

    if bounding_box is not None:

        sql_cmd = sql_cmd + \
                  'AND t1.coordinates[1] >= ' + \
                  '{} '.format(bounding_box['min_lat']) + \
                  'AND t1.coordinates[1] < ' + \
                  '{} '.format(bounding_box['max_lat']) + \
                  'AND t1.coordinates[0] >= ' + \
                  '{} '.format(bounding_box['min_lon']) + \
                  'AND t1.coordinates[0] < ' + \
                  '{} '.format(bounding_box['max_lon'])

    sql_cmd = sql_cmd + ' ORDER BY obj_identifier, date;'
    logger.debug('psql command: {}'.format(sql_cmd))
    cursor.execute(sql_cmd)
    obs = cursor.fetchall()
    # obs_column_list = ['obj_identifier',
    #                    'station_id',
    #                    'name',
    #                    'type',
    #                    'lon',
    #                    'lat',
    #                    'elevation',
    #                    'recorded_elevation',
    #                    'date',
    #                    'obs_swe_mm',
    #                    'obs_snwd_mm']
    obs_column_list = ['station_obj_id',
                       'station_id',
                       'station_name',
                       'station_type',
                       'station_lon',
                       'station_lat',
                       'station_dem_elevation',
                       'station_rec_elevation',
                       'date',
                       'obs_swe_mm',
                       'obs_snwd_mm']
    df = pd.DataFrame(obs, columns=obs_column_list)
    snow_by_station = df.groupby('obj_identifier')
    logger.debug('Found snow data for {} sites.'.format(len(snow_by_station)))
    obs_snow = []
    target_datetime_str = target_datetime.strftime('%Y-%m-%d %HZ')
    
    for obj_identifier, group in snow_by_station:

        # print(group)

        # Index the valid SWE value nearest to the target_datetime.
        # This works for finding the valid swe index closest to the
        # target_datetime, as long as pd.notna has at least one True
        # result:
        # print(((group[pd.notna(group['obs_swe_mm']) == True]['date'] -
        # target_datetime).astype('timedelta64[h]').abs()).idxmin())
        # Here we chop the process into more digestible pieces.
        swe_idx = None
        ok_swe = group[pd.notna(group['obs_swe_mm']) == True]
        if not ok_swe.empty:
            ok_swe_date = ok_swe['date']
            ok_swe_offset = ok_swe_date - target_datetime
            ok_swe_abs_offset = ok_swe_offset.astype('timedelta64[h]').abs()
            swe_idx = ok_swe_abs_offset.idxmin()
            swe_row = df.iloc[swe_idx]
            swe_row_dict = swe_row.to_dict()
            swe_row_dict.pop('date')
            swe_row_dict.pop('obs_swe_mm')
            swe_row_dict.pop('obs_snwd_mm')

        # Index the valid snow depth value nearest to the target_datetime.
        snwd_idx = None
        ok_snwd = group[pd.notna(group['obs_snwd_mm']) == True]
        if not ok_snwd.empty:
            ok_snwd_date = ok_snwd['date']
            ok_snwd_offset = ok_snwd_date - target_datetime
            ok_snwd_abs_offset = ok_snwd_offset.astype('timedelta64[h]').abs()
            snwd_idx = ok_snwd_abs_offset.idxmin()
            snwd_row = df.iloc[snwd_idx]
            snwd_row_dict = snwd_row.to_dict()
            snwd_row_dict.pop('date')
            snwd_row_dict.pop('obs_swe_mm')
            snwd_row_dict.pop('obs_snwd_mm')

        if swe_idx is not None and snwd_idx is not None:

            # Defer to the SWE observation and make sure the two are not too
            # time-lagged.
            snow_row_dict = swe_row_dict
            snow_row_dict.update({'date_swe':
                                  swe_row['date'].to_pydatetime(),
                                  'obs_swe_mm': swe_row['obs_swe_mm'],
                                  'date_snwd': None,
                                  'obs_snwd_mm': None})
            swe_snwd_offset = df.iloc[swe_idx]['date'] - \
                              df.iloc[snwd_idx]['date']
            if np.abs(swe_snwd_offset.total_seconds()) / 3600.0 > \
               max_sep_hrs_for_concurrence:
                logger.debug('Snow depth and SWE ' +
                             'at {} '.format(obj_identifier) +
                             'for target {} '.format(target_datetime_str) +
                             'are too lagged to treat as concurrent.')
            else:
                snow_row_dict['date_snwd'] = snwd_row['date'].to_pydatetime()
                snow_row_dict['obs_snwd_mm'] = snwd_row['obs_snwd_mm']

        elif swe_idx is None:

            # Only have snow depth.
            logger.debug('No SWE data ' +
                         'at {} '.format(obj_identifier) +
                         'for target {}.'.format(target_datetime_str))
            snow_row_dict = snwd_row_dict
            snow_row_dict.update({'date_swe': None,
                                  'obs_swe_mm': None,
                                  'date_snwd':
                                  snwd_row['date'].to_pydatetime(),
                                  'obs_snwd_mm': snwd_row['obs_snwd_mm']})

        elif snwd_idx is None:

            # Only have SWE.
            logger.debug('No snow depth data ' +
                         'at {} '.format(obj_identifier) +
                         'for target {}.'.format(target_datetime_str))
            snow_row_dict = swe_row_dict
            snow_row_dict.update({'date_swe':
                                  swe_row['date'].to_pydatetime(),
                                  'obs_swe_mm': swe_row['obs_swe_mm'],
                                  'date_snwd': None,
                                  'obs_snwd_mm': None})
        else:

            # No good data how does that happen this is a programming error.
            logger.error('(PROGRAMMING) No valid SWE or snow depth.')
            return None
            
        obs_snow.append(snow_row_dict)

        # if swe_idx != snwd_idx:
        #     print(snow_row_dict)
        #     xxx = input()

    lag = dt.datetime.utcnow() - end_datetime
    if lag > dt.timedelta(days=30):
        if pkl_file_path is not None:
            file_obj = open(pkl_file_path, 'wb')
            pkl.dump(obs_snow, file_obj)
            file_obj.close()
            logger.debug('Stored results in {}.'.format(pkl_file_path))
    else:
        logger.debug('Data are too recent to cache ' +
                     'in {}.'.format(pkl_file_path))

    return obs_snow

def main():
    
    # Initialize logger.
    opt_verbose = True
    logger = local_logger.init(logging.WARNING)
    if sys.stdout.isatty():
        if opt_verbose:
            logger.setLevel(logging.DEBUG)
        else:
            logger.setLevel(logging.INFO)

    target_datetime = dt.datetime.strptime('2018-12-01 00:00:00',
                                           '%Y-%m-%d %H:%M:%S')
    window_hrs_prev = 24
    window_hrs_post = 24
    max_sep_hrs_for_concurrence = 6
    scratch_dir = '/net/scratch/nwm_snow_da/wdb0_pkl'

    start_datetime = dt.datetime.strptime('2018-10-01 00:00:00',
                                          '%Y-%m-%d %H:%M:%S')
    finish_datetime = dt.datetime.strptime('2020-09-01 00:00:00',
                                           '%Y-%m-%d %H:%M:%S')

    target_datetime = start_datetime
    while target_datetime <= finish_datetime:
        obs_snow = get_snow_obs_for_target(target_datetime,
                                           window_hrs_prev,
                                           window_hrs_post,
                                           max_sep_hrs_for_concurrence,
                                           scratch_dir=scratch_dir)
        logger.info('Found data for {} '.format(len(obs_snow)) +
                    'sites for {}.'.
                    format(target_datetime.strftime('%Y-%m-%d %HZ')))

        
        # Write to csv.
        csv_file_name = 'wdb0_obs_swe_snwd_' + \
            'target_{}_'.format(target_datetime.strftime('%Y%m%d%H')) + \
            'p{}h_m{}h_'.format(window_hrs_prev, window_hrs_post) + \
            'max_sep_{}h'.format(max_sep_hrs_for_concurrence) + \
            '.csv'
        with open(csv_file_name, 'w', newline='') as csv_file:
            csv_writer = csv.writer(csv_file, delimiter=',',
                                    quotechar='"', quoting=csv.QUOTE_MINIMAL)
            csv_writer.writerow(list(obs_snow[0].keys()))
            for item in obs_snow:
                csv_writer.writerow(list(item.values()))

        target_datetime = ndt.plus_one_month(target_datetime)

if __name__ == '__main__':
    main()
