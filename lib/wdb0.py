import os
import psycopg2
import pandas as pd
import numpy as np
import pickle as pkl
import datetime as dt
import logging

# logger = logging.getLogger(__name__)
# logger.setLevel(logging.DEBUG)
# console_handler = logging.StreamHandler()
# console_formatter = \
#     logging.Formatter('%(name)s.%(funcName)s: %(levelname)s: %(message)s')
# console_handler.setFormatter(console_formatter)
# console_handler.setLevel(logging.DEBUG) # Change INFO to DEBUG for development.
# logger.addHandler(console_handler)

"""
Functions for reading data from wdb0.
get_snow_depth_obs
get_swe_obs
get_swe_obs_df
get_prev_snow_depth_obs
get_prev_swe_obs
get_air_temp_obs
get_prev_air_temp_obs
get_snwd_snfl_obs
get_snwd_precip_obs
get_swe_prcp_obs
get_prv_airtemp_obs
"""

def get_snow_depth_obs(begin_datetime,
                       end_datetime,
                       no_data_value=-99999.0,
                       bounding_box=None,
                       scratch_dir=None,
                       verbose=None):

    """
    Get hourly snow depth observations from the "web_data" database on
    wdb0.

    XXXNote that observations from the end datetime are NOT included. For
    XXXexample, if begin_datetime is 2019-01-01 00 and end_datetime is
    XXX2019-02-01 00, then 31 * 24 = 744 hours of data are gathered, from
    XXX2019-01-01 00 to 2019-01-31 23.
    Note that observations from the end datetime ARE included. For
    example, if begin_datetime is 2019-01-01 00 and end_datetime is
    2019-01-31 23, then 31 * 24 = 744 hours of data are gathered, from
    2019-01-01 00 to 2019-01-31 23.
    """

    file_name = 'wdb0_obs_snow_depth_' + \
                begin_datetime.strftime('%Y%m%d%H') + \
                '_to_' + \
                end_datetime.strftime('%Y%m%d%H') + \
                '.pkl'

    if scratch_dir is not None:
        file_name = os.path.join(scratch_dir, file_name)

    if os.path.isfile(file_name):

        # Retrieve data from pkl file and return.
        file_obj = open(file_name, 'rb')
        obs_snow_depth = pkl.load(file_obj)
        file_obj.close()
        return(obs_snow_depth)

    time_range = end_datetime - begin_datetime
    num_hours = time_range.days * 24 + time_range.seconds // 3600 + 1
    # print('num_hours = {}'.format(num_hours))

    # Open the web database.
    conn_string = "host='wdb0.dmz.nohrsc.noaa.gov' dbname='web_data'"
    conn = psycopg2.connect(conn_string)
    conn.set_client_encoding("utf-8")
    cursor = conn.cursor()

    # Define a SQL statement.
    sql_cmd = 'SELECT ' + \
              'point.allstation.obj_identifier, ' + \
              'TRIM(point.allstation.station_id), ' + \
              'TRIM(point.allstation.name), ' + \
              'point.allstation.coordinates[0] AS lon, ' + \
              'point.allstation.coordinates[1] AS lat, ' + \
              'point.allstation.elevation, ' + \
              'point.allstation.recorded_elevation, ' + \
              'date, ' + \
              'value * 100.0 AS obs_snow_depth_cm ' + \
              'FROM point.allstation, ' + \
              'point.obs_snow_depth ' + \
              'WHERE date >= \'' + \
              begin_datetime.strftime('%Y-%m-%d %H:%M:%S') + \
              '\' ' + \
              'AND date <= \'' + \
              end_datetime.strftime('%Y-%m-%d %H:%M:%S') + \
              '\' ' + \
              'AND point.allstation.obj_identifier = ' + \
              'point.obs_snow_depth.obj_identifier ' + \
              'AND value IS NOT NULL '

    if bounding_box is not None:
        sql_cmd = sql_cmd + \
                  'AND point.allstation.coordinates[0] >= ' + \
                  '{} '.format(bounding_box[0]) + \
                  'AND point.allstation.coordinates[0] < ' + \
                  '{} '.format(bounding_box[1]) + \
                  'AND point.allstation.coordinates[1] >= ' + \
                  '{} '.format(bounding_box[2]) + \
                  'AND point.allstation.coordinates[1] < ' + \
                  '{} '.format(bounding_box[3])

    sql_cmd = sql_cmd + 'ORDER BY obj_identifier, date;'

    if verbose:
        print('INFO: psql command "{}"'.format(sql_cmd))

    cursor.execute(sql_cmd)

    # The result below is just a huge list of tuples.
    obs_depth = cursor.fetchall()

    obs_depth_column_list = ['obj_identifier',
                             'station_id',
                             'name',
                             'lon',
                             'lat',
                             'elevation',
                             'recorded_elevation',
                             'date',
                             'obs_snow_depth_cm']

    df = pd.DataFrame(obs_depth, columns=obs_depth_column_list)

    # This section organizes the query results into lists and arrays.

    station_ind = -1
    current_station_obj_id = -1
    station_obj_id = []
    station_id = []
    station_name = []
    station_lon = []
    station_lat = []
    station_elevation = []
    station_rec_elevation = []
    # Create a 2-d [time, station] array.
    obs = np.ma.empty([1, num_hours], dtype=float)
    obs[0,:] = no_data_value
    obs[0,:] = np.ma.masked
    for ind, row in df.iterrows():
        if row['obj_identifier'] != current_station_obj_id:
            station_ind += 1
            if station_ind > 0:
                # Just finished all data for previous station.
                obs = np.ma.append(obs,
                                   np.ma.empty([1,
                                                num_hours],
                                               dtype=float),
                                   axis=0)
                obs[station_ind,:] = no_data_value
                obs[station_ind,:] = np.ma.masked
            # New station
            station_obj_id.append(row['obj_identifier'])
            station_id.append(row['station_id'])
            station_name.append(row['name'])
            station_lon.append(row['lon'])
            station_lat.append(row['lat'])
            station_elevation.append(row['elevation'])
            station_rec_elevation.append(row['recorded_elevation'])

            current_station_obj_id = station_obj_id[station_ind]

        # Add the observation to the obs array.
        time_diff = row['date'] - begin_datetime
        time_ind = time_diff.days * 24 + time_diff.seconds // 3600

        try:
            obs[station_ind, time_ind] = row['obs_snow_depth_cm']
        except:
            print(num_hours)
            print(station_ind, time_ind)
            print(obs)
            print(obs.shape)
            print(row)
            exit(1)

    num_stations = station_ind + 1

    obs_datetime = [begin_datetime +
                    dt.timedelta(hours=i) for i in range(num_hours)]

    # Place results in a dictionary.
    obs_snow_depth = {'num_stations': num_stations,
                      'num_hours': num_hours,
                      'station_obj_id': station_obj_id,
                      'station_id': station_id,
                      'station_name': station_name,
                      'station_lon': station_lon,
                      'station_lat': station_lat,
                      'station_elevation': station_elevation,
                      'station_rec_elevation': station_rec_elevation,
                      'obs_datetime': obs_datetime,
                      'values_cm': obs}

    # Create the pkl file if all data fetched is more than 60 days earlier
    # than the current date/time.
    lag = dt.datetime.utcnow() - end_datetime
    if lag > dt.timedelta(days=60):
        file_obj = open(file_name, 'wb')
        pkl.dump(obs_snow_depth, file_obj)
        file_obj.close()

    return(obs_snow_depth)


def get_swe_obs(begin_datetime,
                end_datetime,
                no_data_value=-99999.0,
                bounding_box=None,
                scratch_dir=None,
                verbose=None):

    """
    Get hourly snow water equivalent observations from the "web_data" database
    on wdb0.
    Note that observations from the end datetime ARE included. For
    example, if begin_datetime is 2019-01-01 00 and end_datetime is
    2019-01-31 23, then 31 * 24 = 744 hours of data are gathered, from
    2019-01-01 00 to 2019-01-31 23.
    """

    file_name = 'wdb0_obs_snow_water_equivalent_' + \
                begin_datetime.strftime('%Y%m%d%H') + \
                '_to_' + \
                end_datetime.strftime('%Y%m%d%H') + \
                '.pkl'

    if scratch_dir is not None:
        file_name = os.path.join(scratch_dir, file_name)

    if os.path.isfile(file_name):

        # Retrieve data from pkl file and return.
        file_obj = open(file_name, 'rb')
        obs_swe = pkl.load(file_obj)
        file_obj.close()
        return(obs_swe)

    time_range = end_datetime - begin_datetime
    num_hours = time_range.days * 24 + time_range.seconds // 3600 + 1

    # Open the web database.
    conn_string = "host='wdb0.dmz.nohrsc.noaa.gov' dbname='web_data'"
    conn = psycopg2.connect(conn_string)
    conn.set_client_encoding("utf-8")
    cursor = conn.cursor()

    # Define a SQL statement.
    sql_cmd = 'SELECT ' + \
              'point.allstation.obj_identifier, ' + \
              'TRIM(point.allstation.station_id), ' + \
              'TRIM(point.allstation.name), ' + \
              'point.allstation.coordinates[0] AS lon, ' + \
              'point.allstation.coordinates[1] AS lat, ' + \
              'point.allstation.elevation, ' + \
              'point.allstation.recorded_elevation, ' + \
              'date, ' + \
              'value * 1000.0 AS obs_swe_mm ' + \
              'FROM point.allstation, ' + \
              'point.obs_swe ' + \
              'WHERE date >= \'' + \
              begin_datetime.strftime('%Y-%m-%d %H:%M:%S') + \
              '\' ' + \
              'AND date <= \'' + \
              end_datetime.strftime('%Y-%m-%d %H:%M:%S') + \
              '\' ' + \
              'AND point.allstation.obj_identifier = ' + \
              'point.obs_swe.obj_identifier ' + \
              'AND value IS NOT NULL '

    if bounding_box is not None:
        sql_cmd = sql_cmd + \
                  'AND point.allstation.coordinates[0] >= ' + \
                  '{} '.format(bounding_box[0]) + \
                  'AND point.allstation.coordinates[0] < ' + \
                  '{} '.format(bounding_box[1]) + \
                  'AND point.allstation.coordinates[1] >= ' + \
                  '{} '.format(bounding_box[2]) + \
                  'AND point.allstation.coordinates[1] < ' + \
                  '{} '.format(bounding_box[3])

    sql_cmd = sql_cmd + 'ORDER BY obj_identifier, date;'

    if verbose:
        print('INFO: psql command "{}"'.format(sql_cmd))

    cursor.execute(sql_cmd)

    # The result below is just a huge list of tuples.
    obs_swe = cursor.fetchall()

    obs_swe_column_list = ['obj_identifier',
                           'station_id',
                           'name',
                           'lon',
                           'lat',
                           'elevation',
                           'recorded_elevation',
                           'date',
                           'obs_swe_mm']

    df = pd.DataFrame(obs_swe, columns=obs_swe_column_list)

    # This section organizes the query results into lists and arrays.

    station_ind = -1
    current_station_obj_id = -1
    station_obj_id = []
    station_id = []
    station_name = []
    station_lon = []
    station_lat = []
    station_elevation = []
    station_rec_elevation = []
    # Create a 2-d [time, station] array.
    obs = np.ma.empty([1, num_hours], dtype=float)
    obs[0,:] = no_data_value
    obs[0,:] = np.ma.masked
    for ind, row in df.iterrows():
        if row['obj_identifier'] != current_station_obj_id:
            station_ind += 1
            if station_ind > 0:
                # Just finished all data for previous station.
                obs = np.ma.append(obs,
                                   np.ma.empty([1, num_hours],
                                               dtype=float),
                                   axis=0)
                obs[station_ind,:] = no_data_value
                obs[station_ind,:] = np.ma.masked
            # New station
            station_obj_id.append(row['obj_identifier'])
            station_id.append(row['station_id'])
            station_name.append(row['name'])
            station_lon.append(row['lon'])
            station_lat.append(row['lat'])
            station_elevation.append(row['elevation'])
            station_rec_elevation.append(row['recorded_elevation'])

            current_station_obj_id = station_obj_id[station_ind]

        # Add the observation to the obs array.
        time_diff = row['date'] - begin_datetime
        time_ind = time_diff.days * 24 + time_diff.seconds // 3600
        obs[station_ind, time_ind] = row['obs_swe_mm']

    num_stations = station_ind + 1

    obs_datetime = [begin_datetime +
                    dt.timedelta(hours=i) for i in range(num_hours)]

    # Place results in a dictionary.
    obs_swe = {'num_stations': num_stations,
               'num_hours': num_hours,
               'station_obj_id': station_obj_id,
               'station_id': station_id,
               'station_name': station_name,
               'station_lon': station_lon,
               'station_lat': station_lat,
               'station_elevation': station_elevation,
               'station_rec_elevation': station_rec_elevation,
               'obs_datetime': obs_datetime,
               'values_mm': obs}

    # Create the pkl file if all data fetched is more than 60 days earlier
    # than the current date/time.
    lag = dt.datetime.utcnow() - end_datetime
    if lag > dt.timedelta(days=60):
        file_obj = open(file_name, 'wb')
        pkl.dump(obs_swe, file_obj)
        file_obj.close()

    return(obs_swe)


def get_swe_obs_df(begin_datetime,
                   end_datetime,
                   no_data_value=-99999.0,
                   bounding_box=None,
                   scratch_dir=None,
                   target_hour=None,
                   hr_range=None,
                   verbose=None):

    """
    Get hourly snow water equivalent observations from the "web_data" database
    on wdb0. Return results in a pandas dataframe.
    Note that observations from the end datetime ARE included. For
    example, if begin_datetime is 2019-01-01 00 and end_datetime is
    2019-01-31 23, then 31 * 24 = 744 hours of data are gathered, from
    2019-01-01 00 to 2019-01-31 23.
    """

    file_name = 'wdb0_obs_snow_water_equivalent_df_' + \
                begin_datetime.strftime('%Y%m%d%H') + \
                '_to_' + \
                end_datetime.strftime('%Y%m%d%H') + \
                '.pkl'

    #print(scratch_dir, file_name)
    #print(type(scratch_dir), type(file_name))
    
    if scratch_dir is not None:
        file_name = os.path.join(scratch_dir, file_name)

    if os.path.isfile(file_name) and target_hour is None:

        # Retrieve data from pkl file and return.
        file_obj = open(file_name, 'rb')
        obs_swe_df = pkl.load(file_obj)
        file_obj.close()
        return(obs_swe_df)

    time_range = end_datetime - begin_datetime
    num_hours = time_range.days * 24 + time_range.seconds // 3600 + 1

    # Open the web database.
    conn_string = "host='wdb0.dmz.nohrsc.noaa.gov' dbname='web_data'"
    conn = psycopg2.connect(conn_string)
    conn.set_client_encoding("utf-8")
    cursor = conn.cursor()

    hour_limit_str = ''
    if target_hour is not None:
      from_hour = target_hour - hr_range[0]
      to_hour = target_hour + hr_range[1]
      if from_hour < 0: from_hour = 24 - from_hour
      if to_hour > 23: to_hour = to_hour - 24
      from_hour_str = '{}'.format(str(int(from_hour)).zfill(2))
      to_hour_str = '{}'.format(str(int(to_hour)).zfill(2))
      #print('from {} to {}'.format(int(from_hour), int(to_hour)))
      #print('hour string is {}'.format(str(int(from_hour)).zfill(2)))

    if to_hour > from_hour:
      hour_limit_str = " AND (DATE_PART('hour', date) " + \
                      ">='" + from_hour_str + "' AND " + \
                       " DATE_PART('hour', date) " + \
                      "<='" + to_hour_str + "') "
    else:
      hour_limit_str = " AND ((DATE_PART('hour', date) " + \
                       ">='" + from_hour_str + "' AND " + \
                       "DATE_PART('hour', date) " + \
                       "<='23')"  + " OR " + \
                       " (DATE_PART('hour', date) " + \
                       ">='00'" + " AND " + \
                       " DATE_PART('hour', date) " + \
                       "<='" + to_hour_str + "')) "


    # Define a SQL statement.
    sql_cmd = 'SELECT ' + \
              'point.allstation.obj_identifier, ' + \
              'TRIM(point.allstation.station_id), ' + \
              'TRIM(point.allstation.name), ' + \
              'point.allstation.coordinates[0] AS lon, ' + \
              'point.allstation.coordinates[1] AS lat, ' + \
              'point.allstation.elevation, ' + \
              'point.allstation.recorded_elevation, ' + \
              'date, ' + \
              'value * 1000.0 AS obs_swe_mm ' + \
              'FROM point.allstation, ' + \
              'point.obs_swe ' + \
              'WHERE date >= \'' + \
              begin_datetime.strftime('%Y-%m-%d %H:%M:%S') + \
              '\' ' + \
              'AND date <= \'' + \
              end_datetime.strftime('%Y-%m-%d %H:%M:%S') + \
              '\' ' + \
              hour_limit_str + \
              'AND point.allstation.obj_identifier = ' + \
              'point.obs_swe.obj_identifier ' + \
              'AND value IS NOT NULL '

    print('from python', bounding_box['min_lat'], bounding_box['max_lat'],
          bounding_box['min_lon'], bounding_box['max_lon'])
    if bounding_box is not None:
        sql_cmd = sql_cmd + \
                  'AND point.allstation.coordinates[1] >= ' + \
                  '{} '.format(bounding_box['min_lat']) + \
                  'AND point.allstation.coordinates[1] < ' + \
                  '{} '.format(bounding_box['max_lat']) + \
                  'AND point.allstation.coordinates[0] >= ' + \
                  '{} '.format(bounding_box['min_lon']) + \
                  'AND point.allstation.coordinates[0] < ' + \
                  '{} '.format(bounding_box['max_lon'])

    sql_cmd = sql_cmd + ' ORDER BY obj_identifier, date;'

    print('wdb0 query', sql_cmd)

    if verbose:
        print('INFO: psql command "{}"'.format(sql_cmd))
        print('\nBounding box=', bounding_box, type(bounding_box))
        #print('\nwdb0 hour_limit_str:', hour_limit_str)

    cursor.execute(sql_cmd)

    # The result below is just a huge list of tuples.
    obs_swe = cursor.fetchall()

    obs_swe_column_list = ['obj_identifier',
                           'station_id',
                           'name',
                           'lon',
                           'lat',
                           'elevation',
                           'recorded_elevation',
                           'date',
                           'obs_swe_mm']

    obs_swe_df = pd.DataFrame(obs_swe, columns=obs_swe_column_list)
    conn.close()

    # Create the pkl file if all data fetched is more than 60 days earlier
    # than the current date/time.
    lag = dt.datetime.utcnow() - end_datetime
    if lag > dt.timedelta(days=60):
        file_obj = open(file_name, 'wb')
        pkl.dump(obs_swe_df, file_obj)
        file_obj.close()

    return(obs_swe_df)


def get_prev_snow_depth_obs(target_datetime,
                            num_hrs_prev,
                            no_data_value=-99999.0,
                            bounding_box=None,
                            scratch_dir=None,
                            verbose=None):

    """
    Get hourly snow depth observations from the "web_data" database on wdb0
    for num_hrs_prev for those stations that report snow depth at
    target_datetime.
    The idea here is to limit the preceding observations we collect to those
    from stations that report snow depth at target_datetime, and to ignore all
    others.
    """

    begin_datetime = target_datetime - dt.timedelta(hours=num_hrs_prev)
    end_datetime = target_datetime - dt.timedelta(hours=1)

    # Only use .pkl files if there is no bounding box.
    if bounding_box is None:

        file_name = 'wdb0_obs_snow_depth_' + \
                    '{}_hours_prior_to_'.format(num_hrs_prev) + \
                    target_datetime.strftime('%Y%m%d%H') + \
                    '_snow_depth' + \
                    '.pkl'

        if scratch_dir is not None:
            file_name = os.path.join(scratch_dir, file_name)

            if os.path.isfile(file_name):

                # Retrieve data from pkl file and return.
                file_obj = open(file_name, 'rb')
                obs_snow_depth = pkl.load(file_obj)
                file_obj.close()
                return(obs_snow_depth)

    time_range = end_datetime - begin_datetime
    num_hours = time_range.days * 24 + time_range.seconds // 3600 + 1

    # Open the web database.
    conn_string = "host='wdb0.dmz.nohrsc.noaa.gov' dbname='web_data'"
    conn = psycopg2.connect(conn_string)
    conn.set_client_encoding("utf-8")
    cursor = conn.cursor()

    # Define a SQL statement.
    sql_cmd = 'SELECT ' + \
              't1.obj_identifier, ' + \
              'TRIM(t3.station_id), ' + \
              'TRIM(t3.name), ' + \
              't3.coordinates[0] AS lon, ' + \
              't3.coordinates[1] AS lat, ' + \
              't3.elevation, ' + \
              't3.recorded_elevation, ' + \
              't2.date, ' + \
              't2.value * 100.0 AS obs_snow_depth_cm ' + \
              'FROM ' + \
              '(' + \
              'SELECT obj_identifier ' + \
              'FROM point.obs_snow_depth ' + \
              'WHERE date = \'' + \
              target_datetime.strftime('%Y-%m-%d %H:%M:%S') + \
              '\' ' + \
              'AND value IS NOT NULL ' + \
              'GROUP BY obj_identifier' + \
              ') ' + \
              'AS t1, ' + \
              'point.obs_snow_depth AS t2, ' + \
              'point.allstation AS t3 ' + \
              'WHERE t2.date >= \'' + \
              begin_datetime.strftime('%Y-%m-%d %H:%M:%S') + \
              '\' ' + \
              'AND t2.date <= \'' + \
              end_datetime.strftime('%Y-%m-%d %H:%M:%S') + \
              '\' ' + \
              'AND t1.obj_identifier = t2.obj_identifier ' + \
              'AND t2.obj_identifier = t3.obj_identifier ' + \
              'AND t3.obj_identifier = t1.obj_identifier ' + \
              'AND t2.value IS NOT NULL '

    if bounding_box is not None:
        sql_cmd = sql_cmd + \
                  'AND t3.coordinates[0] >= ' + \
                  '{} '.format(bounding_box[0]) + \
                  'AND t3.coordinates[0] < ' + \
                  '{} '.format(bounding_box[1]) + \
                  'AND t3.coordinates[1] >= ' + \
                  '{} '.format(bounding_box[2]) + \
                  'AND t3.coordinates[1] < ' + \
                  '{} '.format(bounding_box[3])

    sql_cmd = sql_cmd + 'ORDER BY t1.obj_identifier, t2.date;'

    if verbose:
        print('\nINFO: psql command "{}"\n'.format(sql_cmd))

    cursor.execute(sql_cmd)

    # The result below is just a huge list of tuples.
    obs_depth = cursor.fetchall()

    obs_depth_column_list = ['obj_identifier',
                             'station_id',
                             'name',
                             'lon',
                             'lat',
                             'elevation',
                             'recorded_elevation',
                             'date',
                             'obs_snow_depth_cm']

    df = pd.DataFrame(obs_depth, columns=obs_depth_column_list)

    # This section organizes the query results into lists and arrays.

    station_ind = -1
    current_station_obj_id = -1
    station_obj_id = []
    station_id = []
    station_name = []
    station_lon = []
    station_lat = []
    station_elevation = []
    station_rec_elevation = []
    # Create a 2-d [time, station] array.
    obs = np.ma.empty([1, num_hours], dtype=float)
    obs[0,:] = no_data_value
    obs[0,:] = np.ma.masked
    for ind, row in df.iterrows():
        if row['obj_identifier'] != current_station_obj_id:
            station_ind += 1
            if station_ind > 0:
                # Just finished all data for previous station.
                obs = np.ma.append(obs,
                                   np.ma.empty([1,
                                                num_hours],
                                               dtype=float),
                                   axis=0)
                obs[station_ind,:] = no_data_value
                obs[station_ind,:] = np.ma.masked
            # New station
            station_obj_id.append(row['obj_identifier'])
            station_id.append(row['station_id'])
            station_name.append(row['name'])
            station_lon.append(row['lon'])
            station_lat.append(row['lat'])
            station_elevation.append(row['elevation'])
            station_rec_elevation.append(row['recorded_elevation'])

            current_station_obj_id = station_obj_id[station_ind]

        # Add the observation to the obs array.
        time_diff = row['date'] - begin_datetime
        time_ind = time_diff.days * 24 + time_diff.seconds // 3600

        try:
            obs[station_ind, time_ind] = row['obs_snow_depth_cm']
        except:
            print(num_hours)
            print(station_ind, time_ind)
            print(obs)
            print(obs.shape)
            print(row)
            exit(1)

    num_stations = station_ind + 1

    obs_datetime = [begin_datetime +
                    dt.timedelta(hours=i) for i in range(num_hours)]

    # Place results in a dictionary.
    obs_snow_depth = {'num_stations': num_stations,
                      'num_hours': num_hours,
                      'station_obj_id': station_obj_id,
                      'station_id': station_id,
                      'station_name': station_name,
                      'station_lon': station_lon,
                      'station_lat': station_lat,
                      'station_elevation': station_elevation,
                      'station_rec_elevation': station_rec_elevation,
                      'obs_datetime': obs_datetime,
                      'values_cm': obs}

    # Create the pkl file if all data fetched is more than 60 days earlier
    # than the current date/time.
    lag = dt.datetime.utcnow() - end_datetime
    if bounding_box is None and \
       lag > dt.timedelta(days=60):
        file_obj = open(file_name, 'wb')
        pkl.dump(obs_snow_depth, file_obj)
        file_obj.close()
        if verbose:
            print('INFO: wrote query results to {}.'.format(file_name))

    return(obs_snow_depth)


def get_prev_swe_obs(target_datetime,
                     num_hrs_prev,
                     no_data_value=-99999.0,
                     bounding_box=None,
                     scratch_dir=None,
                     verbose=None):

    """
    Get hourly snow water equivalent observations from the "web_data" database
    on wdb0 for num_hrs_prev for those stations that report snow water
    equivalent at target_datetime.
    The idea here is to limit the preceding observations we collect to those
    from stations that report snow water equivalent at target_datetime, and to
    ignore all others.
    """

    begin_datetime = target_datetime - dt.timedelta(hours=num_hrs_prev)
    end_datetime = target_datetime - dt.timedelta(hours=1)

    # Only use .pkl files if there is no bounding box.
    if bounding_box is None:

        file_name = 'wdb0_obs_swe_' + \
                    '{}_hours_prior_to_'.format(num_hrs_prev) + \
                    target_datetime.strftime('%Y%m%d%H') + \
                    '_swe' + \
                    '.pkl'

        if scratch_dir is not None:
            file_name = os.path.join(scratch_dir, file_name)

            if os.path.isfile(file_name):

                # Retrieve data from pkl file and return.
                file_obj = open(file_name, 'rb')
                obs_swe = pkl.load(file_obj)
                file_obj.close()
                return(obs_swe)

    time_range = end_datetime - begin_datetime
    num_hours = time_range.days * 24 + time_range.seconds // 3600 + 1

    # Open the web database.
    conn_string = "host='wdb0.dmz.nohrsc.noaa.gov' dbname='web_data'"
    conn = psycopg2.connect(conn_string)
    conn.set_client_encoding("utf-8")
    cursor = conn.cursor()

    # Define a SQL statement.
    sql_cmd = 'SELECT ' + \
              't1.obj_identifier, ' + \
              'TRIM(t3.station_id), ' + \
              'TRIM(t3.name), ' + \
              't3.coordinates[0] AS lon, ' + \
              't3.coordinates[1] AS lat, ' + \
              't3.elevation, ' + \
              't3.recorded_elevation, ' + \
              't2.date, ' + \
              't2.value * 1000.0 AS obs_swe_mm ' +\
              'FROM ' + \
              '(' + \
              'SELECT obj_identifier ' + \
              'FROM point.obs_swe ' + \
              'WHERE date = \'' + \
              target_datetime.strftime('%Y-%m-%d %H:%M:%S') + \
              '\' ' + \
              'AND value IS NOT NULL ' + \
              'GROUP BY obj_identifier' + \
              ') ' + \
              'AS t1, ' + \
              'point.obs_swe AS t2, ' + \
              'point.allstation AS t3 ' + \
              'WHERE t2.date >= \'' + \
              begin_datetime.strftime('%Y-%m-%d %H:%M:%S') + \
              '\' ' + \
              'AND t2.date <= \'' + \
              end_datetime.strftime('%Y-%m-%d %H:%M:%S') + \
              '\' ' + \
              'AND t1.obj_identifier = t2.obj_identifier ' + \
              'AND t2.obj_identifier = t3.obj_identifier ' + \
              'AND t3.obj_identifier = t1.obj_identifier ' + \
              'AND t2.value IS NOT NULL '

    if bounding_box is not None:
        sql_cmd = sql_cmd + \
                  'AND t3.coordinates[0] >= ' + \
                  '{} '.format(bounding_box[0]) + \
                  'AND t3.coordinates[0] < ' + \
                  '{} '.format(bounding_box[1]) + \
                  'AND t3.coordinates[1] >= ' + \
                  '{} '.format(bounding_box[2]) + \
                  'AND t3.coordinates[1] < ' + \
                  '{} '.format(bounding_box[3])

    sql_cmd = sql_cmd + 'ORDER BY t1.obj_identifier, t2.date;'

    if verbose:
        print('INFO: psql command "{}"'.format(sql_cmd))

    cursor.execute(sql_cmd)

    # The result below is just a huge list of tuples.
    obs_swe = cursor.fetchall()

    obs_swe_column_list = ['obj_identifier',
                           'station_id',
                           'name',
                           'lon',
                           'lat',
                           'elevation',
                           'recorded_elevation',
                           'date',
                           'obs_swe_mm']

    df = pd.DataFrame(obs_swe, columns=obs_swe_column_list)

    # This section organizes the query results into lists and arrays.

    station_ind = -1
    current_station_obj_id = -1
    station_obj_id = []
    station_id = []
    station_name = []
    station_lon = []
    station_lat = []
    station_elevation = []
    station_rec_elevation = []
    # Create a 2-d [time, station] array.
    obs = np.ma.empty([1, num_hours], dtype=float)
    obs[0,:] = no_data_value
    obs[0,:] = np.ma.masked
    for ind, row in df.iterrows():
        if row['obj_identifier'] != current_station_obj_id:
            station_ind += 1
            if station_ind > 0:
                # Just finished all data for previous station.
                obs = np.ma.append(obs,
                                   np.ma.empty([1,
                                                num_hours],
                                               dtype=float),
                                   axis=0)
                obs[station_ind,:] = no_data_value
                obs[station_ind,:] = np.ma.masked
            # New station
            station_obj_id.append(row['obj_identifier'])
            station_id.append(row['station_id'])
            station_name.append(row['name'])
            station_lon.append(row['lon'])
            station_lat.append(row['lat'])
            station_elevation.append(row['elevation'])
            station_rec_elevation.append(row['recorded_elevation'])

            current_station_obj_id = station_obj_id[station_ind]

        # Add the observation to the obs array.
        time_diff = row['date'] - begin_datetime
        time_ind = time_diff.days * 24 + time_diff.seconds // 3600

        # try:
        obs[station_ind, time_ind] = row['obs_swe_mm']
        # except:
        #     print(num_hours)
        #     print(station_ind, time_ind)
        #     print(obs)
        #     print(obs.shape)
        #     print(row)
        #     exit(1)

    num_stations = station_ind + 1

    obs_datetime = [begin_datetime +
                    dt.timedelta(hours=i) for i in range(num_hours)]

    # Place results in a dictionary.
    obs_swe = {'num_stations': num_stations,
               'num_hours': num_hours,
               'station_obj_id': station_obj_id,
               'station_id': station_id,
               'station_name': station_name,
               'station_lon': station_lon,
               'station_lat': station_lat,
               'station_elevation': station_elevation,
               'station_rec_elevation': station_rec_elevation,
               'obs_datetime': obs_datetime,
               'values_mm': obs}

    # Create the pkl file if all data fetched is more than 60 days earlier
    # than the current date/time.
    lag = dt.datetime.utcnow() - end_datetime
    if bounding_box is None and \
       lag > dt.timedelta(days=60):
        file_obj = open(file_name, 'wb')
        pkl.dump(obs_swe, file_obj)
        file_obj.close()
        if verbose:
            print('INFO: wrote query results to {}.'.format(file_name))

    return(obs_swe)


def get_air_temp_obs(begin_datetime,
                     end_datetime,
                     no_data_value=-99999.0,
                     bounding_box=None,
                     scratch_dir=None,
                     verbose=None,
                     prev_obs_air_temp=None,
                     read_pkl=True,
                     write_pkl=True,
                     station_obj_id=None):

    """
    Get hourly air temperature observations from the "web_data" database on
    wdb0.
    """

    logger = logging.getLogger()

    file_name = 'wdb0_obs_air_temp_' + \
        begin_datetime.strftime('%Y%m%d%H') + \
        '_to_' + \
        end_datetime.strftime('%Y%m%d%H') + \
        '.pkl'

    if scratch_dir is not None:
        file_name = os.path.join(scratch_dir, file_name)

    # Only read data from a .pkl file if there is no bounding box.
    if bounding_box is None and \
       read_pkl is True and \
       station_obj_id is None:

        if os.path.isfile(file_name):

            # Retrieve data from pkl file and return.
            logger.debug('Fetching data from {}.'.format(file_name))
            file_obj = open(file_name, 'rb')
            obs_air_temp = pkl.load(file_obj)
            file_obj.close()
            return(obs_air_temp)

    # Define the list of datetimes covering [begin_datetime, end_datetime]
    time_range = end_datetime - begin_datetime
    num_hours = time_range.days * 24 + time_range.seconds // 3600 + 1
    obs_datetime = [begin_datetime +
                    dt.timedelta(hours=i) for i in range(num_hours)]

    # if prev_obs_air_temp is None:
    #     logger.debug('none 01')
    #     xxx = input()
    # logger.debug(prev_obs_air_temp is not None)

    if prev_obs_air_temp is not None:

        # Determine whether the prev_obs_air_temp.obs_datetime overlaps with a
        # portion of either the beginning or the end of obs_datetime.
        prev_obs_datetime = prev_obs_air_temp['obs_datetime']
        prev_begin_datetime = prev_obs_datetime[0]
        prev_end_datetime = prev_obs_datetime[-1]
        # Gather datetimes that exist in both obs_datetime and
        # prev_obs_datetime.
        dt_in_both = sorted(list(set(prev_obs_datetime) & set(obs_datetime)))

        if len(dt_in_both) == 0:
            # No time overlap; data from prev_obs_air_temp cannot be used.
            logger.warning('No data overlap between requested date/times ' +
                           'and previous observations.')
            prev_obs_air_temp = None
        else:
            # Verify that the list of new datetimes for which we already have
            # data is coterminous with either the beginning or the end of
            # obs_datetime, which keeps the function simpler.
            obs_dt_from_prev_obs_dt = \
                [obs_datetime.index(i) for i in dt_in_both]
            if (obs_dt_from_prev_obs_dt[0] != 0) and \
               (obs_dt_from_prev_obs_dt[-1] != (num_hours - 1)):
                logger.warning('Ignoring previous air temperature ' +
                               'observations.')
                prev_obs_air_temp = None
            # else:
            #     logger.debug('Previous air temperature observations okay.')

    # else:
    #     logger.debug('No previous air temperature observations.')
    #     logger.debug(prev_obs_air_temp)

    # if prev_obs_air_temp is None:
    #     logger.debug('none 02')
    #     xxx = input()

    if prev_obs_air_temp is not None:

        # Identify the datetimes for which new data is needed.
        dt_needed = sorted(list(set(obs_datetime) - set(prev_obs_datetime)))
        if len(dt_needed) > 0:
            curr_begin_datetime = dt_needed[0]
            curr_end_datetime = dt_needed[-1]
            curr_num_hours = len(dt_needed)
            # Verify the time range is contiguous.
            time_range_needed = dt_needed[-1] - dt_needed[0]
            num_hours_needed = time_range_needed.days * 24 + \
                time_range_needed.seconds // 3600 + 1
        else:            
            # We do not need to get any new data? Figure out which indices of
            # obs_datetime match prev_obs_datetime, subset prev_obs_datetime
            # for those, and return the result. No .pkl file will be created.
            prev_obs_in_obs = \
                [prev_obs_datetime.index(i) for i in obs_datetime]
            obs = prev_obs_air_temp['values_deg_c'][:,prev_obs_in_obs]
            obs_air_temp = prev_obs_air_temp
            obs_air_temp['num_hours'] = num_hours
            obs_air_temp['values_deg_c'] = obs
            return(obs_air_temp)

        if num_hours_needed != curr_num_hours:
            logger.warning('Date/times needed are non-contiguous ' +
                           'if previous datetimes are used. ' +
                           'Will fetch all data instead.')
            prev_obs_air_temp = None
        else:
            # Identify datetimes for new data and their indices in the full
            # time series.
            # curr_obs_datetime = [curr_begin_datetime +
            #                      dt.timedelta(hours=i)
            #                      for i in range(curr_num_hours)]
            curr_obs_datetime = dt_needed
            obs_dt_from_curr_obs_dt = [obs_datetime.index(i)
                                       for i in curr_obs_datetime]

    else:

        curr_begin_datetime = begin_datetime
        curr_end_datetime = end_datetime
        curr_num_hours = num_hours
        curr_obs_datetime = obs_datetime

    # if prev_obs_air_temp is None:
    #     logger.debug('none 03')
    #     xxx = input()

    # Open the web database.
    conn_string = "host='wdb0.dmz.nohrsc.noaa.gov' dbname='web_data'"
    conn = psycopg2.connect(conn_string)
    conn.set_client_encoding("utf-8")
    cursor = conn.cursor()

    # Define a SQL statement.
    if station_obj_id is None:
        sql_cmd = 'SELECT ' + \
                  'point.allstation.obj_identifier, ' + \
                  'TRIM(point.allstation.station_id), ' + \
                  'TRIM(point.allstation.name), ' + \
                  'point.allstation.coordinates[0] as lon, ' + \
                  'point.allstation.coordinates[1] as lat, ' + \
                  'point.allstation.elevation, ' + \
                  'point.allstation.recorded_elevation, ' + \
                  'date, ' + \
                  'value AS obs_air_temp_deg_c ' + \
                  'FROM point.allstation, point.obs_airtemp ' + \
                  'WHERE date >= \'' + \
                  curr_begin_datetime.strftime('%Y-%m-%d %H:%M:%S') + \
                  '\' ' + \
                  'AND date <= \'' + \
                  curr_end_datetime.strftime('%Y-%m-%d %H:%M:%S') + \
                  '\' ' + \
                  'AND point.allstation.obj_identifier = ' + \
                  'point.obs_airtemp.obj_identifier ' + \
                  'AND value IS NOT NULL '
    else:
        sql_cmd = 'SELECT ' + \
                  'point.allstation.obj_identifier, ' + \
                  'TRIM(point.allstation.station_id), ' + \
                  'TRIM(point.allstation.name), ' + \
                  'point.allstation.coordinates[0] as lon, ' + \
                  'point.allstation.coordinates[1] as lat, ' + \
                  'point.allstation.elevation, ' + \
                  'point.allstation.recorded_elevation, ' + \
                  'date, ' + \
                  'value AS obs_air_temp_deg_c ' + \
                  'FROM point.allstation, point.obs_airtemp ' + \
                  'WHERE point.allstation.obj_identifier = ' + \
                  '\'{}\' '.format(station_obj_id) + \
                  'AND date >= \'' + \
                  curr_begin_datetime.strftime('%Y-%m-%d %H:%M:%S') + \
                  '\' ' + \
                  'AND date <= \'' + \
                  curr_end_datetime.strftime('%Y-%m-%d %H:%M:%S') + \
                  '\' ' + \
                  'AND point.allstation.obj_identifier = ' + \
                  'point.obs_airtemp.obj_identifier ' + \
                  'AND value IS NOT NULL '

    if bounding_box is not None:
        sql_cmd = sql_cmd + \
                  'AND point.allstation.coordinates[0] >= ' + \
                  '{} '.format(bounding_box[0]) + \
                  'AND point.allstation.coordinates[0] < ' + \
                  '{} '.format(bounding_box[1]) + \
                  'AND point.allstation.coordinates[1] >= ' + \
                  '{} '.format(bounding_box[2]) + \
                  'AND point.allstation.coordinates[1] < ' + \
                  '{} '.format(bounding_box[3])

    sql_cmd = sql_cmd + 'ORDER BY obj_identifier, date;'

    logger.debug('psql command "{}"'.format(sql_cmd))

    cursor.execute(sql_cmd)

    # The result below is just a huge list of tuples.
    fetched_airtemp = cursor.fetchall()

    obs_air_temp_column_list = ['obj_identifier',
                                'station_id',
                                'name',
                                'lon',
                                'lat',
                                'elevation',
                                'recorded_elevation',
                                'date',
                                'obs_air_temp_deg_c']

    df = pd.DataFrame(fetched_airtemp, columns=obs_air_temp_column_list)

    # This section organizes the query results into lists and arrays.

    station_ind = -1
    this_station_obj_id = -1
    curr_station_obj_id = []
    curr_station_id = []
    curr_station_name = []
    curr_station_lon = []
    curr_station_lat = []
    curr_station_elev = []
    curr_station_rec_elev = []
    # Create a 2-d [time, station] array.
    curr_obs = np.ma.empty([1, curr_num_hours], dtype=float)
    curr_obs[0,:] = no_data_value
    curr_obs[0,:] = np.ma.masked
    # if station_obj_id is not None:
    #     logger.debug(df.dtypes)
    #     yyy = input()
    for ind, row in df.iterrows():
        if row['obj_identifier'] != this_station_obj_id:
            station_ind += 1
            if station_ind > 0:
                # Just finished all data for previous station.
                curr_obs = np.ma.append(curr_obs,
                                   np.ma.empty([1,
                                                curr_num_hours],
                                               dtype=float),
                                   axis=0)
                curr_obs[station_ind,:] = no_data_value
                curr_obs[station_ind,:] = np.ma.masked
            # New station
            curr_station_obj_id.append(row['obj_identifier'])
            curr_station_id.append(row['station_id'])
            curr_station_name.append(row['name'])
            curr_station_lon.append(np.float64(row['lon']))
            curr_station_lat.append(np.float64(row['lat']))
            curr_station_elev.append(np.int32(row['elevation']))
            curr_station_rec_elev.append(np.int32(row['recorded_elevation']))

            this_station_obj_id = curr_station_obj_id[station_ind]

        # Add the observation to the curr_obs array.
        time_diff = row['date'] - curr_begin_datetime
        time_ind = time_diff.days * 24 + time_diff.seconds // 3600

        curr_obs[station_ind, time_ind] = row['obs_air_temp_deg_c']

    curr_num_stations = station_ind + 1

    # Place results in a dictionary.
    if prev_obs_air_temp is not None:
        logger.debug('Using previous air temp observations.')
    logger.debug('num_hours: {}'.format(num_hours))
    logger.debug('curr_num_hours: {}'.format(curr_num_hours))
    curr_obs_air_temp = {'num_stations': curr_num_stations,
                         'num_hours': curr_num_hours,
                         'station_obj_id': curr_station_obj_id,
                         'station_id': curr_station_id,
                         'station_name': curr_station_name,
                         'station_lon': curr_station_lon,
                         'station_lat': curr_station_lat,
                         'station_elevation': curr_station_elev,
                         'station_rec_elevation': curr_station_rec_elev,
                         'obs_datetime': curr_obs_datetime,
                         'values_deg_c': curr_obs}

    if prev_obs_air_temp is not None:

        # Combine prev_obs_air_temp and curr_obs_air_temp.
        logger.debug('# prev stations: {}'.
                     format(prev_obs_air_temp['num_stations']))
        logger.debug('# current stations: {}'.
                     format(curr_obs_air_temp['num_stations']))
        prev_station_obj_id = prev_obs_air_temp['station_obj_id']
        new_station_obj_id = sorted(list(set(prev_station_obj_id) |
                                         set(curr_station_obj_id)))
        logger.debug('# combined: {}'.format(len(new_station_obj_id)))

        # Extract other "prev" station variables for shorter names.
        prev_station_id = prev_obs_air_temp['station_id']
        prev_station_name = prev_obs_air_temp['station_name']
        prev_station_lon = prev_obs_air_temp['station_lon']
        prev_station_lat = prev_obs_air_temp['station_lat']
        prev_station_elev = prev_obs_air_temp['station_elevation']
        prev_station_rec_elev = prev_obs_air_temp['station_rec_elevation']
        
        # Identify indices of previous and current object identifiers in the
        # combined list.
        # Using ".index" is very slow. Try to make this faster.
        new_station_from_prev = [new_station_obj_id.index(i)
                                 for i in prev_station_obj_id]
        new_station_from_curr = [new_station_obj_id.index(i)
                                 for i in curr_station_obj_id]

        # Create new station variable lists.
        new_num_stations = len(new_station_obj_id)

        new_station_id = np.array([''] * new_num_stations, dtype=object)
        new_station_id[new_station_from_prev] = prev_station_id
        new_station_id[new_station_from_curr] = curr_station_id
        new_station_id = list(new_station_id)

        new_station_name = np.array([''] * new_num_stations, dtype=object)
        new_station_name[new_station_from_prev] = prev_station_name
        new_station_name[new_station_from_curr] = curr_station_name
        new_station_name = list(new_station_name)

        new_station_lon = np.array([no_data_value] * new_num_stations,
                                   dtype=np.float64)
        new_station_lon[new_station_from_prev] = prev_station_lon
        new_station_lon[new_station_from_curr] = curr_station_lon
        new_station_lon = list(new_station_lon)

        new_station_lat = np.array([no_data_value] * new_num_stations,
                                   dtype=np.float64)
        new_station_lat[new_station_from_prev] = prev_station_lat
        new_station_lat[new_station_from_curr] = curr_station_lat
        new_station_lat = list(new_station_lat)

        new_station_elev = np.array([0] * new_num_stations, dtype=np.int32)
        new_station_elev[new_station_from_prev] = prev_station_elev
        new_station_elev[new_station_from_curr] = curr_station_elev
        new_station_elev = list(new_station_elev)

        new_station_rec_elev = np.array([0] * new_num_stations,
                                        dtype=np.int32)
        new_station_rec_elev[new_station_from_prev] = prev_station_rec_elev
        new_station_rec_elev[new_station_from_curr] = curr_station_rec_elev
        new_station_rec_elev = list(new_station_rec_elev)

        # si = 2000
        # for sc in range(si, si+16):
        #     logger.info('{:>6d}  {:>16}  {:>20.20}  {:>9.4f}  {:>8.4f}  '.
        #                 format(new_station_obj_id[sc],
        #                        new_station_id[sc],
        #                        new_station_name[sc],
        #                        new_station_lon[sc],
        #                        new_station_lat[sc]) +
        #                 '{:>5d}  {:>5d}'.
        #                 format(new_station_elev[sc],
        #                        new_station_rec_elev[sc]))

        prev_obs_in_obs = [prev_obs_datetime.index(i) for i in dt_in_both]
        prev_obs = prev_obs_air_temp['values_deg_c'][:,prev_obs_in_obs]

        obs = np.ma.empty([new_num_stations, num_hours], dtype=float)
        obs[:,:] = no_data_value
        obs[:,:] = np.ma.masked
        # logger.debug(new_station_obj_id[si])
        # logger.debug(obs[si,:])
        obs[np.ix_(new_station_from_prev, obs_dt_from_prev_obs_dt)] = \
            prev_obs
        # logger.debug(obs[si,:])
        obs[np.ix_(new_station_from_curr, obs_dt_from_curr_obs_dt)] = \
            curr_obs
        # logger.debug(obs[si,:])

        obs_air_temp = {'num_stations': new_num_stations,
                        'num_hours': num_hours,
                        'station_obj_id': new_station_obj_id,
                        'station_id': new_station_id,
                        'station_name': new_station_name,
                        'station_lon': new_station_lon,
                        'station_lat': new_station_lat,
                        'station_elevation': new_station_elev,
                        'station_rec_elevation': new_station_rec_elev,
                        'obs_datetime': obs_datetime,
                        'values_deg_c': obs}

    else:

        obs_air_temp = curr_obs_air_temp

    # Create the pkl file if all data fetched is more than 60 days earlier
    # than the current date/time, as long as write_pkl is not set to False.
    lag = dt.datetime.utcnow() - end_datetime
    if bounding_box is None and \
       station_obj_id is None and \
       lag > dt.timedelta(days=60) and \
       write_pkl is True:
        file_obj = open(file_name, 'wb')
        pkl.dump(obs_air_temp, file_obj)
        file_obj.close()
        logger.debug('Wrote query results to {}.'.format(file_name))

    return(obs_air_temp)


def get_prev_air_temp_obs(target_datetime,
                          num_hrs_prev,
                          no_data_value=-99999.0,
                          bounding_box=None,
                          scratch_dir=None,
                          verbose=None):

    """
    Get hourly air temperature observations from the "web_data" database on
    wdb0 for num_hrs_prev for those stations that report snow depth at
    target_datetime.
    The idea here is to limit the preceding observations we collect to those
    from stations that report snow depth at target_datetime, and to ignore all
    others.
    """

    begin_datetime = target_datetime - dt.timedelta(hours=num_hrs_prev)
    end_datetime = target_datetime - dt.timedelta(hours=1)

    # Only use .pkl files if there is no bounding box.
    if bounding_box is None:

        file_name = 'wdb0_obs_air_temp_' + \
                    '{}_hours_prior_to_'.format(num_hrs_prev) + \
                    target_datetime.strftime('%Y%m%d%H') + \
                    '_snow_depth' + \
                    '.pkl'

        if scratch_dir is not None:
            file_name = os.path.join(scratch_dir, file_name)

            if os.path.isfile(file_name):

                # Retrieve data from pkl file and return.
                file_obj = open(file_name, 'rb')
                obs_air_temp = pkl.load(file_obj)
                file_obj.close()
                return(obs_air_temp)

    time_range = end_datetime - begin_datetime
    num_hours = time_range.days * 24 + time_range.seconds // 3600 + 1

    # Open the web database.
    conn_string = "host='wdb0.dmz.nohrsc.noaa.gov' dbname='web_data'"
    conn = psycopg2.connect(conn_string)
    conn.set_client_encoding("utf-8")
    cursor = conn.cursor()

    # Define a SQL statement.
    sql_cmd = 'SELECT ' + \
              't1.obj_identifier, ' + \
              'TRIM(t3.station_id), ' + \
              'TRIM(t3.name), ' + \
              't3.coordinates[0] AS lon, ' + \
              't3.coordinates[1] AS lat, ' + \
              't3.elevation, ' + \
              't3.recorded_elevation, ' + \
              't2.date, ' + \
              't2.value AS obs_air_temp_deg_c ' + \
              'FROM ' + \
              '(' + \
              'SELECT obj_identifier ' + \
              'FROM point.obs_snow_depth ' + \
              'WHERE date = \'' + \
              target_datetime.strftime('%Y-%m-%d %H:%M:%S') + \
              '\' ' + \
              'AND value IS NOT NULL ' + \
              'GROUP BY obj_identifier' + \
              ') ' + \
              'AS t1, ' + \
              'point.obs_airtemp AS t2, ' + \
              'point.allstation AS t3 ' + \
              'WHERE t2.date >= \'' + \
              begin_datetime.strftime('%Y-%m-%d %H:%M:%S') + \
              '\' ' + \
              'AND t2.date <= \'' + \
              end_datetime.strftime('%Y-%m-%d %H:%M:%S') + \
              '\' ' + \
              'AND t1.obj_identifier = t2.obj_identifier ' + \
              'AND t2.obj_identifier = t3.obj_identifier ' + \
              'AND t3.obj_identifier = t1.obj_identifier ' + \
              'AND t2.value IS NOT NULL '

    if bounding_box is not None:
        sql_cmd = sql_cmd + \
                  'AND t3.coordinates[0] >= ' + \
                  '{} '.format(bounding_box[0]) + \
                  'AND t3.coordinates[0] < ' + \
                  '{} '.format(bounding_box[1]) + \
                  'AND t3.coordinates[1] >= ' + \
                  '{} '.format(bounding_box[2]) + \
                  'AND t3.coordinates[1] < ' + \
                  '{} '.format(bounding_box[3])

    sql_cmd = sql_cmd + 'ORDER BY t1.obj_identifier, t2.date;'

    if verbose:
        print('INFO: psql command "{}"'.format(sql_cmd))

    cursor.execute(sql_cmd)

    # The result below is just a huge list of tuples.
    fetched_airtemp = cursor.fetchall()

    obs_air_temp_column_list = ['obj_identifier',
                                'station_id',
                                'name',
                                'lon',
                                'lat',
                                'elevation',
                                'recorded_elevation',
                                'date',
                                'obs_air_temp_deg_c']

    df = pd.DataFrame(fetched_airtemp, columns=obs_air_temp_column_list)

    # This section organizes the query results into lists and arrays.

    station_ind = -1
    current_station_obj_id = -1
    station_obj_id = []
    station_id = []
    station_name = []
    station_lon = []
    station_lat = []
    station_elevation = []
    station_rec_elevation = []
    # Create a 2-d [time, station] array.
    obs = np.ma.empty([1, num_hours], dtype=float)
    obs[0,:] = no_data_value
    obs[0,:] = np.ma.masked
    for ind, row in df.iterrows():
        if row['obj_identifier'] != current_station_obj_id:
            print(station_ind)
            station_ind += 1
            if station_ind > 0:
                # Just finished all data for previous station.
                obs = np.ma.append(obs,
                                   np.ma.empty([1,
                                                num_hours],
                                               dtype=float),
                                   axis=0)
                obs[station_ind,:] = no_data_value
                obs[station_ind,:] = np.ma.masked
            # New station
            station_obj_id.append(row['obj_identifier'])
            station_id.append(row['station_id'])
            station_name.append(row['name'])
            station_lon.append(row['lon'])
            station_lat.append(row['lat'])
            station_elevation.append(row['elevation'])
            station_rec_elevation.append(row['recorded_elevation'])

            current_station_obj_id = station_obj_id[station_ind]

        # Add the observation to the obs array.
        time_diff = row['date'] - begin_datetime
        time_ind = time_diff.days * 24 + time_diff.seconds // 3600

        try:
            obs[station_ind, time_ind] = row['obs_air_temp_deg_c']
        except:
            print(num_hours)
            print(station_ind, time_ind)
            print(obs)
            print(obs.shape)
            print(row)
            exit(1)

    num_stations = station_ind + 1

    obs_datetime = [begin_datetime +
                    dt.timedelta(hours=i) for i in range(num_hours)]

    # Place results in a dictionary.
    obs_air_temp = {'num_stations': num_stations,
                    'num_hours': num_hours,
                    'station_obj_id': station_obj_id,
                    'station_id': station_id,
                    'station_name': station_name,
                    'station_lon': station_lon,
                    'station_lat': station_lat,
                    'station_elevation': station_elevation,
                    'station_rec_elevation': station_rec_elevation,
                    'obs_datetime': obs_datetime,
                    'values_deg_c': obs}

    # Create the pkl file if all data fetched is more than 60 days earlier
    # than the current date/time.
    lag = dt.datetime.utcnow() - end_datetime
    if bounding_box is None and \
       lag > dt.timedelta(days=60):
        file_obj = open(file_name, 'wb')
        pkl.dump(obs_air_temp, file_obj)
        file_obj.close()
        if verbose:
            print('INFO: wrote query results to {}.'.format(file_name))

    return(obs_air_temp)


def get_snwd_snfl_obs(target_datetime,
                      duration_hours,
                      no_data_value=-99999.0,
                      bounding_box=None,
                      scratch_dir=None,
                      verbose=None):

    """
    Get snowfall observations from the "web_data" database on wdb0 having
    duration of duration_hours, an observation time of target_datetime, and
    limit the query to those associated with observed snow depth at
    target_datetime.
    The idea here is to limit the snowfall observations we collect to those
    from stations that report snow depth at target_datetime, and to ignore all
    others.
    """

    # Only use .pkl files if there is no bounding box.
    if bounding_box is None:

        file_name = 'wdb0_obs_snowfall_' + \
                    '{}_hours_ending_'.format(duration_hours) + \
                    target_datetime.strftime('%Y%m%d%H') + \
                    '_snow_depth' + \
                    '.pkl'

        if scratch_dir is not None:
            file_name = os.path.join(scratch_dir, file_name)

            if os.path.isfile(file_name):

                # Retrieve data from pkl file and return.
                file_obj = open(file_name, 'rb')
                obs_snowfall = pkl.load(file_obj)
                file_obj.close()
                return(obs_snowfall)

    # Open the web database.
    conn_string = "host='wdb0.dmz.nohrsc.noaa.gov' dbname='web_data'"
    conn = psycopg2.connect(conn_string)
    conn.set_client_encoding("utf-8")
    cursor = conn.cursor()

    # Define a SQL statement.
    sql_cmd = 'SELECT ' + \
              't1.obj_identifier, ' + \
              'TRIM(t3.station_id), ' + \
              'TRIM(t3.name), ' + \
              't3.coordinates[0] AS lon, ' + \
              't3.coordinates[1] AS lat, ' + \
              't3.elevation, ' + \
              't3.recorded_elevation, ' + \
              't2.date, ' + \
              't2.value * 100.0 AS obs_snowfall_cm ' + \
              'FROM ' + \
              '(' + \
              'SELECT obj_identifier ' + \
              'FROM point.obs_snow_depth ' + \
              'WHERE date = \'' + \
              target_datetime.strftime('%Y-%m-%d %H:%M:%S') + \
              '\' ' + \
              'AND value IS NOT NULL ' + \
              'GROUP BY obj_identifier' + \
              ') ' + \
              'AS t1, ' + \
              'point.obs_snowfall_raw AS t2, ' + \
              'point.allstation AS t3 ' + \
              'WHERE t2.date = \'' + \
              target_datetime.strftime('%Y-%m-%d %H:%M:%S') + \
              '\' ' + \
              'AND t2.duration = {} '.format(duration_hours * 3600) + \
              'AND t1.obj_identifier = t2.obj_identifier ' + \
              'AND t2.obj_identifier = t3.obj_identifier ' + \
              'AND t3.obj_identifier = t1.obj_identifier ' + \
              'AND t2.value IS NOT NULL '

    if bounding_box is not None:
        sql_cmd = sql_cmd + \
                  'AND t3.coordinates[0] >= ' + \
                  '{} '.format(bounding_box[0]) + \
                  'AND t3.coordinates[0] < ' + \
                  '{} '.format(bounding_box[1]) + \
                  'AND t3.coordinates[1] >= ' + \
                  '{} '.format(bounding_box[2]) + \
                  'AND t3.coordinates[1] < ' + \
                  '{} '.format(bounding_box[3])

    sql_cmd = sql_cmd + 'ORDER BY t1.obj_identifier, t2.date;'

    if verbose:
        print('INFO: psql command "{}"'.format(sql_cmd))

    cursor.execute(sql_cmd)

    # The result below is just a huge list of tuples.
    obs_snowfall = cursor.fetchall()

    obs_snowfall_column_list = ['obj_identifier',
                                'station_id',
                                'name',
                                'lon',
                                'lat',
                                'elevation',
                                'recorded_elevation',
                                'date',
                                'obs_snowfall_cm']

    df = pd.DataFrame(obs_snowfall, columns=obs_snowfall_column_list)

    # This section organizes the query results into lists and arrays.

    station_ind = -1
    current_station_obj_id = -1
    station_obj_id = []
    station_id = []
    station_name = []
    station_lon = []
    station_lat = []
    station_elevation = []
    station_rec_elevation = []
    # Create a 2-d [time, station] array.
    # GF - 2nd dim used to be num_hours
    obs = np.ma.empty([1], dtype=float)
    obs[0] = no_data_value
    obs[0] = np.ma.masked
    # print(len(df))
    for ind, row in df.iterrows():
        if row['obj_identifier'] != current_station_obj_id:
            station_ind += 1
            if station_ind > 0:
                # Just finished all data for previous station.
                # GF - 2nd dim used to be num_hours
                obs = np.ma.append(obs,
                                   np.ma.empty([1],
                                               dtype=float),
                                   axis=0)
                obs[station_ind] = no_data_value
                obs[station_ind] = np.ma.masked
            # New station
            station_obj_id.append(row['obj_identifier'])
            station_id.append(row['station_id'])
            station_name.append(row['name'])
            station_lon.append(row['lon'])
            station_lat.append(row['lat'])
            station_elevation.append(row['elevation'])
            station_rec_elevation.append(row['recorded_elevation'])

            current_station_obj_id = station_obj_id[station_ind]

        # print(type(row['obj_identifier']))
        # if row['obj_identifier'] == 11929:
        #     print(row)
        #     print(row['obs_snowfall_cm'])

        try:
            obs[station_ind] = row['obs_snowfall_cm']
        except:
            print(obs)
            print(obs.shape)
            print(row)
            exit(1)

    num_stations = station_ind + 1

    # Place results in a dictionary.
    obs_snowfall = {'num_stations': num_stations,
                    'station_obj_id': station_obj_id,
                    'station_id': station_id,
                    'station_name': station_name,
                    'station_lon': station_lon,
                    'station_lat': station_lat,
                    'station_elevation': station_elevation,
                    'station_rec_elevation': station_rec_elevation,
                    'obs_datetime': target_datetime,
                    'values_cm': obs}

    # Create the pkl file if all data fetched is more than 60 days earlier
    # than the current date/time.
    lag = dt.datetime.utcnow() - target_datetime
    if bounding_box is None and \
       lag > dt.timedelta(days=60):
        file_obj = open(file_name, 'wb')
        pkl.dump(obs_snowfall, file_obj)
        file_obj.close()
        if verbose:
            print('INFO: wrote query results to {}.'.format(file_name))

    return(obs_snowfall)


def get_snwd_prcp_obs(target_datetime,
                      duration_hours,
                      no_data_value=-99999.0,
                      bounding_box=None,
                      scratch_dir=None,
                      verbose=None):

    """
    Get precipitation observations from the "web_data" database on wdb0 having
    duration of duration_hours, an observation time of target_datetime, and
    limit the query to those associated with observed snow depth at
    target_datetime.
    The idea here is to limit the precipitation observations we collect to
    those from stations that report snow depth at target_datetime, and to
    ignore all others.
    """

    # Only use .pkl files if there is no bounding box.
    if bounding_box is None:

        file_name = 'wdb0_obs_precipitation_' + \
                    '{}_hours_ending_'.format(duration_hours) + \
                    target_datetime.strftime('%Y%m%d%H') + \
                    '_snow_depth' + \
                    '.pkl'

        if scratch_dir is not None:
            file_name = os.path.join(scratch_dir, file_name)

            if os.path.isfile(file_name):

                # Retrieve data from pkl file and return.
                file_obj = open(file_name, 'rb')
                obs_precip = pkl.load(file_obj)
                file_obj.close()
                return(obs_precip)

    # Open the web database.
    conn_string = "host='wdb0.dmz.nohrsc.noaa.gov' dbname='web_data'"
    conn = psycopg2.connect(conn_string)
    conn.set_client_encoding("utf-8")
    cursor = conn.cursor()

    # Define a SQL statement.
    sql_cmd = 'SELECT ' + \
              't1.obj_identifier, ' + \
              'TRIM(t3.station_id), ' + \
              'TRIM(t3.name), ' + \
              't3.coordinates[0] AS lon, ' + \
              't3.coordinates[1] AS lat, ' + \
              't3.elevation, ' + \
              't3.recorded_elevation, ' + \
              't2.date, ' + \
              't2.value * 1000.0 AS obs_precip_mm ' + \
              'FROM ' + \
              '(' + \
              'SELECT obj_identifier ' + \
              'FROM point.obs_snow_depth ' + \
              'WHERE date = \'' + \
              target_datetime.strftime('%Y-%m-%d %H:%M:%S') + \
              '\' ' + \
              'AND value IS NOT NULL ' + \
              'GROUP BY obj_identifier' + \
              ') ' + \
              'AS t1, ' + \
              'point.obs_precip_raw AS t2, ' + \
              'point.allstation AS t3 ' + \
              'WHERE t2.date = \'' + \
              target_datetime.strftime('%Y-%m-%d %H:%M:%S') + \
              '\' ' + \
              'AND t2.duration = {} '.format(duration_hours * 3600) + \
              'AND t1.obj_identifier = t2.obj_identifier ' + \
              'AND t2.obj_identifier = t3.obj_identifier ' + \
              'AND t3.obj_identifier = t1.obj_identifier ' + \
              'AND t2.value IS NOT NULL '

    if bounding_box is not None:
        sql_cmd = sql_cmd + \
                  'AND t3.coordinates[0] >= ' + \
                  '{} '.format(bounding_box[0]) + \
                  'AND t3.coordinates[0] < ' + \
                  '{} '.format(bounding_box[1]) + \
                  'AND t3.coordinates[1] >= ' + \
                  '{} '.format(bounding_box[2]) + \
                  'AND t3.coordinates[1] < ' + \
                  '{} '.format(bounding_box[3])

    sql_cmd = sql_cmd + 'ORDER BY t1.obj_identifier, t2.date;'

    if verbose:
        print('INFO: psql command "{}"'.format(sql_cmd))

    cursor.execute(sql_cmd)

    # The result below is just a huge list of tuples.
    obs_precip = cursor.fetchall()

    obs_precip_column_list = ['obj_identifier',
                              'station_id',
                              'name',
                              'lon',
                              'lat',
                              'elevation',
                              'recorded_elevation',
                              'date',
                              'obs_precip_mm']

    df = pd.DataFrame(obs_precip, columns=obs_precip_column_list)

    # This section organizes the query results into lists and arrays.

    station_ind = -1
    current_station_obj_id = -1
    station_obj_id = []
    station_id = []
    station_name = []
    station_lon = []
    station_lat = []
    station_elevation = []
    station_rec_elevation = []
    # Create a 2-d [time, station] array.
    # GF - 2nd dim used to be num_hours
    obs = np.ma.empty([1], dtype=float)
    obs[0] = no_data_value
    obs[0] = np.ma.masked
    # print(len(df))
    for ind, row in df.iterrows():
        if row['obj_identifier'] != current_station_obj_id:
            station_ind += 1
            if station_ind > 0:
                # Just finished all data for previous station.
                # GF - 2nd dim used to be num_hours
                obs = np.ma.append(obs,
                                   np.ma.empty([1],
                                               dtype=float),
                                   axis=0)
                obs[station_ind] = no_data_value
                obs[station_ind] = np.ma.masked
            # New station
            station_obj_id.append(row['obj_identifier'])
            station_id.append(row['station_id'])
            station_name.append(row['name'])
            station_lon.append(row['lon'])
            station_lat.append(row['lat'])
            station_elevation.append(row['elevation'])
            station_rec_elevation.append(row['recorded_elevation'])

            current_station_obj_id = station_obj_id[station_ind]

        try:
            obs[station_ind] = row['obs_precip_mm']
        except:
            print(obs)
            print(obs.shape)
            print(row)
            exit(1)

    num_stations = station_ind + 1
    # print(len(obs))

    # Place results in a dictionary.
    obs_precip = {'num_stations': num_stations,
                  'station_obj_id': station_obj_id,
                  'station_id': station_id,
                  'station_name': station_name,
                  'station_lon': station_lon,
                  'station_lat': station_lat,
                  'station_elevation': station_elevation,
                  'station_rec_elevation': station_rec_elevation,
                  'obs_datetime': target_datetime,
                  'values_mm': obs}

    # Create the pkl file if all data fetched is more than 60 days earlier
    # than the current date/time.
    lag = dt.datetime.utcnow() - target_datetime
    if bounding_box is None and \
       lag > dt.timedelta(days=60):
        file_obj = open(file_name, 'wb')
        pkl.dump(obs_precip, file_obj)
        file_obj.close()
        if verbose:
            print('INFO: wrote query results to {}.'.format(file_name))

    return(obs_precip)


def get_swe_prcp_obs(target_datetime,
                     duration_hours,
                     no_data_value=-99999.0,
                     bounding_box=None,
                     scratch_dir=None,
                     verbose=None):

    """
    Get precipitation observations from the "web_data" database on wdb0 having
    duration of duration_hours, an observation time of target_datetime, and
    limit the query to those associated with observed snow water equivalent at
    target_datetime.
    The idea here is to limit the precipitation observations we collect to
    those from stations that report snow water equivalent at target_datetime,
    and to ignore all others.
    """

    # Only use .pkl files if there is no bounding box.
    if bounding_box is None:

        file_name = 'wdb0_obs_precipitation_' + \
                    '{}_hours_ending_'.format(duration_hours) + \
                    target_datetime.strftime('%Y%m%d%H') + \
                    '_swe' + \
                    '.pkl'

        if scratch_dir is not None:
            file_name = os.path.join(scratch_dir, file_name)

            if os.path.isfile(file_name):

                # Retrieve data from pkl file and return.
                file_obj = open(file_name, 'rb')
                obs_precip = pkl.load(file_obj)
                file_obj.close()
                return(obs_precip)

    # Open the web database.
    conn_string = "host='wdb0.dmz.nohrsc.noaa.gov' dbname='web_data'"
    conn = psycopg2.connect(conn_string)
    conn.set_client_encoding("utf-8")
    cursor = conn.cursor()

    # Define a SQL statement.
    sql_cmd = 'SELECT ' + \
              't1.obj_identifier, ' + \
              'TRIM(t3.station_id), ' + \
              'TRIM(t3.name), ' + \
              't3.coordinates[0] AS lon, ' + \
              't3.coordinates[1] AS lat, ' + \
              't3.elevation, ' + \
              't3.recorded_elevation, ' + \
              't2.date, ' + \
              't2.value * 1000.0 AS obs_precip_mm ' + \
              'FROM ' + \
              '(' + \
              'SELECT obj_identifier ' + \
              'FROM point.obs_swe ' + \
              'WHERE date = \'' + \
              target_datetime.strftime('%Y-%m-%d %H:%M:%S') + \
              '\' ' + \
              'AND value IS NOT NULL ' + \
              'GROUP BY obj_identifier' + \
              ') ' + \
              'AS t1, ' + \
              'point.obs_precip_raw AS t2, ' + \
              'point.allstation AS t3 ' + \
              'WHERE t2.date = \'' + \
              target_datetime.strftime('%Y-%m-%d %H:%M:%S') + \
              '\' ' + \
              'AND t2.duration = {} '.format(duration_hours * 3600) + \
              'AND t1.obj_identifier = t2.obj_identifier ' + \
              'AND t2.obj_identifier = t3.obj_identifier ' + \
              'AND t3.obj_identifier = t1.obj_identifier ' + \
              'AND t2.value IS NOT NULL '

    if bounding_box is not None:
        sql_cmd = sql_cmd + \
                  'AND t3.coordinates[0] >= ' + \
                  '{} '.format(bounding_box[0]) + \
                  'AND t3.coordinates[0] < ' + \
                  '{} '.format(bounding_box[1]) + \
                  'AND t3.coordinates[1] >= ' + \
                  '{} '.format(bounding_box[2]) + \
                  'AND t3.coordinates[1] < ' + \
                  '{} '.format(bounding_box[3])

    sql_cmd = sql_cmd + 'ORDER BY t1.obj_identifier, t2.date;'

    if verbose:
        print('INFO: psql command "{}"'.format(sql_cmd))

    cursor.execute(sql_cmd)

    # The result below is just a huge list of tuples.
    obs_precip = cursor.fetchall()

    obs_precip_column_list = ['obj_identifier',
                              'station_id',
                              'name',
                              'lon',
                              'lat',
                              'elevation',
                              'recorded_elevation',
                              'date',
                              'obs_precip_mm']

    df = pd.DataFrame(obs_precip, columns=obs_precip_column_list)

    # This section organizes the query results into lists and arrays.

    station_ind = -1
    current_station_obj_id = -1
    station_obj_id = []
    station_id = []
    station_name = []
    station_lon = []
    station_lat = []
    station_elevation = []
    station_rec_elevation = []
    # Create a 2-d [time, station] array.
    # GF - 2nd dim used to be num_hours
    obs = np.ma.empty([1], dtype=float)
    obs[0] = no_data_value
    obs[0] = np.ma.masked
    # print(len(df))
    for ind, row in df.iterrows():
        if row['obj_identifier'] != current_station_obj_id:
            station_ind += 1
            if station_ind > 0:
                # Just finished all data for previous station.
                # GF - 2nd dim used to be num_hours
                obs = np.ma.append(obs,
                                   np.ma.empty([1],
                                               dtype=float),
                                   axis=0)
                obs[station_ind] = no_data_value
                obs[station_ind] = np.ma.masked
            # New station
            station_obj_id.append(row['obj_identifier'])
            station_id.append(row['station_id'])
            station_name.append(row['name'])
            station_lon.append(row['lon'])
            station_lat.append(row['lat'])
            station_elevation.append(row['elevation'])
            station_rec_elevation.append(row['recorded_elevation'])

            current_station_obj_id = station_obj_id[station_ind]

        # try:
        obs[station_ind] = row['obs_precip_mm']
        # except:
            
        #     print(obs)
        #     print(obs.shape)
        #     print(row)
        #     exit(1)

    num_stations = station_ind + 1
    # print(len(obs))

    # Place results in a dictionary.
    obs_precip = {'num_stations': num_stations,
                  'station_obj_id': station_obj_id,
                  'station_id': station_id,
                  'station_name': station_name,
                  'station_lon': station_lon,
                  'station_lat': station_lat,
                  'station_elevation': station_elevation,
                  'station_rec_elevation': station_rec_elevation,
                  'obs_datetime': target_datetime,
                  'values_mm': obs}

    # Create the pkl file if all data fetched is more than 60 days earlier
    # than the current date/time.
    lag = dt.datetime.utcnow() - target_datetime
    if bounding_box is None and \
       lag > dt.timedelta(days=60):
        file_obj = open(file_name, 'wb')
        pkl.dump(obs_precip, file_obj)
        file_obj.close()
        if verbose:
            print('INFO: wrote query results to {}.'.format(file_name))

    return(obs_precip)


def  get_prv_air_temp_obs(target_datetime,
                          num_hours_prev,
                          no_data_value=-99999.0,
                          bounding_box=None,
                          scratch_dir=None,
                          verbose=None):

    """
    Get hourly air temperature observations from the "web_data" database on
    wdb0 for num_hours_prev prior to (but not including) target_datetime.
    """

    begin_datetime = target_datetime - dt.timedelta(hours=num_hours_prev)
    end_datetime = target_datetime - dt.timedelta(hours=1)

    # Only use .pkl files if there is no bounding box.
    if bounding_box is None:

        file_name = 'wdb0_obs_air_temp_' + \
                    '{}_to_'.format(begin_datetime.strftime('%Y%m%d%H')) + \
                    '{}.pkl'.format(end_datetime.strftime('%Y%m%d%H'))

        if scratch_dir is not None:
            file_name = os.path.join(scratch_dir, file_name)

            if os.path.isfile(file_name):

                # Retrieve data from pkl file and return.
                file_obj = open(file_name, 'rb')
                obs_air_temp = pkl.load(file_obj)
                file_obj.close()
                return(obs_air_temp)

    time_range = end_datetime - begin_datetime
    num_hours = time_range.days * 24 + time_range.seconds // 3600 + 1

    # Open the web database.
    conn_string = "host='wdb0.dmz.nohrsc.noaa.gov' dbname='web_data'"
    conn = psycopg2.connect(conn_string)
    conn.set_client_encoding("utf-8")
    cursor = conn.cursor()

    # Define a SQL statement.
    sql_cmd = 'SELECT ' + \
              't3.obj_identifier, ' + \
              'TRIM(t3.station_id), ' + \
              'TRIM(t3.name), ' + \
              't3.coordinates[0] AS lon, ' + \
              't3.coordinates[1] AS lat, ' + \
              't3.elevation, ' + \
              't3.recorded_elevation, ' + \
              't2.date, ' + \
              't2.value AS obs_air_temp_deg_c ' + \
              'FROM ' + \
              'point.obs_airtemp AS t2, ' + \
              'point.allstation AS t3 ' + \
              'WHERE t2.date >= \'' + \
              begin_datetime.strftime('%Y-%m-%d %H:%M:%S') + \
              '\' ' + \
              'AND t2.date <= \'' + \
              end_datetime.strftime('%Y-%m-%d %H:%M:%S') + \
              '\' ' + \
              'AND t2.obj_identifier = t3.obj_identifier ' + \
              'AND t2.value IS NOT NULL '

    if bounding_box is not None:
        sql_cmd = sql_cmd + \
                  'AND t3.coordinates[0] >= ' + \
                  '{} '.format(bounding_box[0]) + \
                  'AND t3.coordinates[0] < ' + \
                  '{} '.format(bounding_box[1]) + \
                  'AND t3.coordinates[1] >= ' + \
                  '{} '.format(bounding_box[2]) + \
                  'AND t3.coordinates[1] < ' + \
                  '{} '.format(bounding_box[3])

    sql_cmd = sql_cmd + 'ORDER BY t3.obj_identifier, t2.date;'

    if verbose:
        print('INFO: psql command "{}"'.format(sql_cmd))

    cursor.execute(sql_cmd)

    # The result below is just a huge list of tuples.
    fetched_airtemp = cursor.fetchall()

    obs_air_temp_column_list = ['obj_identifier',
                                'station_id',
                                'name',
                                'lon',
                                'lat',
                                'elevation',
                                'recorded_elevation',
                                'date',
                                'obs_air_temp_deg_c']

    df = pd.DataFrame(fetched_airtemp, columns=obs_air_temp_column_list)

    # This section organizes the query results into lists and arrays.

    station_ind = -1
    current_station_obj_id = -1
    station_obj_id = []
    station_id = []
    station_name = []
    station_lon = []
    station_lat = []
    station_elevation = []
    station_rec_elevation = []
    # Create a 2-d [time, station] array.
    obs = np.ma.empty([1, num_hours], dtype=float)
    obs[0,:] = no_data_value
    obs[0,:] = np.ma.masked
    for ind, row in df.iterrows():
        if row['obj_identifier'] != current_station_obj_id:
            station_ind += 1
            if station_ind > 0:
                # Just finished all data for previous station.
                obs = np.ma.append(obs,
                                   np.ma.empty([1,
                                                num_hours],
                                               dtype=float),
                                   axis=0)
                obs[station_ind,:] = no_data_value
                obs[station_ind,:] = np.ma.masked
            # New station
            station_obj_id.append(row['obj_identifier'])
            station_id.append(row['station_id'])
            station_name.append(row['name'])
            station_lon.append(row['lon'])
            station_lat.append(row['lat'])
            station_elevation.append(row['elevation'])
            station_rec_elevation.append(row['recorded_elevation'])

            current_station_obj_id = station_obj_id[station_ind]

        # Add the observation to the obs array.
        time_diff = row['date'] - begin_datetime
        time_ind = time_diff.days * 24 + time_diff.seconds // 3600

        obs[station_ind, time_ind] = row['obs_air_temp_deg_c']

    num_stations = station_ind + 1

    obs_datetime = [begin_datetime +
                    dt.timedelta(hours=i) for i in range(num_hours)]

    # Place results in a dictionary.
    obs_air_temp = {'num_stations': num_stations,
                    'num_hours': num_hours,
                    'station_obj_id': station_obj_id,
                    'station_id': station_id,
                    'station_name': station_name,
                    'station_lon': station_lon,
                    'station_lat': station_lat,
                    'station_elevation': station_elevation,
                    'station_rec_elevation': station_rec_elevation,
                    'obs_datetime': obs_datetime,
                    'values_deg_c': obs}

    # Create the pkl file if all data fetched is more than 60 days earlier
    # than the current date/time.
    lag = dt.datetime.utcnow() - end_datetime
    if bounding_box is None and \
       lag > dt.timedelta(days=60):
        file_obj = open(file_name, 'wb')
        pkl.dump(obs_air_temp, file_obj)
        file_obj.close()
        if verbose:
            print('INFO: wrote query results to {}.'.format(file_name))

    return(obs_air_temp)
