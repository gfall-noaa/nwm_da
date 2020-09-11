#!/usr/bin/python3

import argparse
import os
from netCDF4 import Dataset, num2date, date2num
import datetime as dt
import numpy as np
import sys
import psycopg2
import pandas as pd
import time
import shutil
from vincenty import vincenty
import math
from geopy import distance
import errno
import logging
import random

sys.path.append(os.path.join(os.path.dirname(__file__), '..',
                             'snodas_climatology'))
import snodas_clim

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'lib'))
import wdb0
import local_logger

def find_nearest_neighbors(lat1,
                           lon1,
                           lat2,
                           lon2,
                           neighborhood_radius_km,
                           min_neighbors,
                           max_neighbors,
                           box_delta_factor=10,
                           verbose=None):
    '''
    For each location established by the arrays lat1 and lon1, identify the
    nearest neighbors from the locations established by lat2 and lon2 (these
    could be the same as lat1 and lon1 but do not have to be) within
    neighborhood_radius_km. For each lat1/lon1 location, return at least
    min_neighbors (or None, if that minimum is not found), and at most
    max_neighbors.
    '''

    logger = logging.getLogger()

    if isinstance(lat1, list):
        lat1 = np.asarray(lat1, dtype=np.float64)
    if isinstance(lon1, list):
        lon1 = np.asarray(lon1, dtype=np.float64)
    if isinstance(lat2, list):
        lat2 = np.asarray(lat2, dtype=np.float64)
    if isinstance(lon1, list):
        lon2 = np.asarray(lon2, dtype=np.float64)

    # Prepare to set up bounding boxes in latitude and longitude by estimating
    # "km per degree" in the latitudinal and longitudinal directions. These
    # should be chosen so that the half-width of the bounding box is never
    # less than the neighborhood radius in either direction.

    # The minimum km per degree latitude occurs at the equator, making that a
    # safe choice. We might also use the minimum (absolute) latitude in the
    # data.
    km_per_deg_lat_ref = distance.distance((0.0005, 0.0),
                                           (-0.0005, 0.0)).km * 1000.0

    max_box_half_width_lat_deg = neighborhood_radius_km / km_per_deg_lat_ref

    # Calculate the amount to increase the bounding box in the latitudinal
    # direction with each step in the search for neighbors.
    delta_lat_deg = max_box_half_width_lat_deg / box_delta_factor
    max_iters = box_delta_factor + 1

    # Create a list of lists to store indices of neighbors for each lat1/lon1
    # location taken from lat2/lon2, and a second one for the associated
    # distances.
    nhood_ind = [[] for i in range(len(lat1))]
    nhood_dist_km = [[] for i in range(len(lat1))]

    num_dist_calc = 0
    t1 = dt.datetime.utcnow()

    for ind1 in range(0, len(lat1)):

        site_lat = lat1[ind1]
        site_lon = lon1[ind1]

        # Establish a maximum bounding box longitude range appropriate for the
        # current station.
        site_hood_max_abs_lat = np.abs(site_lat) + \
                                max_box_half_width_lat_deg
        km_per_deg_lon_ref = \
            distance.distance((site_hood_max_abs_lat, 0.0),
                              (site_hood_max_abs_lat, 1.0)).km
        num_dist_calc += 1

        max_box_half_width_lon_deg = \
            neighborhood_radius_km / km_per_deg_lon_ref
        delta_lon_deg = max_box_half_width_lon_deg / box_delta_factor

        # Create a masked array for storing distances to stations in the
        # (expanding) bounding box.
        hood_dist_km = np.ma.array([-1.0] * len(lat2), mask = True)

        # Find the nearest neighbors for each station.
        num_neighbors_in_hood = 0
        iters = 0
        box_half_width_lat_deg = 0.0
        box_half_width_lon_deg = 0.0

        # Iterate on expanding boxes until max_neighbors are found or the
        # maximum bounding box dimension is reached.
        while num_neighbors_in_hood < max_neighbors and \
              iters < max_iters and \
              (box_half_width_lat_deg < max_box_half_width_lat_deg or \
               box_half_width_lon_deg < max_box_half_width_lon_deg):

            # Expand bounding box in latitude and longitude.
            box_half_width_lat_deg += delta_lat_deg
            box_half_width_lon_deg += delta_lon_deg

            abs_lat_diff = np.abs(lat2 - site_lat)
            abs_lon_diff = np.abs(lon2 - site_lon)

            # Find stations inside the current bounding box.
            # We use a small "minimum bounding rectangle" to eliminate the
            # current station, but be aware that this will also eliminate
            # sites that are effectively colocated.
            in_box_ind = \
                (np.where((abs_lat_diff < box_half_width_lat_deg) &
                          (abs_lon_diff < box_half_width_lon_deg) &
                          ((abs_lat_diff > 1.0e-6) | \
                           (abs_lon_diff > 1.0e-6))))[0]

            # Calculate distances for locations in the box (when we have not
            # done so already).
            for k in range(0, len(in_box_ind)):
                ind2 = in_box_ind[k]
                if hood_dist_km.mask[ind2]:
                    hood_dist_km[ind2] = \
                        distance.distance((site_lat, site_lon),
                                          (lat2[ind2], lon2[ind2])).km
                    num_dist_calc += 1

            # Find stations within the neighborhood radius.
            # Necessary because it is possible for stations inside the
            # bounding box to be beyond the neighborhood radius.
            in_hood_ind = \
                np.where((hood_dist_km > 0.0) & \
                         (hood_dist_km <= neighborhood_radius_km))[0]

            num_neighbors_in_hood = len(in_hood_ind)

            iters += 1

        # Do not put anything in nhood_ind if the min_neighbors threshold is
        # not met. We are throwing away information we have already collected
        # here, which is maybe better left to the calling program...
        if num_neighbors_in_hood < min_neighbors:
            continue

        if num_neighbors_in_hood >= max_neighbors:

            # We found all the neighbors we were looking for (and possibly
            # more than that), but it is possible there are nearer neighbors
            # outside the bounding box.
            max_box_dist = np.max(hood_dist_km[in_hood_ind])

            # Define a new bounding box based on max_box_dist.
            new_half_width_lat_deg = max_box_dist / km_per_deg_lat_ref
            new_half_width_lon_deg = max_box_dist / km_per_deg_lon_ref

            if new_half_width_lat_deg > box_half_width_lat_deg or \
               new_half_width_lon_deg > box_half_width_lon_deg:

                # Look for locations in the expanded bounding box.
                box_half_width_lat_deg = new_half_width_lat_deg
                box_half_width_lon_deg = new_half_width_lon_deg

                # Programming check.
                if new_half_width_lat_deg > box_half_width_lat_deg or \
                   new_half_width_lon_deg > box_half_width_lon_deg:
                    print('ERROR: (PROGRAMMING) bounding box limit exceeded',
                          file=sys.stderr)
                    sys.exit(1)

                abs_lat_diff = np.abs(lat2 - site_lat)
                abs_lon_diff = np.abs(lon2 - site_lon)

                # Find stations inside the current bounding box.
                in_box_ind = \
                    (np.where((abs_lat_diff < box_half_width_lat_deg) &
                              (abs_lon_diff < box_half_width_lon_deg) &
                              ((abs_lat_diff > 1.0e-6) |
                               (abs_lon_diff > 1.0e-6))))[0]

                # Calculate distances for locations in the box (when we have
                # not # done so already).
                for k in range(0, len(in_box_ind)):
                    ind2 = in_box_ind[k]
                    if hood_dist_km.mask[ind2]:
                        hood_dist_km[ind2] = \
                            distance.distance((site_lat, site_lon),
                                              (lat2[ind2], lon2[ind2])).km
                        num_dist_calc += 1

                # Find stations within the neighborhood radius.
                in_hood_ind = \
                    np.where((hood_dist_km > 0.0) & \
                             (hood_dist_km <= neighborhood_radius_km))[0]

                old_num_neighbors_in_hood = num_neighbors_in_hood
                num_neighbors_in_hood = len(in_hood_ind)

                # Programming check.
                if num_neighbors_in_hood < old_num_neighbors_in_hood:
                    print('ERROR: (PROGRAMMING) some major issue here',
                          file=sys.stderr)
                    sys.exit(1)

        # Add neighbors to list of lists.
        order = np.ma.argsort(hood_dist_km[in_hood_ind])
        for nc in range(0, num_neighbors_in_hood):
            ind2 = in_hood_ind[order[nc]]
            if nc >= max_neighbors:
                break
            nhood_ind[ind1].append(ind2)
            nhood_dist_km[ind1].append(hood_dist_km[ind2])

    t2 = dt.datetime.utcnow()
    elapsed_time = t2 - t1
    logger.debug('Found nearest neighbors in {} seconds.'.
                 format(elapsed_time.total_seconds()))

    return nhood_ind, nhood_dist_km


def dist_crude_euclidian(lat1, lon1, lat2, lon2):
    """
    Calculate a crude euclidian distance on the Earth between
    latitude/longitude locations. Inputs should be numpy arrays or scalars.
    """
    gc_wgs84_semi_minor_km = 6356.752314245
    gc_wgs84_semi_major_km = 6378.1370
    gdc_deg_to_rad = math.pi / 180.0
    gdc_lat_arc_to_len = gdc_deg_to_rad * gc_wgs84_semi_minor_km
    gdc_lon_arc_to_len = gdc_deg_to_rad * gc_wgs84_semi_major_km
    gdc_great_circle = math.pi / 360.0 * \
                       (gc_wgs84_semi_major_km + gc_wgs84_semi_minor_km)

    center_latitude = 0.5 * (lat1 + lat2)
    center_latitude_rad = center_latitude * gdc_deg_to_rad
    lat_diff = np.abs(lat1 - lat2) * gdc_lat_arc_to_len
    lon_diff = np.abs(lon1 - lon2) * gdc_lon_arc_to_len * \
               np.cos(center_latitude_rad)
    distance = np.sqrt(lat_diff * lat_diff + lon_diff * lon_diff)

    return distance


def station_qc_db_copy(database_path,
                       verbose=None):
    """
    Make a temporary copy of the QC database.
    """

    logger = logging.getLogger()

    suffix = dt.datetime.utcnow().strftime('%Y%m%d%H%M%S') + \
             '.{}'.format(os.getpid())

    temp_database_path = database_path + '.' + suffix

    try:
        shutil.copy(database_path, temp_database_path)
    except:
        logger.error('Failed to make temporary copy of ' +
                     '{} '.format(database_path) +
                     ' as {}.'.format(temp_database_path))
        sys.exit(1)

    logger.debug('Modifying copy of {} '.format(database_path) +
                 'as {}.'.format(temp_database_path))

    return temp_database_path


def progress(count, total, status=''):
    """
    Progress bar:  
    Copied from
    https://gist.github.com/vladignatyev/06860ec2040cb497f0f3
    """
    bar_len = 60
    filled_len = int(round(bar_len * count / float(total)))

    percents = round(100.0 * count / float(total), 1)
    bar = '=' * filled_len + '-' * (bar_len - filled_len)

    sys.stdout.write('[%s] %s%s ...%s\r' % (bar, percents, '%', status))
    sys.stdout.flush()


def timedelta_to_int_hours(delta_time):

    """
    Converts a datetime timedelta object to an integer number of hours.
    """

    delta_time_hours = delta_time.days * 24 + \
                       delta_time.seconds // 3600
    # delta_time.total_seconds() // 3600 also works
    return delta_time_hours


def update_qc_db_metadata(qcdb,
                          qcdb_obj_id_var,
                          qcdb_lon_var,
                          qcdb_lat_var,
                          qcdb_station_vars,
                          wdb_col_list,
                          verbose=False):
    '''
    Confirm/update metadata for stations in the QC database using the webdb
    allstation table.
    '''

    logger = logging.getLogger()

    # Verify that there are metadata present.
    if qcdb_obj_id_var.size == 0:
        return None

    # print(wdb_col_list.index('obj_identifie'))
    # print(type(wdb_col_list))

    # Generate a string listing of columns to select from the wdb0
    # "point.allstation" table.
    wdb_col_list_str = ''
    for allstation_column_name in wdb_col_list:
        if len(wdb_col_list_str) == 0:
            wdb_col_list_str = wdb_col_list_str + allstation_column_name
        else:
            wdb_col_list_str = wdb_col_list_str + ', ' + allstation_column_name

    # Use station locations to establish a bounding box in longitude and
    # latitude.
    qcdb_min_lon = qcdb_lon_var[:].min()
    qcdb_max_lon = qcdb_lon_var[:].max()
    qcdb_min_lat = qcdb_lat_var[:].min()
    qcdb_max_lat = qcdb_lat_var[:].max()

    # Use minimum and maximum object identifiers to further limit the web
    # database selection.
    qcdb_min_obj_id = qcdb_obj_id_var[:].min()
    qcdb_max_obj_id = qcdb_obj_id_var[:].max()

    # Open the web database.
    conn_string = "host='wdb0.dmz.nohrsc.noaa.gov' dbname='web_data'"
    conn = psycopg2.connect(conn_string)
    conn.set_client_encoding("utf-8")
    cursor = conn.cursor()

    sql_cmd = "SELECT " + wdb_col_list_str + " " + \
              "FROM point.allstation " + \
              "WHERE coordinates[0]" \
              " >= {} ".format(qcdb_min_lon - 0.01) + \
              "AND coordinates[0]" + \
              " <= {} ".format(qcdb_max_lon + 0.01) + \
              "AND coordinates[1]" + \
              " >= {} ".format(qcdb_min_lat - 0.01) + \
              "AND coordinates[1]" + \
              " <= {} ".format(qcdb_max_lat + 0.01) + \
              "AND obj_identifier >= {} ".format(qcdb_min_obj_id) + \
              "AND obj_identifier <= {} ".format(qcdb_max_obj_id) + \
              "ORDER BY obj_identifier;"
    logger.debug('psql command "{}"'.format(sql_cmd))

    # Set this_station_update_datetime to the current system time.
    # This should be done just before reading the allstation table.
    this_station_update_datetime = dt.datetime.utcnow()

    cursor.execute(sql_cmd)
    # allstation is just a huge list of tuples.
    allstation = cursor.fetchall()
    #print(len(allstation))
    wdb_df = pd.DataFrame(allstation, columns=wdb_col_list)

    logger.debug('Found {} stations.'.format(wdb_df.shape[0]))

    # In the course of confirming/updating station metadata we rely on
    # the fact that the dataframe columns are in the same order as the
    # database columns, and both are in the same order as the "station"
    # variables we pulled from the database file.

    # Find common elements.
    qcdb_sort_ind = np.argsort(qcdb_obj_id_var[:])
    wdb_in_qcdb = np.isin(wdb_df['obj_identifier'].values, qcdb_obj_id_var[:])
    wdb_df_rows_to_read = np.nonzero(wdb_in_qcdb)[0]
    # Below must match.
    if len(wdb_df_rows_to_read) != len(qcdb_obj_id_var[:]):
        logger.error('Programming (station count mismatch)')
        qcdb.close()
        sys.exit(1)

    # print(np.sum(wdb_in_qcdb))
    # print(len(qcdb_obj_id_var[:]))
    # print(type(wdb_df_rows_to_read))
    # print(len(wdb_df_rows_to_read))
    # print(wdb_df_rows_to_read[0],
    #       wdb_df['obj_identifier'].values[wdb_df_rows_to_read[0]],
    #       qcdb_obj_id_var[:][0])
    # print(len(wdb_in_qcdb))
    # print(wdb_in_qcdb[0])
    # print(len(wdb_df_rows_to_read))
    # print(wdb_df_rows_to_read[0])
    # print(qcdb_obj_id_var[:][0])

    qcdb_sort_count = 0
    # Loop over just the rows of the wdb dataframe we need to read.
    for wdb_row_ind in wdb_df_rows_to_read:

        # Locate this object ID in the QC database.
        qcdb_ind = qcdb_sort_ind[qcdb_sort_count]

        if verbose:
            progress(qcdb_sort_count, len(wdb_df_rows_to_read),
                     status='Updating metadata')

        # Get this row of the wdb dataframe.
        wdb_row = wdb_df.iloc[wdb_row_ind,:]
        # print(wdb_row['obj_identifier'], qcdb_obj_id_var[:][qcdb_ind])
        this_obj_id = qcdb_obj_id_var[:][qcdb_ind]
        if wdb_row['obj_identifier'] != this_obj_id:
            print('')
            print(wdb_row['obj_identifier'], this_obj_id)
            print('ERROR: programming (object ID mismatch)',
                  file=sys.stderr)
            qcdb.close()
            sys.exit(1)

        # Loop over station variables (columns) in this row.
        for ind, qcdb_station_var in enumerate(qcdb_station_vars):
            wdb_value = wdb_row[wdb_col_list[ind]]
            if isinstance(wdb_value, str):
                # Eliminate leading and trailing whitespace for strings.
                wdb_value = wdb_value.strip()
            if isinstance(wdb_value, dt.datetime):
                # if 'date' in wdb_col_list[ind]:
                # Format pandas "Timestamp" data into strings.
                wdb_value = wdb_value.strftime('%Y-%m-%d %H:%M:%S')
            qcdb_value = qcdb_station_var[qcdb_ind]
            if wdb_value != qcdb_value:
                # Update qcdb variable!
                if verbose:
                    if isinstance(qcdb_value, str):
                        old = '"' + qcdb_value + '"'
                    else:
                        old = qcdb_value
                    if isinstance(wdb_value, str):
                        new = '"' + wdb_value + '"'
                    else:
                        new = wdb_value
                    logger.info('Updating object id {} '.format(this_obj_id) +
                                'variable "{}" '.
                                format(qcdb_station_var.name) +
                                'from {} '.format(old) +
                                'to {}'.format(new))
                    qcdb_station_var[qcdb_ind] = wdb_value

        qcdb_sort_count += 1

        # # Just so we can see a few then bail out.
        # if qcdb_ind > 8:
        #     break

    if verbose: print('')

    qcdb.setncattr_string('last_station_update_datetime',
                          this_station_update_datetime.
                          strftime('%Y-%m-%d %H:%M:%S UTC'))

    return None


def qc_durre_snwd_wre(value_cm):
    """
    Basic integrity checks:
    Snow depth world record exceedance.
    """
    threshold_value = 1146.0
    if value_cm < 0.0 or value_cm > threshold_value:
        return True
    else:
        return False


def qc_durre_snwd_change_wre(snow_depth_value_cm,
                             prev_sd_value_cm,
                             prev_sd_qc):
    """
    Basic integrity checks:
    Snow depth increase world record exceedance.
    """

    threshold_sd_increase_cm = 192.5

    # Mask previous snow depth data that have any QC flags set.
    prev_sd_value_cm = np.ma.masked_where(prev_sd_qc != 0,
                                          prev_sd_value_cm)

    num_unmasked_prev_sd = prev_sd_value_cm.count()

    if num_unmasked_prev_sd == 0:
        # Test is not possible.
        return None, None

    # Find the minimum observed preceding snow depth value for
    # comparison with the observation being checked. This snow
    # depth will be our version of SNWD(-1) in Durre (2010),
    # Table 1 (world record exceedance check: snow depth increase)
    ref_ind = np.ma.argmin(prev_sd_value_cm)

    if snow_depth_value_cm - prev_sd_value_cm[ref_ind] > \
       threshold_sd_increase_cm:
        return True, ref_ind
    else:
        return False, ref_ind


def qc_durre_snwd_streak(snow_depth_value_cm,
                         prev_sd_value_cm,
                         prev_sd_qc,
                         streak_value_threshold=None):
    """
    Basic integrity checks:
    Snow depth streak check.
    """
    if streak_value_threshold is None:
        streak_value_threshold = 0.1
    streak_min_consecutive = 10

    # Mask previous snow depth data that have any QC flags set.
    prev_sd_value_cm = np.ma.masked_where(prev_sd_qc != 0,
                                          prev_sd_value_cm)

    # Assemble previous and current data into one time series.
    station_time_series = np.ma.append(prev_sd_value_cm, snow_depth_value_cm)

    if station_time_series.count() < streak_min_consecutive:
        return None

    if np.ma.max(station_time_series) <= streak_value_threshold:
        return None
    
    if (np.ma.max(station_time_series) -
        np.ma.min(station_time_series)) < streak_value_threshold:
        return True
    else:
        return False


def obs_rate_category(obs, min_sub_period_proportion=0.5, verbose=False):
    """
    Given a list of hourly observations in the form of a numpy masked
    array, determine the "rate category" that indicates how frequently
    observations are available, using the following five categories:

    1. sporadic (very few observations)
    2. quasi-daily (reporting rate >= 1 observation every 4 days)
    3. daily (reporting rate >= 3 observations every 4 days)
    4. synoptic (reporting rate >= 6 observations per day)
    5. hourly (reporting rate >= 18 observations per day)

    A given criteron must be met on average, but also consistently,
    or the next "lower" criterion is considered.
    """
    qa_period_hours = len(obs)

    # ind = np.where(obs.mask == False)
    # num_reports = len(ind[0])
    # if num_reports == 0:
    num_reports = obs.count()
    if num_reports == 0:
        return None

    ave_reporting_rate = \
        np.float(num_reports) / np.float(qa_period_hours) * 24.0
    if verbose:
        print('average reporting rate: {} obs/day'.format(ave_reporting_rate))

    reporting_rate_threshold = [0.0, 0.25, 0.75, 6.0, 18.0]

    reporting_rate_name = ['sporadic',
                           'quasi-daily',
                           'daily',
                           'synoptic',
                           'hourly']
    demoted = False
    found_a_category = False

    for rc, r0 in enumerate(sorted(reporting_rate_threshold, reverse=True)):

        if ave_reporting_rate < r0:
            continue

        # if verbose:
        #     print('checking for rate >= {}'.format(r0))

        # Determine the number of individual sub periods where the criterion
        # is met.
        if (r0 > 0.0):
            qa_sub_period_hours = int(max(24.0 / r0, 24.0))
        else:
            qa_sub_period_hours = qa_period_hours
        num_qa_sub_periods = np.int(np.floor(qa_period_hours /
                                             qa_sub_period_hours))
        num_sub_periods_met = 0
        for spc in range(num_qa_sub_periods):
            oc1 = spc * qa_sub_period_hours
            oc2 = oc1 + qa_sub_period_hours
            qa_sub_period_obs = obs[oc1:oc2]
            qa_sub_period_num_reports = \
                len(np.where(qa_sub_period_obs.mask == False)[0])
            qa_sub_period_rate = \
                np.float(qa_sub_period_num_reports) / \
                np.float(qa_sub_period_hours) * 24.0
            if (qa_sub_period_rate >= r0):
                num_sub_periods_met += 1
        # if verbose:
        #     print('met criteria for {} of {} sub-periods'.
        #           format(num_sub_periods_met, num_qa_sub_periods))
        if num_sub_periods_met < \
           (min_sub_period_proportion * num_qa_sub_periods):
            # if verbose:
            #     print('that is less than {}%; '.
            #           format(min_sub_period_proportion * 100) + 
            #           'will check next lower frequency')
            # print('  no - ' + 
            #       'met criteria for only {} '.format(num_sub_periods_met) +
            #       'of {} sub-periods'.format(num_qa_sub_periods))
            demoted = True
        else:
            # if verbose:
            #     print('that is >= {}% - consistently reports >= {} obs/day'.
            #           format(min_sub_period_proportion * 100,
            #                  r0))
            # print('  yes')
            found_a_category = True
            break

    if found_a_category:
        # print('{} reports, '.format(num_reports) +
        #       'reporting rate {} '.format(r0) + 
        #       '({})'.format(reporting_rate_name[len(reporting_rate_name) - 1 - rc]))
        # print('demoted ', demoted)
        return len(reporting_rate_name) - 1 - rc
    else:
        # print('FAILED')
        return None

    return None


def qc_durre_snwd_gap(snow_depth_value_cm,
                      prev_sd_value_cm,
                      prev_sd_qc,
                      ref_ceiling_cm=None,
                      ref_default_cm=None,
                      verbose=None):
    '''
    Outlier checks:
    Gap check for snow depth.
    If ref_ceiling_cm and ref_default_cm are included, then when the default
    reference value (median_obs) exceeds ref_ceiling_cm, it is replaced by the
    value in the station_time_series that is nearest to ref_default_cm. The
    purpose of this option is to override median_obs values that are based on
    dubious large values of snow depth, which often occur at automated sites.
    '''

    logger = logging.getLogger()

    # First value of reporting_rate_threshold needs to be zero!
    reporting_rate_threshold = [0.0, 0.25, 0.75, 6.0, 18.0]

    gap_threshold_cm = [100.0,
                        75.0,
                        60.0,
                        45.0,
                        30.0]

    reporting_rate_name = ['sporadic',
                           'quasi-daily',
                           'daily',
                           'synoptic',
                           'hourly']

    # Mask previous snow depth data that have any QC flags set.
    prev_sd_value_cm = np.ma.masked_where(prev_sd_qc != 0,
                                          prev_sd_value_cm)

    # Assemble previous and current data into one time series.
    # Note that this guarantees that station_time_series will have at least
    # one unmasked value (the observation being QCed, at the end), even if all
    # the others are masked.
    station_time_series = np.ma.append(prev_sd_value_cm, snow_depth_value_cm)

    # Note: add reporting_rate_threshold, gap_threshold_cm,
    # and reporting_rate_name as INPUTS to obs_rate_category,
    # perhaps as elements in a dictionary. Currently
    # reporting_rate_threshold and reporting_rate_name are
    # independently defined in obs_rate_category and that is
    # not great.
    rc = obs_rate_category(station_time_series)

    # TODO: make sure this is sufficient... ideally there would be no way to
    # get None back from obs_rate_category.
    if rc is None:
        if verbose:
            print('WARNING - no match for observation rate.',
                  file=sys.stderr)
        return None

    # Sort observations to simulate a cumulative distribution function.
    sort_ind = station_time_series.argsort()
    obs_sorted = station_time_series[sort_ind]
    median_obs = np.ma.median(station_time_series)

    # Initialize the reference value to the median.
    ref_obs_init = median_obs

    if ref_ceiling_cm is not None and \
       ref_default_cm is not None:
        # Replace ref_obs_init with the time series element nearest in value
        # to ref_default_cm (typically the climatological median), if
        # ref_obs_init exceeds the ref_ceiling_cm.
        if ref_obs_init > ref_ceiling_cm:
            ind = (np.abs(station_time_series - ref_default_cm)).argmin()
            ref_obs_init = station_time_series[ind]
            logger.debug('Replacing median {} '.format(median_obs) +
                         'with value {} '.format(ref_obs_init) +
                         '(observation nearer to climatology) ' +
                         'in gap check.')

    # Initialize list of flagged reports.
    ts_flag_ind = []
    ref_obs = []

    # Upper gap check.

    # Initialize the reference value.
    prev_obs = ref_obs_init

    for oc in np.where((obs_sorted.mask == False) &
                       (obs_sorted >= ref_obs_init))[0]:

        if (obs_sorted[oc] - prev_obs) > gap_threshold_cm[rc]:

            # This and all following observations on this side of obs_sorted
            # will be flagged, if they fit into the database.

            # Identify location of observation in time series.
            ts_ind = sort_ind[oc]

            ts_flag_ind.append(ts_ind)
            ref_obs.append(prev_obs)

        else:
            prev_obs = obs_sorted[oc]


    # Lower gap check.

    # Initialize the reference value.
    prev_obs = ref_obs_init

    for oc in np.flipud(np.where((obs_sorted.mask == False) &
                                 (obs_sorted < ref_obs_init))[0]):

        if (prev_obs - obs_sorted[oc]) > gap_threshold_cm[rc]:

            # This and all following observations on this side of obs_sorted
            # will be flagged, if they fit into the database.

            # Identify location of observation in time series.
            ts_ind = sort_ind[oc]

            ts_flag_ind.append(ts_ind)
            ref_obs.append(prev_obs)

        else:
            prev_obs = obs_sorted[oc]

    return ts_flag_ind, ref_obs


def qc_durre_snwd_tair(snow_depth_value_cm,
                       prev_sd_value_cm,
                       prev_sd_qc,
                       prev_at_value_deg_c):
    """
    Internal and temporal consistency checks on temperature
    (Durre 2010, Table 3):
    Snow-temperature consistency check,
    variant "SNWD" (for changes in snow depth.)
    """

    # In Durre (2010), the test is described this way:
    #
    # SNWD(0) - SNWD(-1) >= 0 and min[TMIN(-1:1)] >= 7 deg C
    #
    # In this adaptation the snow depth difference is between site_snwd_val_cm
    # and the latest unmasked value in site_prev_snwd_val, and the flag is
    # returned as True (flagged) if an increase in snow depth between those
    # adjacent depth reports is accompanied by a minimum observed temperature
    # (over the same time period) >= 7 degrees Celsius.

    # Mask previous snow depth data that have any QC flags set.
    prev_sd_value_cm = np.ma.masked_where(prev_sd_qc != 0,
                                          prev_sd_value_cm)

    # Count unmasked previous snow depth data.
    num_unmasked_prev_snwd = prev_sd_value_cm.count()

    if num_unmasked_prev_snwd == 0:
        return None, None

    # Locate the snow depth observation adjacent to the one being QCed.
    ref_ind = np.max(np.where(prev_sd_value_cm.mask == False))

    # Get all temperatures between the above observation and the one being
    # QCed.
    contextual_at_values_deg_c = prev_at_value_deg_c[ref_ind:]
    if contextual_at_values_deg_c.count() < 2:
        return None, None

    if snow_depth_value_cm <= prev_sd_value_cm[ref_ind]:
        return False, ref_ind

    if contextual_at_values_deg_c.min() >= 7.0:
        return True, ref_ind
    else:
        return False, ref_ind


def qc_durre_snwd_snfl(site_snwd_val_cm,
                       site_prev_snwd_val,
                       prev_sd_qc,
                       site_snfl_val_cm):
    """
    Internal and temporal consistency checks (Durre 2010, Table 3):
    Snowfall--snow depth consistency check
    site_snwd_val_cm - snow depth value being QCed
    site_prev_snwd_val - previous snow depth values (time series)
    prev_sd_qc - QC flags for previous snow depth values (time series)
    site_snfl_val_cm - snowfall accumulation
    """

    # Forgiveness for snow depth change exceeding reported snowfall;
    # 6.0 cm = 2.36 inches, which will prevent any round-off problems with
    # 2 inch snowfall reports.
    snfl_forgiveness_cm = 6.0

    # Mask previous snow depth data that have any QC flags set.
    site_prev_snwd_val = np.ma.masked_where(prev_sd_qc != 0,
                                            site_prev_snwd_val)

    # Count unmasked previous snow depth data.
    num_unmasked_prev_snwd = site_prev_snwd_val.count()

    if num_unmasked_prev_snwd == 0:
        return None, None

    # Locate the snow depth observation furthest from the one being QCed.
    ref_ind = np.min(np.where(site_prev_snwd_val.mask == False))

    # print(type(site_snwd_val_cm))
    # print(type(site_prev_snwd_val[ref_ind]))
    # print(type(site_snfl_val_cm))
    if site_snwd_val_cm - site_prev_snwd_val[ref_ind] > \
       site_snfl_val_cm + snfl_forgiveness_cm:
        return True, ref_ind
    else:
        return False, ref_ind


def qc_durre_snwd_prcp(site_snwd_val_cm,
                       site_prev_snwd_val,
                       prev_sd_qc,
                       site_prcp_val_mm):
    '''
    Internal and temporal consistency checks (Durre 2010, Table 3):
    Precipitation--snow depth consistency check; i.e. "SNWD increase with 0
    PRCP".
    site_snwd_val_cm - snow depth value being QCed
    site_prev_snwd_val - previous snow depth values (time series)
    prev_sd_qc - QC flags for previous snow depth values (time series)
    site_prcp_val_mm - precipitation accumulation
    '''

    # In Durre (2010), the test is described this way:
    #
    # SNWD(0) - SNWD(-1) >= 100 mm and MAX[PRCP(-1:1)] = 0
    #
    # In this adaptation the snow depth difference is between site_snwd_val_cm
    # and the earliest unmasked value in site_prev_snwd_val, and there is only
    # one precipitation value: site_prcp_val_mm, which should correspond
    # roughly to the time period covered by site_prev_snwd_val and
    # site_snwd_val_cm. Instead of a strict zero threshold for precipitation
    # this test uses a potentially nonzero prcp_accum_threshold_mm

    snwd_change_threshold_cm = 10.0
    prcp_accum_threshold_mm = 0.1

    # Mask previous snow depth data that have any QC flags set.
    site_prev_snwd_val = np.ma.masked_where(prev_sd_qc != 0,
                                            site_prev_snwd_val)

    # Count unmasked previous snow depth data.
    num_unmasked_prev_snwd = site_prev_snwd_val.count()

    if num_unmasked_prev_snwd == 0:
        return None, None

    # Locate the snow depth observation furthest from the one being QCed.
    ref_ind = np.min(np.where(site_prev_snwd_val.mask == False))

    if site_snwd_val_cm - site_prev_snwd_val[ref_ind] >= \
       snwd_change_threshold_cm and \
       site_prcp_val_mm < prcp_accum_threshold_mm:
        return True, ref_ind
    else:
        return False, ref_ind


def qc_durre_snwd_prcp_ratio(site_snwd_val_cm,
                             site_prev_snwd_val_cm,
                             prev_sd_qc,
                             site_prcp_val_mm):
    '''
    Internal and temporal consistency checks (Durre 2010, Table 3):
    Precipitation--snow depth consistency check; i.e. "SNWD/PRCP ratio".
    site_snwd_val_cm - snow depth value being QCed
    site_prev_snwd_val_cm - previous snow depth values (time series)
    prev_sd_qc - QC flags for previous snow depth values (time series)
    site_prcp_val_mm - precipitation accumulation
    '''

    # In Durre (2010), the test is described this way:
    # 
    # SNWD(0) - SNWD(-1) >= 200 mm and 
    # SNWD(0) - SNWD(-1) >= 100[PRCP(0) + PRCP(-1)] and
    # SNWD(0) - SNWD(-1) >= 100[PRCP(0) + PRCP(1)] and
    #
    # In this adaptation the snow depth difference is between site_snwd_val_cm
    # and the earliest unmasked value in site_prev_snwd_val_cm, and there is
    # only one precipitation value: site_prcp_val_mm, which should correspond
    # roughly to the time period covered by site_prev_snwd_val_cm and
    # site_snwd_val_cm.

    snwd_change_threshold_cm = 20.0
    snwd_prcp_ratio_threshold = 100

    # Mask previous snow depth data that have any QC flags set.
    site_prev_snwd_val_cm = np.ma.masked_where(prev_sd_qc != 0,
                                               site_prev_snwd_val_cm)

    # Count unmasked previous snow depth data.
    num_unmasked_prev_snwd = site_prev_snwd_val_cm.count()

    if num_unmasked_prev_snwd == 0:
        return None, None

    # Locate the snow depth observation furthest from the one being QCed.
    ref_ind = np.min(np.where(site_prev_snwd_val_cm.mask == False))

    # No flag for zero precipitation; that should be handled by a separate
    # test.
    if site_prcp_val_mm == 0.0:
        return False, ref_ind

    site_snwd_change_cm = site_snwd_val_cm - site_prev_snwd_val_cm[ref_ind]
    if site_snwd_change_cm >= snwd_change_threshold_cm and \
       10.0 * site_snwd_change_cm / site_prcp_val_mm >= \
       snwd_prcp_ratio_threshold:
        return True, ref_ind
    else:
        return False, ref_ind


def qc_durre_snwd_tair_spatial(snow_depth_value_cm,
                               prev_sd_value_cm,
                               prev_sd_qc,
                               nhood_tair_deg_c):
    """
    Spatial snow-temperature consistency check for changes in snow depth,
    using neighborhood temperature reports.
    nhood_tair_deg_c rows represent neighbors, columns represent times.
    """

    # Determine the number of hours of preceding data.
    num_prev_hours = prev_sd_value_cm.shape[0]

    # Verify that the air temperature data are consistent in the time
    # dimension.
    if nhood_tair_deg_c.shape[1] != num_prev_hours + 1:
        print('ERROR: snow depth and air temperature data have ' +
              'inconsistent time dimensions.',
              file=sys.stderr)
        sys.exit(1)

    # Mask previous snow depth data that have any QC flags set.
    prev_sd_value_cm = np.ma.masked_where(prev_sd_qc != 0,
                                          prev_sd_value_cm)

    # Count unmasked previous snow depth data.
    num_unmasked_prev_snwd = prev_sd_value_cm.count()

    if num_unmasked_prev_snwd == 0:
        return None, None

    # Locate the snow depth observation adjacent to the one being QCed.
    ref_ind = np.max(np.where(prev_sd_value_cm.mask == False))

    # Get all neighboring temperatures between the above observation and the
    # one being QCed.
    contextual_nhood_tair_deg_c = nhood_tair_deg_c[ref_ind:]

    # Note that the subsetting above could result in some stations in the
    # neighborhood providing us with no data, which would effectively reduce
    # the size of the neighborhood.
    if contextual_nhood_tair_deg_c.count() < 2:
        return None, None

    if snow_depth_value_cm <= prev_sd_value_cm[ref_ind]:
        return False, ref_ind

    if contextual_nhood_tair_deg_c.min() >= 7.0:
        return True, ref_ind
    else:
        return False, ref_ind


def qc_durre_swe_wre(value_mm):
    """
    Basic integrity checks:
    swe world record exceedance.
    Uses the snow depth threshold from Durre (2010), but in mm instead of cm
    (implicit 10:1 ratio).
    """
    threshold_value = 1146.0
    if value_mm < 0.0 or value_mm > threshold_value:
        return True
    else:
        return False


def qc_durre_swe_change_wre(swe_value_mm,
                            prev_swe_value_mm,
                            prev_swe_qc):
    """
    Basic integrity checks:
    swe increase world record exceedance.
    Uses the snow depth threshold from Durre (2010), but in mm instead of cm
    (implicit 10:1 ratio).
    """

    threshold_swe_increase_mm = 192.5

    # Mask previous swe data that have any QC flags set.
    prev_swe_value_mm = np.ma.masked_where(prev_swe_qc != 0,
                                           prev_swe_value_mm)

    num_unmasked_prev_swe = prev_swe_value_mm.count()

    if num_unmasked_prev_swe == 0:
        # Test is not possible.
        return None, None

    # Find the minimum observed preceding swe value for comparison with the
    # observation being checked.
    ref_ind = np.ma.argmin(prev_swe_value_mm)

    if swe_value_mm - prev_swe_value_mm[ref_ind] > \
       threshold_swe_increase_mm:
        return True, ref_ind
    else:
        return False, ref_ind


def qc_durre_swe_streak(swe_value_mm,
                        prev_swe_value_mm,
                        prev_swe_qc,
                        streak_value_threshold=None):
    """
    Basic integrity checks:
    swe streak check.
    """
    if streak_value_threshold is None:
        streak_value_threshold = 0.1
    streak_min_consecutive = 10

    # Mask previous swe data that have any QC flags set.
    prev_swe_value_mm = np.ma.masked_where(prev_swe_qc != 0,
                                           prev_swe_value_mm)

    # Assemble previous and current data into one time series.
    station_time_series = np.ma.append(prev_swe_value_mm, swe_value_mm)

    if station_time_series.count() < streak_min_consecutive:
        return None

    if np.ma.max(station_time_series) <= streak_value_threshold:
        return None
    
    if (np.ma.max(station_time_series) -
        np.ma.min(station_time_series)) < streak_value_threshold:
        return True
    else:
        return False


def qc_durre_swe_gap(swe_value_mm,
                     prev_swe_value_mm,
                     prev_swe_qc,
                     ref_ceiling_mm=None,
                     ref_default_mm=None,
                     verbose=None):
    '''
    Outlier checks:
    Gap check for SWE.
    Uses the same thresholds as qc_durre_snwd_gap, but in mm instead of cm
    (implicit 10:1 ratio).
    If ref_ceiling_mm and ref_default_mm are included, then when the default
    reference value (median_obs) exceeds ref_ceiling_mm, it is replaced by the
    value in the station_time_series that is nearest to ref_default_mm. The
    purpose of this option is to override median_obs values that are based on
    dubious large values of SWE, which often occur at automated sites.
    '''

    logger = logging.getLogger()

    # First value of reporting_rate_threshold needs to be zero!
    reporting_rate_threshold = [0.0, 0.25, 0.75, 6.0, 18.0]

    gap_threshold_mm = [100.0,
                        75.0,
                        60.0,
                        45.0,
                        30.0]

    reporting_rate_name = ['sporadic',
                           'quasi-daily',
                           'daily',
                           'synoptic',
                           'hourly']

    # Mask previous SWE data that have any QC flags set.
    prev_swe_value_mm = np.ma.masked_where(prev_swe_qc != 0,
                                           prev_swe_value_mm)

    # Assemble previous and current data into one time series.
    # Note that this guarantees that station_time_series will have at least
    # one unmasked value (the observation being QCed, at the end), even if all
    # the others are masked.
    station_time_series = np.ma.append(prev_swe_value_mm, swe_value_mm)

    # Note: add reporting_rate_threshold, gap_threshold_mm,
    # and reporting_rate_name as INPUTS to obs_rate_category,
    rc = obs_rate_category(station_time_series)

    if rc is None:
        logger.warning('No match for observation rate.')
        return None

    # Sort observations to simulate a cumulative distribution function.
    sort_ind = station_time_series.argsort()
    obs_sorted = station_time_series[sort_ind]
    median_obs = np.ma.median(station_time_series)

    # Initialize the reference value to the median.
    ref_obs_init = median_obs

    if ref_ceiling_mm is not None and \
       ref_default_mm is not None:
        # Replace ref_obs_init with the time series element nearest in value
        # to ref_default_mm (typically the climatological median), if
        # ref_obs_init exceeds the ref_ceiling_mm.
        if ref_obs_init > ref_ceiling_mm:
            ind = (np.abs(station_time_series - ref_default_mm)).argmin()
            ref_obs_init = station_time_series[ind]
            logger.debug('Replacing median {} '.format(median_obs) +
                         'with value {} '.format(ref_obs_init) +
                         '(observation nearer to climatology) ' +
                         'in gap check.')

    # Initialize list of flagged reports.
    ts_flag_ind = []
    ref_obs = []

    # Upper gap check.

    # Initialize the reference value.
    prev_obs = ref_obs_init

    for oc in np.where((obs_sorted.mask == False) &
                       (obs_sorted >= ref_obs_init))[0]:

        if (obs_sorted[oc] - prev_obs) > gap_threshold_mm[rc]:

            # This and all following observations on this side of obs_sorted
            # will be flagged, if they fit into the database.

            # Identify location of observation in time series.
            ts_ind = sort_ind[oc]

            ts_flag_ind.append(ts_ind)
            ref_obs.append(prev_obs)

        else:
            prev_obs = obs_sorted[oc]


    # Lower gap check.

    # Initialize the reference value.
    prev_obs = ref_obs_init

    for oc in np.flipud(np.where((obs_sorted.mask == False) &
                                 (obs_sorted < ref_obs_init))[0]):

        if (prev_obs - obs_sorted[oc]) > gap_threshold_mm[rc]:

            # This and all following observations on this side of obs_sorted
            # will be flagged, if they fit into the database.

            # Identify location of observation in time series.
            ts_ind = sort_ind[oc]

            ts_flag_ind.append(ts_ind)
            ref_obs.append(prev_obs)

        else:
            prev_obs = obs_sorted[oc]

    return ts_flag_ind, ref_obs


def qc_durre_swe_prcp(site_swe_val_mm,
                      site_prev_swe_val,
                      prev_swe_qc,
                      site_prcp_val_mm):
    '''
    Precipitation--SWE consistency check; i.e. "SWE increase with 0
    PRCP".
    site_swe_val_mm - SWE value being QCed
    site_prev_swe_val - previous SWE values (time series)
    prev_swe_qc - QC flags for previous SWE values (time series)
    site_prcp_val_mm - precipitation accumulation
    '''

    swe_change_threshold_mm = 10.0
    prcp_accum_threshold_mm = 0.1

    # Mask previous swe data that have any QC flags set.
    site_prev_swe_val = np.ma.masked_where(prev_swe_qc != 0,
                                           site_prev_swe_val)

    # Count unmasked previous swe data.
    num_unmasked_prev_swe = site_prev_swe_val.count()

    if num_unmasked_prev_swe == 0:
        return None, None

    # Locate the swe observation furthest from the one being QCed.
    ref_ind = np.min(np.where(site_prev_swe_val.mask == False))

    if site_swe_val_mm - site_prev_swe_val[ref_ind] >= \
       swe_change_threshold_mm and \
       site_prcp_val_mm < prcp_accum_threshold_mm:
        return True, ref_ind
    else:
        return False, ref_ind


def qc_durre_swe_prcp_ratio(site_swe_val_mm,
                             site_prev_swe_val_mm,
                             prev_swe_qc,
                             site_prcp_val_mm):
    '''
    Internal and temporal consistency checks (Durre 2010, Table 3)
    Precipitation - snow water equivalent ("SWE/PRCP ratio") consistency
    check, adapted from SNWD/PRCP ratio" test.
    '''

    swe_change_threshold_mm = 20.0
    swe_prcp_ratio_threshold = 100

    # Mask previous SWE data that have any QC flags set.
    site_prev_swe_val_mm = np.ma.masked_where(prev_swe_qc != 0,
                                              site_prev_swe_val_mm)

    # Count unmasked previous swe data.
    num_unmasked_prev_swe = site_prev_swe_val_mm.count()

    if num_unmasked_prev_swe == 0:
        return None, None

    # Locate the SWE observation furthest from the one being QCed.
    ref_ind = np.min(np.where(site_prev_swe_val_mm.mask == False))

    # No flag for zero precipitation; that should be handled by a separate
    # test.
    if site_prcp_val_mm == 0.0:
        return False, ref_ind

    site_swe_change_mm = site_swe_val_mm - site_prev_swe_val_mm[ref_ind]
    if site_swe_change_mm >= swe_change_threshold_mm and \
       site_swe_change_mm / site_prcp_val_mm >= \
       swe_prcp_ratio_threshold:
        return True, ref_ind
    else:
        return False, ref_ind


def parse_args():
    """
    Parse command line arguments.
    """
    help_message = 'Update quality control information for station ' + \
                   'observations in the NOHRSC web database.'
    default_pkl_dir = os.path.join('/net/scratch', os.getlogin())
    parser = argparse.ArgumentParser(description=help_message)
    parser.add_argument('-m', '--min_days_latency',
                        type=int,
                        metavar='# of days',
                        default=0,
                        nargs='?',
                        help='Set the minimum days of latency for ' + \
                             'updates. This setting is intended for ' + \
                             'near-real-time updates, to force updates ' + \
                             'for times prior to the ' + \
                             '"last_datetime_updated" attribute in case ' + \
                             'new observations have appeared for those ' + \
                             'times since the previous update. Its use is ' + \
                             'not generally recommended for retrospective ' + \
                             'updates (i.e., those occurring more than ' + \
                             '60 days earlier than real time).')
    parser.add_argument('-x', '--max_update_hours',
                        type=int,
                        metavar='# of hours',
                        nargs='?',
                        help='Set the maximum number of hours to update.')
    parser.add_argument('database_path',
                        type=str,
                        metavar='database',
                        help='QC database file (full path)')
    parser.add_argument('-u', '--metadata_update_interval_hours',
                        type=int,
                        metavar='# of hours',
                        nargs='?',
                        default='24',
                        help='Set the station metadata update interval ' +
                             '(hours); default=24.')
    parser.add_argument('-c', '--check_climatology',
                        action='store_true',
                        help='Enhance QC tests using SNODAS climatology.')
    parser.add_argument('-p', '--pkl_dir',
                        type=str,
                        metavar='dir',
                        nargs='?',
                        default=default_pkl_dir,
                        help='Set directory for reading and writing .pkl ' + \
                             'files generated by observational database ' + \
                             'queries; default={}.'.format(default_pkl_dir))
    parser.add_argument('-v', '--verbose',
                        action='store_true',
                        help='Provide verbose output.')

    args = parser.parse_args()

    if args.min_days_latency < 0:
        print('ERROR: --min_days_latency argument must be nonnegative.',
              format=sys.stderr)
        sys.exit(1)

    if args.max_update_hours is not None:
        if args.max_update_hours <= 0:
            print('ERROR: --max_update_hours argument must be positive.',
                  format=sys.stderr)
            sys.exit(1)

    if args.pkl_dir is not None:
        if not os.path.isdir(args.pkl_dir):
            raise FileNotFoundError(errno.ENOENT,
                                    os.strerror(errno.ENOENT),
                                    args.pkl_dir)

    return args


def main():
    """
    Update quality control information for station observations in the
    NOHRSC web database.
    """

    # Initialize logger.
    logger = local_logger.init(logging.WARNING)
    if sys.stdout.isatty():
        logger.setLevel(logging.INFO)
    
    # Read command line arguments.
    args = parse_args()
    if args is None:
        #print('ERROR: Failed to parse command line.', file=sys.stderr)
        logger.error('Failed to parse command line.')
        sys.exit(1)

    if args.verbose:
        logger.setLevel(logging.DEBUG)
        
    if not os.path.exists(args.database_path):
        # print('ERROR: {} not found.'.format(args.database_path),
        #       file=sys.stderr)
        logger.error('{} not found.'.format(args.database_path))
        sys.exit(1)

    # Set configuration parameters.

    # Temporary file storage for observations read from the web database
    # (intended for .pkl files generated by wdb0.py).
    # pkl_dir = '/net/scratch/nwm_snow_da/wdb0_pkl'
    # print(pkl_dir)
    # print(args.pkl_dir)
    # sys.exit(1)
    # pkl_dir = '/net/scratch/{}'.format(os.getlogin())

    # Set the commit period in hours. This is the number of hours updated
    # before a "commit" is performed by copying the temp_database_path back
    # over to args.database_path and generating a new temporary database
    # copy.
    database_commit_period = 3

    # Open the QC database.
    try:
        qcdb = Dataset(args.database_path, 'r')
    except:
        logger.error('Failed to open QC database {}.'
                     .format(args.database_path))
        sys.exit(1)
    temp_database_exists = True

    # Read the time variable.
    try:
        qcdb_var_time = qcdb.variables['time']
    except:
        print('ERROR: Database file {} '.format(args.database_path) +
              'has no "time" variable.',
              file=sys.stderr)
        qcdb.close()
        exit(1)
    if (len(qcdb_var_time.dimensions) != 1 or
        qcdb_var_time.dimensions[0] != 'time'):
        print('ERROR: Database file {} '.format(args.database_path) +
              '"time" variable has unexpected structure.',
              file=sys.stderr)
        qcdb.close()
        exit(1)
    try:
        qcdb_var_time_units = qcdb_var_time.getncattr('units')
    except:
        print('ERROR: Database file {} '.format(args.database_path) +
              '"time" variable has no "units" attribute.',
              file=sys.stderr)
        qcdb.close()
        exit(1)

#    var_station_obj_id = qcdb.variables['station_obj_identifier']

    # Read the station object ID variable.
    try:
        qcdb_var_station_obj_id = qcdb.variables['station_obj_identifier']
    except:
        print('ERROR: Database file {} '.format(args.database_path) +
              'has no "station_obj_identifier" variable.',
              file=sys.stderr)
        qcdb.close()
        exit(1)
    if (len(qcdb_var_station_obj_id.dimensions) != 1 or
        qcdb_var_station_obj_id.dimensions[0] != 'station'):
        print('ERROR: Database file {} '.format(args.database_path) +
              '"station_obj_identifier" variable has unexpected structure.',
              file=sys.stderr)
        qcdb.close()
        exit(1)

    # Get the start/end datetimes of the QC database.
    qcdb_start_datetime = num2date(qcdb_var_time[0],
                                   units=qcdb_var_time_units,
                                   only_use_cftime_datetimes=False)
    qcdb_end_datetime = num2date(qcdb_var_time[-1],
                                 units=qcdb_var_time_units,
                                 only_use_cftime_datetimes=False)

    # Read the "last_datetime_updated" attribute.
    try:
        last_dt_updated_str = qcdb.getncattr('last_datetime_updated')
    except:
        print('ERROR: Database file {} '.format(args.database_path) +
              'has no "last_datetime_updated" attribute.',
              file=sys.stderr)
        qcdb.close()
        sys.exit(1)
    qcdb_last_datetime_updated = \
        dt.datetime.strptime(last_dt_updated_str, '%Y-%m-%d %H:%M:%S UTC')

    if args.check_climatology:

        sd_clim_dir = '/net/lfs0data5/SNODAS_climatology/snow_depth'
        swe_clim_dir = '/net/lfs0data5/SNODAS_climatology/swe'

    qcdb_snwd_qc_flag = qcdb.variables['snow_depth_qc']
    qcdb_snwd_qc_chkd = qcdb.variables['snow_depth_qc_checked']

    qcdb_swe_qc_flag = qcdb.variables['swe_qc']
    qcdb_swe_qc_chkd = qcdb.variables['swe_qc_checked']

    # Read the "last_station_update_datetime" attribute.
    try:
        last_station_update_str = \
            qcdb.getncattr('last_station_update_datetime')
    except:
        print('ERROR: Database file {} '.format(args.database_path) +
              'has no "last_station_update_datetime" attribute.',
              file=sys.stderr)
        qcdb.close()
        sys.exit(1)
    last_station_update_datetime = \
        dt.datetime.strptime(last_station_update_str,
                             '%Y-%m-%d %H:%M:%S UTC')

    # Select hours to update.
    # This selection is bracketed between:
    # - the larger of:
    #       the smaller of qcdb_last_datetime_updated and
    #                      current_update_datetime - min_days_latency - 1 hour
    #       and the start of the database
    #  
    # and
    # - the smaller of:
    #       current_update_datetime
    #       and the end of the database

    current_update_datetime = dt.datetime.utcnow()
    current_update_datetime_num = date2num(current_update_datetime,
                                           qcdb_var_time_units)
    time_bracket_below = min(qcdb_last_datetime_updated,
                             current_update_datetime -
                             dt.timedelta(days=args.min_days_latency) -
                             dt.timedelta(hours=1))
    time_bracket_below_num = date2num(time_bracket_below, qcdb_var_time_units)
    qcdb_update_time_ind = np.where((qcdb_var_time[:] >
                                     time_bracket_below_num) & 
                                    (qcdb_var_time[:] <= 
                                     current_update_datetime_num))[0]

    if len(qcdb_update_time_ind) == 0:
        logger.info('No dates to update in {}.'.format(args.database_path))
        qcdb.close()
        sys.exit(0)

    if args.verbose:
        if args.max_update_hours is not None:
            logger.info('Updating for {} out of a possible {} times.'.
                        format(args.max_update_hours,
                               len(qcdb_update_time_ind)))
        else:
            logger.info('Updating for {} times.'.
                        format(len(qcdb_update_time_ind)))

    # Make sure times to update are sorted in chronological order so
    # older data are quality controlled first. This is a totally
    # unnecessary step as long as the qcdb_var_time is already in
    # chronological order, which it always will be. But this is fast
    # and worth keeping here as a guarantee.
    ind = np.argsort(qcdb_var_time[qcdb_update_time_ind])
    qcdb_update_time_ind = qcdb_update_time_ind[ind]
    ind = None

    # Read the station dimension from the QC database file.
    try:
        qcdb_num_stations = qcdb.dimensions['station'].size
    except:
        logger.error('Database file {} '.format(args.database_path) +
                     'has no "station" dimension.')
        qcdb.close()
        sys.exit(1)
    logger.debug('QC database {} has {} stations.'.
                 format(args.database_path, qcdb_num_stations))

    # Get all qcdb variables along the station dimension that have a
    # "allstation_column_name" attribute.
    #"qcdb_station_vars"
    qcdb_station_vars = \
        qcdb.get_variables_by_attributes(allstation_column_name=
                                         lambda v: v is not None)
    wdb_col_list_str = '' # string list of database columns
    wdb_col_list = [] # list of dataframe columns
    obj_id_found = False
    qcdb_lon_var = None
    qcdb_lat_var = None
    # TODO: add longitude_found and latitude_found
    for qcdb_station_var in qcdb_station_vars:
        if qcdb_station_var.ndim != 1:
            # May want to issue a warning about variables having a
            # 'allstation_column_name' attribute but not having a
            # single dimension (which should be 'station').
            continue
        if qcdb_station_var.dimensions[0] != 'station':
            # Ditto "may want to issue a warning" as above.
            continue
        allstation_column_name = \
            qcdb_station_var.getncattr('allstation_column_name')
        if allstation_column_name == 'obj_identifier':
            qcdb_obj_id_var = qcdb_station_var
            obj_id_found = True
        if allstation_column_name == 'coordinates[0]':
            qcdb_lon_var = qcdb_station_var
        if allstation_column_name == 'coordinates[1]':
            qcdb_lat_var = qcdb_station_var
        # if allstation_column_name == 'start_date':
        #     start_date_var = qcdb_station_var
        #     start_date_found = True
        # if allstation_column_name == 'stop_date':
        #     stop_date_var = qcdb_station_var
        #     stop_date_found = True
        # if allstation_column_name == 'use':
        #     use_var = qcdb_station_var
        #     use_found = True
        if len(wdb_col_list_str) == 0:
            wdb_col_list_str = wdb_col_list_str + allstation_column_name
        else:
            wdb_col_list_str = wdb_col_list_str + ', ' + allstation_column_name
        wdb_col_list.append(allstation_column_name)
    if obj_id_found is False:
        print('ERROR: No variable in {}'.format(args.database_path) + 
              ' using the "station" dimension has an ' +
              '"allstation_column_name" attribute of "obj_identifier".',
              file=sys.stderr)
        qcdb.close()
        exit(1)
    if qcdb_lon_var is None:
        print('ERROR: No variable in {}'.format(args.database_path) + 
              ' using the "station" dimension has an ' +
              '"allstation_column_name" attribute of "longitude".',
              file=sys.stderr)
        qcdb.close()
        exit(1)
    if qcdb_lat_var is None:
        print('ERROR: No variable in {}'.format(args.database_path) + 
              ' using the "station" dimension has an ' +
              '"allstation_column_name" attribute of "latitude".',
              file=sys.stderr)
        qcdb.close()
        exit(1)




    #############################################################
    # Switch from the original QC database to a temporary copy. #
    #############################################################

    qcdb.close()
    temp_database_path = station_qc_db_copy(args.database_path,
                                            verbose=args.verbose)
    try:
        qcdb = Dataset(temp_database_path, 'r+')
    except:
        logger.error('Failed to open QC database {}.'
                     .format(temp_database_path))
        sys.exit(1)
    temp_database_exists = True









    # Decide whether or not to update metadata.
    if last_station_update_str == '1970-01-01 00:00:00 UTC':
        note = ' (i.e., never)'
    else:
        note = ''
    logger.debug('{} station metadata last updated {}'.
                 format(temp_database_path,
                        last_station_update_str) + note)

    time_since_metadata_update = \
        current_update_datetime - last_station_update_datetime
    hours_since_metadata_update = \
        timedelta_to_int_hours(time_since_metadata_update)
    if (qcdb_obj_id_var.size > 0) and \
       (hours_since_metadata_update >= args.metadata_update_interval_hours):
        if args.verbose:
            logger.info('Updating station metadata.')
        update_qc_db_metadata(qcdb,
                              qcdb_obj_id_var,
                              qcdb_lon_var,
                              qcdb_lat_var,
                              qcdb_station_vars,
                              wdb_col_list,
                              verbose=args.verbose)
    # else:
    #     print('time since metadata update:')
    #     print(time_since_metadata_update)
    #     print('hours since metadata update is {}'.format(hours_since_metadata_update))
    #     qcdb.close()
    #     exit(1)

    # Set parameters.
    num_hrs_wre = 24
    num_hrs_streak = 15 * 24
    num_hrs_gap = 15 * 24
    num_hrs_prev_tair = 24
    num_hrs_snowfall = 24
    num_hrs_prcp = 24

    streak_value_threshold = 0.1

    # Switch for flagging low values in tests involving snow depth change.
    flag_sd_change_wre_low_value = False
    flag_sd_change_tair_low_value = False # Affects spatial test as well.
    flag_sd_change_snfl_low_value = False
    flag_sd_change_prcp_low_value = False # Affects ratio test as well.

    # Switch for flagging low values in tests involving swe change.
    flag_swe_change_wre_low_value = False
    flag_swe_change_prcp_low_value = False

    # Debugging.
    debug_station_id = None
    # debug_station_id = 'UTSCI_MADIS'

    wdb_prev_tair = None
    wdb_prev_tair_fetch_datetimes = None
    #wdb_prev_tair_fetch_elapsed = None
    data_elapsed_cutoff_seconds = 60 * 24 * 3600 # 60 days
    fetch_elapsed_cutoff_seconds = 2 * 60       # 10 minutes
    
    num_stations_added = 0
    num_flagged_sd_wr = 0
    num_flagged_sd_change_wr = 0
    num_flagged_sd_streak = 0
    num_flagged_sd_gap = 0
    num_flagged_sd_at_cons = 0
    num_flagged_sd_sf_cons = 0
    num_flagged_sd_pr_cons = 0
    num_flagged_sd_at_spatial_cons = 0

    num_flagged_swe_wr = 0
    num_flagged_swe_change_wr = 0
    num_flagged_swe_streak = 0
    num_flagged_swe_gap = 0
    num_flagged_swe_pr_cons = 0

    num_hrs_updated = 0
    qcdb_num_stations_start = 0


    ##################################
    # Loop over all times to update. #
    ##################################

    for qcdb_ti in qcdb_update_time_ind:
        obs_datetime_num = qcdb_var_time[qcdb_ti]
        obs_datetime = num2date(obs_datetime_num,
                                units=qcdb_var_time_units,
                                only_use_cftime_datetimes=False)
        # if args.verbose:
        logger.info('Updating QC database for {}.'.format(obs_datetime))

        # Initialize counters for the current time.
        num_stations_added_this_time = 0

        #######################
        # SNOW DEPTH QC BEGIN #
        #######################


        # Get snow depth and other observations used for snow depth QC.


        # Get all snow depth data for this datetime.
        t1 = dt.datetime.utcnow()
        wdb_snwd = wdb0.get_snow_depth_obs(obs_datetime,
                                           obs_datetime,
                                           scratch_dir=args.pkl_dir,
                                           verbose=args.verbose)
        t2 = dt.datetime.utcnow()
        elapsed_time = t2 - t1
        logger.debug('Found {} snow depth reports.'.
                     format(wdb_snwd['num_stations']))
        logger.debug('Query ran in {} seconds.'.
                     format(elapsed_time.total_seconds()))

        # Previous snow depth data is needed for multiple tests.
        # - World record increase exceedance check uses 24 hours.
        # - Streak check uses 15 days.
        # - Gap check uses 15 days.
        # - Temperature consistency checks use 24 hours.
        # - Snowfall consistency check uses 24 hours.
        # - Precipitation consistency checks use 24 hours.
        num_hrs_prev_snwd = max(num_hrs_wre,
                                num_hrs_streak,
                                num_hrs_gap,
                                num_hrs_prev_tair,
                                num_hrs_snowfall,
                                num_hrs_prcp)

        if args.check_climatology:

            # Get SNODAS snow depth climatology data for the current time.
            wdb_snwd_clim_med_mm = \
                snodas_clim.at_loc(sd_clim_dir,
                                   obs_datetime,
                                   wdb_snwd['station_lon'],
                                   wdb_snwd['station_lat'],
                                   element='snow_depth',
                                   metric='median',
                                   sampling='neighbor')
            wdb_snwd_clim_max_mm = \
                snodas_clim.at_loc(sd_clim_dir,
                                   obs_datetime,
                                   wdb_snwd['station_lon'],
                                   wdb_snwd['station_lat'],
                                   element='snow_depth',
                                   metric='max',
                                   sampling='neighbor')
            wdb_snwd_clim_iqr_mm = \
                snodas_clim.at_loc(sd_clim_dir,
                                   obs_datetime,
                                   wdb_snwd['station_lon'],
                                   wdb_snwd['station_lat'],
                                   element='snow_depth',
                                   metric='iqr',
                                   sampling='neighbor')


        # Get previous num_hrs_prev_snwd hours of snow depth data.
        t1 = dt.datetime.utcnow()
        wdb_prev_snwd = \
            wdb0.get_prev_snow_depth_obs(obs_datetime,
                                         num_hrs_prev_snwd,
                                         scratch_dir=args.pkl_dir,
                                         verbose=args.verbose)
        t2 = dt.datetime.utcnow()
        elapsed_time = t2 - t1

        logger.debug('Found {} '.
                     format(wdb_prev_snwd['values_cm'].count()) +
                     'preceding snow depth reports ' +
                     'from {} stations.'.
                     format(wdb_prev_snwd['num_stations']))
        logger.debug('Query ran in {} seconds.'.
                     format(elapsed_time.total_seconds()))

        # Extract previous snow depth values and station object identifiers,
        # for convenience (shorter variable names).
        wdb_prev_snwd_val_cm = wdb_prev_snwd['values_cm']
        wdb_prev_snwd_obj_id = wdb_prev_snwd['station_obj_id']

        # Get previous num_hrs_prev_snwd hours of snow depth QC data.
        t1 = obs_datetime - dt.timedelta(hours=num_hrs_prev_snwd)
        t1_num = date2num(t1, qcdb_var_time_units)
        t1_ind = int(t1_num - qcdb_var_time[0])
        t2 = obs_datetime - dt.timedelta(hours=1)
        t2_num = date2num(t2, qcdb_var_time_units)
        t2_ind = int(t2_num - qcdb_var_time[0])

        # Remove negative indices.
        left_ind = max(t1_ind, 0)
        right_ind = max(t2_ind + 1, 0)

        qcdb_prev_snwd_qc_flag = qcdb_snwd_qc_flag[:, left_ind:right_ind]

        # Calculate the number of hours to add at the start of
        # qcdb_prev_snwd_qc_flag, for cases where num_hrs_prev_snwd extends
        # to times earlier than the time range covered by the database.
        num_pad_hours = min(left_ind - t1_ind, num_hrs_prev_snwd)

        # Programming check.
        if qcdb_prev_snwd_qc_flag.shape[1] + num_pad_hours != \
           num_hrs_prev_snwd:
            print('ERROR: (programming) miscalculation of num_pad_hours.',
                  file=sys.stderr)
            qcdb.close()
            sys.exit(1)

        # If necessary (i.e., if any or all of the previous
        # num_hrs_prev_snwd hours fall outside the time domain of the QC
        # database), pad previous snow depth QC data with zeroes.
        if num_pad_hours > 0 and qcdb_num_stations > 0:
            logger.debug('Padding previous snow depth QC flag ' +
                         'with {} hours of zeroes.'.
                         format(num_pad_hours))

            new_qc = np.ma.masked_array(np.array([[0] * num_pad_hours] *
                                                 qcdb_num_stations))
            qcdb_prev_snwd_qc_flag = \
                np.ma.concatenate([new_qc, qcdb_prev_snwd_qc_flag],
                                  axis=1)


        # Get snowfall data associated with snow depth observations.
        t1 = dt.datetime.utcnow()
        wdb_snfl = \
            wdb0.get_snwd_snfl_obs(obs_datetime,
                                   num_hrs_snowfall,
                                   scratch_dir=args.pkl_dir,
                                   verbose=args.verbose)
        t2 = dt.datetime.utcnow()
        elapsed_time = t2 - t1
        logger.debug('Found {} snowfall reports.'.
                     format(wdb_snfl['num_stations']))
        logger.debug('Query ran in {} seconds.'.
                     format(elapsed_time.total_seconds()))

        # Extract snowfall values and station object identifiers, for
        # convenience (shorter variable names).
        wdb_snfl_val_cm = wdb_snfl['values_cm']
        wdb_snfl_obj_id = wdb_snfl['station_obj_id']


        # Get precipitation data associated with snow depth observations.
        t1 = dt.datetime.utcnow()
        wdb_snwd_prcp = \
            wdb0.get_snwd_prcp_obs(obs_datetime,
                                   num_hrs_prcp,
                                   scratch_dir=args.pkl_dir,
                                   verbose=args.verbose)
        t2 = dt.datetime.utcnow()
        elapsed_time = t2 - t1
        logger.debug('Found {} precipitation reports.'.
                     format(wdb_snwd_prcp['num_stations']))
        logger.debug('Query ran in {} seconds.'.
                     format(elapsed_time.total_seconds()))

        # Extract precipitation values and station object identifiers, for
        # convenience (shorter variable names).
        wdb_snwd_prcp_val_mm = wdb_snwd_prcp['values_mm']
        wdb_snwd_prcp_obj_id = wdb_snwd_prcp['station_obj_id']

        # Get air temperature observations. These are needed for snow depth
        # reporters (for the snow-temperature consistency check) and for other
        # sites as well (for the spatial snow-temperature consistency check).
        current_datetime = dt.datetime.utcnow()
        # TODO:
        # If wdb_prev_tair exists already, and
        #    wdb_prev_tair_datetime is less than 15 minutes in the past
        #    then
        # use the prev_obs_air_temp=wdb_prev_tair keyword in the call to
        # wdb0.get_air_temp_obs
        # if wdb_prev_tair is not None:
        #     logger.debug('have fetched tair before during this update')
        #     if wdb_prev_tair_fetch_datetimes is None:
        #         logger.debug('weird - no wdb_prev_tair_fetch_datetimes')
        # else:
        #     logger.debug('have not fetched tair before during this update')
        #     if wdb_prev_tair_fetch_datetimes is not None:
        #         logger.debug('weird - have wdb_prev_tair_fetch_datetimes')

        if wdb_prev_tair is not None:
            # A copy of the datatimes for the prior wdb_prev_tair will be
            # needed to update wdb_prev_tair_fetch_datetimes.
            prior_wdb_prev_tair_datetime = \
                wdb_prev_tair['obs_datetime'].copy()
            # If the newest value of prior_wdb_prev_tair_datetime is less than
            # 60 days in the past, and the earliest value of
            # wdb_prev_tair_fetch_datetimes is more than 10 minutes in the
            # past (i.e., the maximum time elapsed since fetching data is more
            # than 10 minutes), we need to start over.
            current_datetime = dt.datetime.utcnow()
            data_elapsed = list(current_datetime - \
                                np.array(prior_wdb_prev_tair_datetime))
            data_elapsed_sec = [td.total_seconds()
                                for td in data_elapsed]
            fetch_elapsed = list(current_datetime - \
                                 wdb_prev_tair_fetch_datetimes)
            fetch_elapsed_sec = [td.total_seconds()
                                 for td in fetch_elapsed]

            logger.debug('min(data_elapsed_sec) = {}'.
                         format(min(data_elapsed_sec)))
            logger.debug('max(fetch_elapsed_sec) = {}'.
                         format(max(fetch_elapsed_sec)))

            if min(data_elapsed_sec) < data_elapsed_cutoff_seconds and \
               max(fetch_elapsed_sec) > fetch_elapsed_cutoff_seconds:
                logger.warning('Re-fetching all air temperature data.')
                wdb_prev_tair = None
                wdb_prev_tair_fetch_datetimes = None

        # if wdb_prev_tair is not None:
        #     logger.debug('wdb_prev_tair is not None')
        # else:
        #     logger.debug('wdb_prev_tair is None')

        wdb_prev_tair = \
            wdb0.get_air_temp_obs(obs_datetime -
                                  dt.timedelta(hours=num_hrs_prev_tair),
                                  obs_datetime,
                                  scratch_dir=args.pkl_dir,
                                  verbose=args.verbose,
                                  read_pkl=False,
                                  prev_obs_air_temp=wdb_prev_tair)

        # Compare time series for a randomly chosen station to verify that
        # prev_obs_air_temp logic in wdb0.get_air_temp_obs works.
        si = random.randrange(wdb_prev_tair['num_stations'])
        # if wdb_prev_tair['num_stations'] > 11496:
        #     si = 11496
        # logger.debug('Not passing wdb_prev_tair to wdb0.get_air_temp_obs.')
        random_tair_sample = \
            wdb0.get_air_temp_obs(obs_datetime -
                                  dt.timedelta(hours=num_hrs_prev_tair),
                                  obs_datetime,
                                  verbose=args.verbose,
                                  station_obj_id=\
                                  wdb_prev_tair['station_obj_id'][si])
        if not (wdb_prev_tair['values_deg_c'][si,:] ==
                random_tair_sample['values_deg_c'][0,:]).all():
            logger.error('Air temperature time series mismatch!')
            msg = '{}({})'.format(wdb_prev_tair['station_id'][si],
                                  wdb_prev_tair['station_obj_id'][si]) + \
                  ' {} - {}'.format(wdb_prev_tair['obs_datetime'][0].
                                    strftime('%Y-%m-%d %H'),
                                    wdb_prev_tair['obs_datetime'][-1].
                                    strftime('%Y-%m-%d %H'))
            logger.error('Random sample (index {}): {}'.format(si,msg))
            logger.error(wdb_prev_tair['values_deg_c'][si,:])
            msg = '{}({})'.format(random_tair_sample['station_id'][0],
                                  random_tair_sample['station_obj_id'][0]) + \
                  ' {} - {}'.format(random_tair_sample['obs_datetime'][0].
                                    strftime('%Y-%m-%d %H'),
                                    random_tair_sample['obs_datetime'][-1].
                                    strftime('%Y-%m-%d %H'))
            logger.error('Reference: {}'.format(msg))
            logger.error(random_tair_sample['values_deg_c'][0,:])
            sys.exit()
        
        current_datetime = dt.datetime.utcnow()
        if wdb_prev_tair_fetch_datetimes is None:
            wdb_prev_tair_fetch_datetimes = \
                np.array([current_datetime] * (num_hrs_prev_tair + 1))
            # wdb_prev_tair_fetch_elapsed = \
            #     (current_datetime - wdb_prev_tair_fetch_datetimes). \
            #     astype('timedelta64[ms]')
        else:
            # Update wdb_prev_tair_fetch_datetimes, shifting values from
            # prior_wdb_prev_tair_datetime to their positions in
            # the new wdb_prev_tair['obs_datetime'] and setting new values to
            # current_datetime.
            new_dt = sorted(list(set(wdb_prev_tair['obs_datetime']) -
                                 set(prior_wdb_prev_tair_datetime)))
            old_dt = sorted(list(set(wdb_prev_tair['obs_datetime']) &
                                 set(prior_wdb_prev_tair_datetime)))
            new_dt_ind = [wdb_prev_tair['obs_datetime'].index(i)
                          for i in new_dt]
            old_dt_ind = [wdb_prev_tair['obs_datetime'].index(i)
                          for i in old_dt]
            old_dt_from_prev_ind = [prior_wdb_prev_tair_datetime.index(i)
                                    for i in old_dt]
            killme = wdb_prev_tair_fetch_datetimes.copy()
            wdb_prev_tair_fetch_datetimes[old_dt_ind] = \
                killme[old_dt_from_prev_ind]
            wdb_prev_tair_fetch_datetimes[new_dt_ind] = current_datetime
            # wdb_prev_tair_fetch_elapsed = \
            #     (current_datetime - wdb_prev_tair_fetch_datetimes). \
            #     astype('timedelta64[ms]')

        # logger.debug(type(wdb_prev_tair_fetch_datetimes))
        # logger.debug(type(wdb_prev_tair_fetch_elapsed))
        # logger.debug([wdb_prev_tair_fetch_elapsed[i].total_seconds()
        #               for i in range(len(wdb_prev_tair_fetch_elapsed))])
        # logger.debug([td.total_seconds()
        #               for td in wdb_prev_tair_fetch_elapsed])
        # logger.debug([td.astype(int) / 1000 \
        #               for td in wdb_prev_tair_fetch_elapsed])

        logger.debug('Found {} '.
                     format(wdb_prev_tair['values_deg_c'].count()) +
                     'preceding air temperature reports ' +
                     'from {} stations.'.
                     format(wdb_prev_tair['num_stations']))
        logger.debug('Query ran in {} seconds.'.
                     format(elapsed_time.total_seconds()))

        # Extract previous air temperature values and station object
        # identifiers, for convenience (shorter variable names).
        wdb_prev_tair_val = wdb_prev_tair['values_deg_c']
        wdb_prev_tair_obj_id = wdb_prev_tair['station_obj_id']

        # Find neighboring indices from wdb_prev_tair for each snow depth
        # observation in wdb_snwd.
        neighborhood_radius_km = 75.0
        min_tair_neighbors = 3
        max_tair_neighbors = 7
        nhood_ind, nhood_dist_km = \
            find_nearest_neighbors(wdb_snwd['station_lat'],
                                   wdb_snwd['station_lon'],
                                   wdb_prev_tair['station_lat'],
                                   wdb_prev_tair['station_lon'],
                                   neighborhood_radius_km,
                                   min_tair_neighbors,
                                   max_tair_neighbors,
                                   verbose=args.verbose)

        # Initialize counters for the current time.
        # num_stations_added_this_time = 0
        num_flagged_sd_wr_this_time = 0
        num_flagged_sd_change_wr_this_time = 0
        num_flagged_sd_streak_this_time = 0
        num_flagged_sd_gap_this_time = 0
        num_flagged_sd_at_cons_this_time = 0
        num_flagged_sd_sf_cons_this_time = 0
        num_flagged_sd_pr_cons_this_time = 0
        num_flagged_sd_at_spatial_cons_this_time = 0

        # Rename wdb_snwd values for convenience.
        wdb_snwd_obj_id = wdb_snwd['station_obj_id']
        wdb_snwd_station_id = wdb_snwd['station_id']
        wdb_snwd_val_cm = wdb_snwd['values_cm'][:,0]

        logger.debug('Performing snow depth QC for {}'.format(obs_datetime))

        ####################################################
        # Loop over all reports for the current date/time. #
        ####################################################

        for wdb_snwd_si in range(0, wdb_snwd['num_stations']):

            # Extract values of wdb_snwd values for this station.
            site_snwd_obj_id = wdb_snwd_obj_id[wdb_snwd_si]
            site_snwd_station_id = wdb_snwd_station_id[wdb_snwd_si]
            site_snwd_val_cm = wdb_snwd_val_cm[wdb_snwd_si]

            if args.check_climatology:
                site_snwd_clim_med_mm = wdb_snwd_clim_med_mm[wdb_snwd_si]
                site_snwd_clim_max_mm = wdb_snwd_clim_max_mm[wdb_snwd_si]
                site_snwd_clim_iqr_mm = wdb_snwd_clim_iqr_mm[wdb_snwd_si]

            # Locate station index in QC database.
            qcdb_si = np.where(qcdb_obj_id_var[:] == site_snwd_obj_id)

            debug_this_station = False
            if debug_station_id is not None and \
               site_snwd_station_id == debug_station_id:
                debug_this_station = True

            if len(qcdb_si[0]) == 0:

                # New station - get its metadata.

                # Open the web database.
                conn_string = "host='wdb0.dmz.nohrsc.noaa.gov' " + \
                              "dbname='web_data'"
                conn = psycopg2.connect(conn_string)
                conn.set_client_encoding("utf-8")
                cursor = conn.cursor()

                # Read metadata for the current station.
                sql_cmd = "SELECT " + wdb_col_list_str + " " + \
                          "FROM point.allstation " + \
                          "WHERE obj_identifier = {};". \
                          format(site_snwd_obj_id)
                cursor.execute(sql_cmd)
                wdb_station_meta = cursor.fetchall()
                cursor.close()
                conn.close()
                if len(wdb_station_meta) != 1:
                    print('ERROR: found {} matches in SQL statement ' +
                          'for station object ID {}; expecting 1.'.
                          format(len(wdb_station_meta), site_snwd_obj_id),
                          file=sys.stderr)
                    qcdb.close()
                    exit(1)
                wdb_station_meta = wdb_station_meta[0]

                # For all station variables append or confirm/update data.
                for ind, qcdb_station_var in enumerate(qcdb_station_vars):
                    allstation_column_name = wdb_col_list[ind]

                    if isinstance(wdb_station_meta[ind], dt.datetime):
                        # Format as "YYYY-MM-DD HH:MM:SS"
                        if not qcdb_station_var.dtype is np.str:
                            print('ERROR: NetCDF variable {}'.
                                  format(qcdb_station_var.name) +
                                  'must be of "str" type.',
                                  file=sys.stderr)
                            qcdb.close()
                            exit(1)
                        wdb_allstation_column_data = \
                            wdb_station_meta[ind].strftime('%Y-%m-%d %H:%M:%S')
                    else:
                        wdb_allstation_column_data = wdb_station_meta[ind]
                        if isinstance(wdb_allstation_column_data, str):
                            wdb_allstation_column_data = \
                                wdb_allstation_column_data.strip()

                    if allstation_column_name == 'station_id':
                        if len(qcdb_si[0]) == 0:
                            # This is a new station.
                            if qcdb_num_stations_start > 0 and args.verbose:
                                logger.info('Adding station "{}".'.
                                            format(wdb_allstation_column_data))

                    if len(qcdb_si[0]) == 0:
                        # Station (si) object ID not in QC database.
                        # Append. THIS ADDS 1 TO THE STATION DIMENSION.
                        # if ind == 0:
                        #     print(qcdb.dimensions['station'].size)
                        qcdb_station_var[qcdb_num_stations] = \
                            wdb_allstation_column_data
                        # if ind == 0:
                        #     print(qcdb.dimensions['station'].size)

                # Metadata was appended above. Now QC data needs to
                # be appended as well.
                qcdb_si = qcdb_num_stations
                qcdb_station_is_new = True
                qcdb_num_stations += 1
                num_stations_added += 1
                num_stations_added_this_time += 1
                if qcdb_num_stations_start > 0 and args.verbose:
                    logger.info('QC database now includes {} stations.'.
                                format(qcdb_num_stations))
                # Initialize qc variables to 0 for this (new) station.
                qcdb_snwd_qc_chkd[qcdb_si,:] = 0
                qcdb_snwd_qc_flag[qcdb_si,:] = 0
                qcdb_swe_qc_chkd[qcdb_si,:] = 0
                qcdb_swe_qc_flag[qcdb_si,:] = 0

                # Add artificial qc data to qcdb_prev_snwd_qc_flag for
                # the new station.
                # ??? Should we also generate/expand ???
                # ??? qcdb_prev_swe_qc_flag here     ???
                #     Pretty sure we do not need to.
                new_row = np.ma.masked_array(np.array([[0] *
                                                       num_hrs_prev_snwd]))
                if qcdb_prev_snwd_qc_flag.shape[0] == 0:
                    qcdb_prev_snwd_qc_flag = new_row
                else:
                    qcdb_prev_snwd_qc_flag = \
                        np.ma.concatenate([qcdb_prev_snwd_qc_flag, new_row],
                                          axis=0)
                new_row = None

            else:                  

                qcdb_si = qcdb_si[0][0]
                qcdb_station_is_new = False

            ########################################################
            # Locate station index relative to all data needed for #
            # performing QC tests.                                 #
            ########################################################

            # Locate station index in previous snow depth data.
            # TODO: fix this brute force method. Maybe try the index method
            # with a try/except arrangement.
            wdb_prev_snwd_si = []
            for ind, val in enumerate(wdb_prev_snwd_obj_id):
                if val == site_snwd_obj_id:
                    wdb_prev_snwd_si.append(ind)

            if len(wdb_prev_snwd_si) != 0:
                # The object ID for this station was found in the
                # preceding snow depth data.
                if len(wdb_prev_snwd_si) > 1:
                    print('ERROR: multiple matches for station ' +
                          'object ID {} '.format(site_snwd_obj_id) +
                          'in preceding snow depth data.',
                          file=sys.stderr)
                    qcdb.close()
                    exit(1)
                wdb_prev_snwd_si = wdb_prev_snwd_si[0]
            else:
                wdb_prev_snwd_si = None

            # Locate station index in previous air temperature data.
            wdb_prev_tair_si = []
            for ind, val in enumerate(wdb_prev_tair_obj_id):
                if val == site_snwd_obj_id:
                    wdb_prev_tair_si.append(ind)

            if len(wdb_prev_tair_si) != 0:
                # The object ID for this station was found in the
                # previous + current air temperature data.
                if len(wdb_prev_tair_si) > 1:
                    print('ERROR: multiple matches for station ' +
                          'object ID {} '.format(site_snwd_obj_id) +
                          'in previous + current air temperature data.',
                          file=sys.stderr)
                    qcdb.close()
                    exit(1)
                wdb_prev_tair_si = wdb_prev_tair_si[0]
            else:
                wdb_prev_tair_si = None

            # Locate station index in snowfall data.
            wdb_snfl_si = []
            for ind, val in enumerate(wdb_snfl_obj_id):
                if val == site_snwd_obj_id:
                    wdb_snfl_si.append(ind)

            if len(wdb_snfl_si) != 0:
                # The object ID for this station was found in the snowfall
                # data.
                if len(wdb_snfl_si) > 1:
                    print('ERROR: multiple matches for station ' +
                          'object ID {} '.format(site_snwd_obj_id) +
                          'in snowfall data.',
                          file=sys.stderr)
                    qcdb.close()
                    exit(1)
                wdb_snfl_si = wdb_snfl_si[0]
            else:
                wdb_snfl_si = None

            # Locate station index in precipitation data.
            wdb_snwd_prcp_si = []
            for ind, val in enumerate(wdb_snwd_prcp_obj_id):
                if val == site_snwd_obj_id:
                    wdb_snwd_prcp_si.append(ind)

            if len(wdb_snwd_prcp_si) != 0:
                # The object ID for this station was found in the
                # precipitation data.
                if len(wdb_snwd_prcp_si) > 1:
                    print('ERROR: multiple matches for station ' +
                          'object ID {} '.format(site_prcp_obj_id) +
                          'in precipitation data.',
                          file=sys.stderr)
                    qcdb.close()
                    exit(1)
                wdb_snwd_prcp_si = wdb_snwd_prcp_si[0]
            else:
                wdb_snwd_prcp_si = None


            ################################################
            # Perform QC tests on the current observation. #
            ################################################

            # Get QC test names and bits for the QC flag and the "QC checked"
            # flag.
            snwd_qc_test_names = qcdb_snwd_qc_flag. \
                                 getncattr('qc_test_names')
            snwd_qc_test_bits = qcdb_snwd_qc_flag. \
                                getncattr('qc_test_bits')
            snwdc_qc_test_names = qcdb_snwd_qc_chkd. \
                                  getncattr('qc_test_names')
            snwdc_qc_test_bits = qcdb_snwd_qc_chkd. \
                                 getncattr('qc_test_bits')


            #########################################################
            # Perform the snow depth world record exceedance check. #
            #########################################################
            
            # Identify the QC bit for the test.
            qc_test_name = 'world_record_exceedance'
            ind = snwd_qc_test_names.index(qc_test_name)
            qc_bit = snwd_qc_test_bits[ind]
            if snwdc_qc_test_names[ind] != qc_test_name:
                print('ERROR: inconsistent qc_test_names data in ' +
                      'QC database.',
                      file=sys.stderr)
                qcdb.close()
                exit(1)
            if snwdc_qc_test_bits[ind] != snwd_qc_test_bits[ind]:
                print('ERROR: inconsistent qc_test_bits data in ' +
                      'QC database.',
                      file=sys.stderr)
                qcdb.close()
                exit(1)

            # print(qcdb_si,
            #       qcdb_ti,
            #       type(qcdb_snwd_qc_chkd[qcdb_si, qcdb_ti]),
            #       qcdb_snwd_qc_chkd[qcdb_si, qcdb_ti])
            if not qcdb_snwd_qc_chkd[qcdb_si, qcdb_ti] & (1 << qc_bit):

                if qc_durre_snwd_wre(site_snwd_val_cm):
                    # Value has been flagged.
                    logger.debug('Flagging snow depth value {} '.
                                 format(site_snwd_val_cm) +
                                 'at station {} '.
                                 format(site_snwd_station_id) +
                                 '({}) '.format(site_snwd_obj_id) +
                                 '("{}").'.format(qc_test_name))

                    # Turn on the QC bit for this test.
                    qcdb_snwd_qc_flag[qcdb_si, qcdb_ti] = \
                        qcdb_snwd_qc_flag[qcdb_si, qcdb_ti] \
                        | (1 << qc_bit)

                    num_flagged_sd_wr_this_time += 1
                    num_flagged_sd_wr += 1

                # Turn on the QC checked bit for this test, regardless of
                # whether the observation was flagged.
                qcdb_snwd_qc_chkd[qcdb_si, qcdb_ti] = \
                    qcdb_snwd_qc_chkd[qcdb_si, qcdb_ti] \
                    | (1 << qc_bit)

            ############################################################
            # Perform world record exceedance check for change in snow #
            # depth.                                                   #
            ############################################################

            qc_test_name = 'world_record_increase_exceedance'
            if debug_this_station:
                print('***** Debugging snwd "{}" test for value {} at {} ({}).'.
                      format(qc_test_name,
                             site_snwd_val_cm,
                             site_snwd_station_id,
                             site_snwd_obj_id))

            if wdb_prev_snwd_si is not None:

                # Preceding snow depth data is available, making this test
                # possible. 

                # Identify the QC bit for the test.
                ind = snwd_qc_test_names.index(qc_test_name)
                qc_bit = snwd_qc_test_bits[ind]
                if snwdc_qc_test_names[ind] != qc_test_name:
                    print('ERROR: inconsistent qc_test_names data in ' +
                          'QC database.',
                          file=sys.stderr)
                    qcdb.close()
                    exit(1)
                if snwdc_qc_test_bits[ind] != snwd_qc_test_bits[ind]:
                    print('ERROR: inconsistent qc_test_bits data in ' +
                          'QC database.',
                          file=sys.stderr)
                    qcdb.close()
                    exit(1)

                if debug_this_station:
                    print('***** have previous data.')

                if not qcdb_snwd_qc_chkd[qcdb_si, qcdb_ti] & (1 << qc_bit):

                    # Test has not been performed for this observation.

                    # Note that there are three indices for locating this
                    # station:
                    # 1. wdb_snwd_si    = the station index in
                    #                     wdb_snwd_val_cm
                    # 2. qcdb_si        = the station index in the QC database
                    #                     which is covered by these arrays:
                    #                     qcdb_prev_snwd_qc_flag
                    #                     qcdb_snwd_qc_chkd
                    #                     qcdb_snwd_qc_flag
                    # 3. wdb_prev_snwd_si = the station index in
                    #                     prev_sd and wdb_prev_snwd_obj_id

                    prev_snwd_ti = num_hrs_prev_snwd - num_hrs_wre

                    site_prev_snwd_val_cm = \
                        wdb_prev_snwd_val_cm[wdb_prev_snwd_si, prev_snwd_ti:]

                    site_prev_snwd_qc = \
                        qcdb_prev_snwd_qc_flag[qcdb_si, prev_snwd_ti:]

                    flag, ref_ind = \
                        qc_durre_snwd_change_wre(site_snwd_val_cm,
                                                 site_prev_snwd_val_cm,
                                                 site_prev_snwd_qc)

                    if debug_this_station:
                        print('***** values: {} {}'.
                              format(site_prev_snwd_val_cm, site_snwd_val_cm))
                        print('***** flag: {}'.format(flag))

                    if flag:

                        logger.debug('Flagging snow depth change ' + 
                                     '{} '.
                                     format(site_prev_snwd_val_cm[ref_ind]) +
                                     'to {} '.format(site_snwd_val_cm) +
                                     'at station {} '.
                                     format(site_snwd_station_id) +
                                     '({}) '.format(site_snwd_obj_id) +
                                     '("{}").'.format(qc_test_name))

                        # Turn on the QC bit for this test.
                        qcdb_snwd_qc_flag[qcdb_si, qcdb_ti] = \
                            qcdb_snwd_qc_flag[qcdb_si, qcdb_ti] | (1 << qc_bit)

                        if flag_sd_change_wre_low_value:

                            # Flag previous (low) value as well if that time
                            # fits into the database.
                            ref_ind_db = qcdb_ti - num_hrs_wre + ref_ind
                            if ref_ind_db >= 0:
                                ref_datetime = \
                                    num2date(qcdb_var_time[0] + ref_ind_db,
                                             units=qcdb_var_time_units,
                                             only_use_cftime_datetimes=False)
                                logger.debug('Also flagging low-valued ' +
                                             'observation {} at {}.'.
                                             format(site_prev_snwd_val_cm[ref_ind],
                                                    ref_datetime))
                                # First make sure the previous value is
                                # not flagged.
                                if qcdb_snwd_qc_flag[qcdb_si, ref_ind_db] & \
                                   (1 << qc_bit) != 0:
                                    logger.error('(PROGRAMMING) ' +
                                                 'reference value was ' +
                                                 'previously flagged and ' +
                                                 'should not have been used.')
                                    qcdb.close()
                                    sys.exit(1)
                                # Flag the previous value.
                                qcdb_snwd_qc_flag[qcdb_si, ref_ind_db] = \
                                    qcdb_snwd_qc_flag[qcdb_si,ref_ind_db] | \
                                    (1 << qc_bit)
                                # Identify the previous value as having been
                                # through this check, even though this is
                                # only indirectly the case. Most likely it has
                                # already been tested, and passed, but now is
                                # associated with the later problematic report.
                                qcdb_snwd_qc_chkd[qcdb_si, ref_ind_db] = \
                                    qcdb_snwd_qc_chkd[qcdb_si, ref_ind_db] | \
                                    (1 << qc_bit)

                        # Increment flagged-value counters.
                        num_flagged_sd_change_wr_this_time += 1
                        num_flagged_sd_change_wr += 1

                    if flag is not None:

                        # Turn on the QC checked bit for this test, regardless
                        # of whether the observation was flagged.
                        qcdb_snwd_qc_chkd[qcdb_si, qcdb_ti] = \
                            qcdb_snwd_qc_chkd[qcdb_si, qcdb_ti] | (1 << qc_bit)

                else:

                    if debug_this_station:
                        print('***** snow depth increase WRE test done')

                    # Test has already been performed.
                    flag_value = qcdb_snwd_qc_flag[qcdb_si, qcdb_ti] & \
                                 (1 << qc_bit)
                    if flag_value == 0:
                        flag_str = 'not flagged'
                    else:
                        flag_str = 'flagged'
                    if args.verbose:
                        logger.info('Check "{}" '.format(qc_test_name) +
                                    'already done for site {} ({}) '.
                                    format(site_snwd_station_id,
                                           site_snwd_obj_id) +
                                    'value {} ({})'.
                                    format(site_snwd_val_cm, flag_str))

            ########################################
            # Perform streak check for snow depth. #
            ########################################

            qc_test_name = 'streak'
            if wdb_prev_snwd_si is not None:

                # Preceding snow depth data is available, making this test
                # possible.

                # Identify the QC bit for the test.
                ind = snwd_qc_test_names.index(qc_test_name)
                qc_bit = snwd_qc_test_bits[ind]
                if snwdc_qc_test_names[ind] != qc_test_name:
                    print('ERROR: inconsistent qc_test_names data in ' +
                          'QC database.',
                          file=sys.stderr)
                    qcdb.close()
                    exit(1)
                if snwdc_qc_test_bits[ind] != snwd_qc_test_bits[ind]:
                    print('ERROR: inconsistent qc_test_bits data in ' +
                          'QC database.',
                          file=sys.stderr)
                    qcdb.close()
                    exit(1)

                if not qcdb_snwd_qc_chkd[qcdb_si, qcdb_ti] & (1 << qc_bit):

                    # Test has not been performed for this observation.

                    prev_snwd_ti = num_hrs_prev_snwd - num_hrs_streak

                    site_prev_snwd_val_cm = \
                        wdb_prev_snwd_val_cm[wdb_prev_snwd_si, prev_snwd_ti:]

                    site_prev_snwd_qc = \
                        qcdb_prev_snwd_qc_flag[qcdb_si, prev_snwd_ti:]

                    flag = qc_durre_snwd_streak(site_snwd_val_cm,
                                                site_prev_snwd_val_cm,
                                                site_prev_snwd_qc)

                    if flag:

                        logger.debug('Flagging snow depth data ' +
                                     'for "{}" check '.
                                     format(qc_test_name) +
                                     'at station {} '.
                                     format(site_snwd_station_id) +
                                     '({}).'.format(site_snwd_obj_id))

                        # Turn on the QC bit for this test.
                        qcdb_snwd_qc_flag[qcdb_si, qcdb_ti] = \
                            qcdb_snwd_qc_flag[qcdb_si, qcdb_ti] | (1 << qc_bit)

                        # TODO:
                        # Flag previous values as well?

                        num_flagged_sd_streak_this_time += 1
                        num_flagged_sd_streak += 1

                    if flag is not None:

                        # Turn on the QC checked bit for this test, regardless
                        # of whether the observation was flagged.
                        qcdb_snwd_qc_chkd[qcdb_si, qcdb_ti] = \
                            qcdb_snwd_qc_chkd[qcdb_si, qcdb_ti] | (1 << qc_bit)

                else:

                    # Test has already been performed.
                    flag_value = qcdb_snwd_qc_flag[qcdb_si, qcdb_ti] & \
                                 (1 << qc_bit)
                    if flag_value == 0:
                        flag_str = 'not flagged'
                    else:
                        flag_str = 'flagged'
                    if args.verbose:
                        logger.info('Check "{}" '.format(qc_test_name) +
                                    'already done for site {} ({}) '.
                                    format(site_snwd_station_id,
                                           site_snwd_obj_id) +
                                    'value {} ({})'.
                                    format(site_snwd_val_cm, flag_str))

            #####################################
            # Perform gap check for snow depth. #
            #####################################

            qc_test_name = 'gap'
            if debug_this_station:
                print('***** Debugging snwd "{}" test for value {} at {} ({}).'.
                      format(qc_test_name,
                             site_snwd_val_cm,
                             site_snwd_station_id,
                             site_snwd_obj_id))
            if wdb_prev_snwd_si is not None:

                # Preceding snow depth data is available, making this test
                # possible.

                # Identify the QC bit for the test.
                ind = snwd_qc_test_names.index(qc_test_name)
                qc_bit = snwd_qc_test_bits[ind]
                if snwdc_qc_test_names[ind] != qc_test_name:
                    print('ERROR: inconsistent qc_test_names data in ' +
                          'QC database.',
                          file=sys.stderr)
                    qcdb.close()
                    exit(1)
                if snwdc_qc_test_bits[ind] != snwd_qc_test_bits[ind]:
                    print('ERROR: inconsistent qc_test_bits data in ' +
                          'QC database.',
                          file=sys.stderr)
                    qcdb.close()
                    exit(1)

                if not qcdb_snwd_qc_chkd[qcdb_si, qcdb_ti] & (1 << qc_bit):

                    # Test has not been performed for this observation.

                    prev_snwd_ti = num_hrs_prev_snwd - num_hrs_gap

                    site_prev_snwd_val_cm = \
                        wdb_prev_snwd_val_cm[wdb_prev_snwd_si, prev_snwd_ti:]

                    site_prev_snwd_qc = \
                        qcdb_prev_snwd_qc_flag[qcdb_si, prev_snwd_ti:]

                    station_time_series = \
                        np.ma.append(site_prev_snwd_val_cm, site_snwd_val_cm)

                    if args.check_climatology:

                        snwd_ref_ceiling_cm = (site_snwd_clim_max_mm + \
                                               site_snwd_clim_iqr_mm) * 0.1

                        snwd_ref_default_cm = site_snwd_clim_med_mm * 0.1

                        ts_flag_ind, ref_obs = \
                            qc_durre_snwd_gap(site_snwd_val_cm,
                                              site_prev_snwd_val_cm,
                                              site_prev_snwd_qc,
                                              ref_ceiling_cm=
                                              snwd_ref_ceiling_cm,
                                              ref_default_cm=
                                              snwd_ref_default_cm,
                                              verbose=args.verbose)

                    else:

                        ts_flag_ind, ref_obs = \
                            qc_durre_snwd_gap(site_snwd_val_cm,
                                              site_prev_snwd_val_cm,
                                              site_prev_snwd_qc,
                                              verbose=args.verbose)

                    if ts_flag_ind is None:
                        print('ERROR: snow depth gap check failed ' +
                              'for station {} '.format(site_snwd_station_id) +
                              '({}).'.format(site_snwd_obj_id))
                        qcdb.close()
                        sys.exit(1)

                    for ind, ts_ind in enumerate(ts_flag_ind):

                        # Identify time index of observation in database.
                        ts_ind_db = qcdb_ti - num_hrs_gap + ts_ind

                        if debug_this_station:
                            flagged_obs_datetime = \
                                num2date(qcdb_var_time[0] + ts_ind_db,
                                         units=qcdb_var_time_units,
                                         only_use_cftime_datetimes=False)
                            logger.debug('snow depth gap test flags {} '.
                                         format(station_time_series[ts_ind]) +
                                         'at {}, '.
                                         format(flagged_obs_datetime) +
                                         'reference {}'.format(ref_obs[ind]))
                            if ts_ind_db < 0:
                                logger.debug('(does not fit in QC database)')
                            else:
                                logger.debug('(fits in QC database)')

                        if ts_ind_db >= 0:

                            # Observation fits in time frame of database.
                            flagged_obs_datetime = \
                                num2date(qcdb_var_time[0] + ts_ind_db,
                                         units=qcdb_var_time_units,
                                         only_use_cftime_datetimes=False)

                            logger.debug('Flagging snow depth data ' +
                                         'for "gap" check ' +
                                         'at station {} '.
                                         format(site_snwd_station_id) +
                                         '({}), '.
                                         format(site_snwd_obj_id) +
                                         'value {}, '.
                                         format(station_time_series[ts_ind]) +
                                         'time {}.'.
                                         format(flagged_obs_datetime))

                            # Make sure the value is not flagged.
                            if qcdb_snwd_qc_flag[qcdb_si, ts_ind_db] & \
                               (1 << qc_bit) != 0:
                                print('ERROR: (PROGRAMMING) ' +
                                      'reference value was ' +
                                      'previously flagged and should ' +
                                      'not have been used.',
                                      file=sys.stderr)
                                qcdb.close()
                                sys.exit(1)

                            if debug_this_station and \
                               ts_ind_db != qcdb_ti:
                                print('***** this is a "previous" value:')
                                print('***** qc_chkd = {}'.
                                      format(qcdb_snwd_qc_chkd[qcdb_si,
                                                               ts_ind_db] &
                                             (1 << qc_bit)))
                                print('***** qc_flag = {}'.
                                      format(qcdb_snwd_qc_flag[qcdb_si,
                                                               ts_ind_db] &
                                             (1 << qc_bit)))

                            # Turn on the QC bit for this value.
                            qcdb_snwd_qc_flag[qcdb_si, ts_ind_db] = \
                                qcdb_snwd_qc_flag[qcdb_si,ts_ind_db] | \
                                (1 << qc_bit)

                            # Identify the previous value as having been
                            # through this check, even though this is
                            # only indirectly the case. Most likely it has
                            # already been tested, and passed, but in the
                            # current context it is being flagged.
                            # if debug_this_station:
                            #     print('***** qc_checked future-before: ' +
                            #           '{}, {}'.
                            #           format(ts_ind_db,
                            #                  qcdb_snwd_qc_chkd[qcdb_si,
                            #                                    ts_ind_db] & \
                            #                  (1 << qc_bit)))
                            qcdb_snwd_qc_chkd[qcdb_si, ts_ind_db] = \
                                qcdb_snwd_qc_chkd[qcdb_si, ts_ind_db] \
                                | (1 << qc_bit)
                            # if debug_this_station:
                            #     print('***** qc_checked future-after: {}'.
                            #           format(qcdb_snwd_qc_chkd[qcdb_si,
                            #                                    ts_ind_db] & \
                            #                  (1 << qc_bit)))

                            num_flagged_sd_gap_this_time += 1
                            num_flagged_sd_gap += 1

                    # Turn on the QC checked bit for this test, regardless of
                    # whether the observation was flagged.
                    # if debug_this_station:
                    #     print('***** qc_checked before: {}, {}'.
                    #           format(qcdb_ti,
                    #                  qcdb_snwd_qc_chkd[qcdb_si, qcdb_ti] & \
                    #                  (1 << qc_bit)))
                    qcdb_snwd_qc_chkd[qcdb_si, qcdb_ti] = \
                        qcdb_snwd_qc_chkd[qcdb_si, qcdb_ti] \
                        | (1 << qc_bit)
                    # if debug_this_station:
                    #     print('***** qc_checked after: {}'.
                    #           format(qcdb_snwd_qc_chkd[qcdb_si, qcdb_ti] & \
                    #                  (1 << qc_bit)))

                else:

                    # Test has already been performed.
                    flag_value = qcdb_snwd_qc_flag[qcdb_si, qcdb_ti] \
                             & (1 << qc_bit)
                    if flag_value == 0:
                        flag_str = 'not flagged'
                    else:
                        flag_str = 'flagged'
                    if args.verbose:
                        logger.info('Check "{}" '.format(qc_test_name) +
                                    'already done for site {} ({}) '.
                                    format(site_snwd_station_id,
                                           site_snwd_obj_id) +
                                    'value {} ({})'.
                                    format(site_snwd_val_cm, flag_str))

            ###############################################
            # Perform snow-temperature consistency check. #
            ###############################################

            qc_test_name = 'temperature_consistency'
            if (wdb_prev_snwd_si is not None) and \
               (wdb_prev_tair_si is not None):

                # Preceding snow depth data and air temperature data is
                # available for this station, making this test possible.

                # Identify the QC bit for the test.
                ind = snwd_qc_test_names.index(qc_test_name)
                qc_bit = snwd_qc_test_bits[ind]
                if snwdc_qc_test_names[ind] != qc_test_name:
                    print('ERROR: inconsistent qc_test_names data in ' +
                          'QC database.',
                          file=sys.stderr)
                    qcdb.close()
                    exit(1)
                if snwdc_qc_test_bits[ind] != snwd_qc_test_bits[ind]:
                    print('ERROR: inconsistent qc_test_bits data in ' +
                          'QC database.',
                          file=sys.stderr)
                    qcdb.close()
                    exit(1)

                if not qcdb_snwd_qc_chkd[qcdb_si, qcdb_ti] & (1 << qc_bit):

                    # Test has not been performed for this observation.

                    # Note that there are four indices for locating this
                    # station:
                    # 1. wdb_snwd_si    = the station index in
                    #                     wdb_snwd
                    # 2. qcdb_si        = the station index in the QC database
                    #                     which includes
                    #                     qcdb_snwd_qc_chkd *
                    #                     qcdb_snwd_qc_flag *
                    # 3. wdb_prev_snwd_si = the station index in
                    #                     prev_sd and wdb_prev_snwd_obj_id
                    # 4. wdb_prev_tair_si = the station index in
                    #                       wdb_prev_tair

                    prev_snwd_ti = num_hrs_prev_snwd - num_hrs_prev_tair

                    site_prev_snwd_val_cm = \
                        wdb_prev_snwd_val_cm[wdb_prev_snwd_si, prev_snwd_ti:]

                    # Programming check.
                    if len(site_prev_snwd_val_cm) != num_hrs_prev_tair:
                        print('ERROR: (programming) previous snow depth ' +
                              'time index is incorrect.',
                              file=sys.stderr)
                        qcdb.close()
                        exit(1)

                    # Programming check on station indices in different
                    # variables.
                    if (qcdb_obj_id_var[qcdb_si] != site_snwd_obj_id or
                        wdb_prev_snwd_obj_id[wdb_prev_snwd_si] != \
                        site_snwd_obj_id):
                        print('ERROR: (programming) object ID mismatch ' +
                              'in "{}" check.'.format(qc_test_name),
                              file=sys.stderr)
                        qcdb.close()
                        exit(1)

                    site_prev_snwd_qc = \
                        qcdb_prev_snwd_qc_flag[qcdb_si, prev_snwd_ti:]

                    # Programming check.
                    if len(site_prev_snwd_qc) != num_hrs_prev_tair:
                        print('ERROR: (programming) previous snow depth ' +
                              'QC time index is incorrect.',
                              file=sys.stderr)
                        qcdb.close()
                        exit(1)

                    site_prev_tair_val_deg_c = \
                        wdb_prev_tair_val[wdb_prev_tair_si,:]

                    # Programming check.
                    if len(site_prev_tair_val_deg_c) != num_hrs_prev_tair + 1:
                        print('ERROR: (programming) previous air ' +
                              'temperature time index is incorrect.',
                              file=sys.stderr)
                        print(len(site_prev_tair_val_deg_c))
                        print(num_hrs_prev_tair + 1)
                        qcdb.close()
                        exit(1)

                    flag, ref_ind = \
                        qc_durre_snwd_tair(site_snwd_val_cm,
                                           site_prev_snwd_val_cm,
                                           site_prev_snwd_qc,
                                           site_prev_tair_val_deg_c)

                    if flag:

                        logger.debug('Flagging snow depth change ' +
                                     '{} '.
                                     format(site_prev_snwd_val_cm[ref_ind]) +
                                     'to {} '.format(site_snwd_val_cm) +
                                     'at station {} '.
                                     format(site_snwd_station_id) +
                                     '({}) '.format(site_snwd_obj_id) +
                                     '("{}").'.format(qc_test_name))

                        # Turn on the QC bit for this test.
                        qcdb_snwd_qc_flag[qcdb_si, qcdb_ti] = \
                            qcdb_snwd_qc_flag[qcdb_si, qcdb_ti] | (1 << qc_bit)

                        if flag_sd_change_tair_low_value:

                            # Flag previous value as well if that time fits
                            # into the database.
                            ref_ind_db = qcdb_ti - num_hrs_prev_tair + ref_ind
                            if ref_ind_db >= 0:
                                ref_datetime = \
                                    num2date(qcdb_var_time[0] + ref_ind_db,
                                             units=qcdb_var_time_units,
                                             only_use_cftime_datetimes=False)
                                logger.debug('Also flagging ' +
                                             'observation {} at {}.'.
                                             format(site_prev_snwd_val_cm[ref_ind],
                                                    ref_datetime))
                                # First make sure the previous value is not
                                # flagged.
                                if qcdb_snwd_qc_flag[qcdb_si, ref_ind_db] & \
                                   (1 << qc_bit) != 0:
                                    print('ERROR: (PROGRAMMING) ' +
                                          'reference value was ' +
                                          'previously flagged and should ' +
                                          'not have been used.',
                                          file=sys.stderr)
                                    qcdb.close()
                                    sys.exit(1)
                                # Flag the previous value.
                                qcdb_snwd_qc_flag[qcdb_si, ref_ind_db] = \
                                    qcdb_snwd_qc_flag[qcdb_si,ref_ind_db] | \
                                    (1 << qc_bit)
                                # Identify the previous value as having been
                                # through this check, even though this is
                                # only indirectly the case. Most likely it has
                                # already been tested, and passed, but now is
                                # associated with the later problematic report.
                                qcdb_snwd_qc_chkd[qcdb_si, ref_ind_db] = \
                                    qcdb_snwd_qc_chkd[qcdb_si, ref_ind_db] \
                                    | (1 << qc_bit)

                        # Increment flagged-value counters.
                        num_flagged_sd_at_cons_this_time += 1
                        num_flagged_sd_at_cons += 1

                    if flag is not None:

                        # Turn on the QC checked bit for this test, regardless
                        # of whether the observation was flagged.
                        qcdb_snwd_qc_chkd[qcdb_si, qcdb_ti] = \
                            qcdb_snwd_qc_chkd[qcdb_si, qcdb_ti] | (1 << qc_bit)

                else:

                    # Test has already been performed.
                    flag_value = qcdb_snwd_qc_flag[qcdb_si, qcdb_ti] & \
                                 (1 << qc_bit)
                    if flag_value == 0:
                        flag_str = 'not flagged'
                    else:
                        flag_str = 'flagged'
                    if args.verbose:
                        logger.info('Check "{}" '.format(qc_test_name) +
                                    'already done for site {} ({}) '.
                                    format(site_snwd_station_id,
                                           site_snwd_obj_id) +
                                    'value {} ({})'.
                                    format(site_snwd_val_cm, flag_str))

            ##################################################
            # Perform snowfall-snow depth consistency check. #
            ##################################################

            qc_test_name = 'snowfall_consistency'

            if wdb_snfl_si is not None and \
               wdb_prev_snwd_si is not None:

                # Previous snow depth data is available, making this test
                # possible.

                # Identify the QC bit for the test.
                ind = snwd_qc_test_names.index(qc_test_name)
                qc_bit = snwd_qc_test_bits[ind]
                if snwdc_qc_test_names[ind] != qc_test_name:
                    print('ERROR: inconsistent qc_test_names data in ' +
                          'QC database.',
                          file=sys.stderr)
                    qcdb.close()
                    exit(1)
                if snwdc_qc_test_bits[ind] != snwd_qc_test_bits[ind]:
                    print('ERROR: inconsistent qc_test_bits data in ' +
                          'QC database.',
                          file=sys.stderr)
                    qcdb.close()
                    exit(1)

                if not qcdb_snwd_qc_chkd[qcdb_si, qcdb_ti] & (1 << qc_bit):

                    # Test has not been performed for this observation.

                    # Indices for locating this station:
                    # 1. wdb_snwd_si    = the station index in
                    #                     wdb_snwd_val_cm
                    # 2. qcdb_si        = the station index in the QC database
                    #                     which is covered by these arrays:
                    #                     qcdb_prev_snwd_qc_flag
                    #                     qcdb_snwd_qc_chkd
                    #                     qcdb_snwd_qc_flag
                    # 3. wdb_prev_snwd_si = the station index in
                    #                     prev_sd and wdb_prev_snwd_obj_id
                    # 4. wdb_snfl_si    = the station index in snfl and
                    #                     snfl_obj_id

                    prev_snwd_ti = num_hrs_prev_snwd - num_hrs_snowfall

                    site_prev_snwd_val_cm = \
                        wdb_prev_snwd_val_cm[wdb_prev_snwd_si, prev_snwd_ti:]

                    site_prev_snwd_qc = \
                        qcdb_prev_snwd_qc_flag[qcdb_si, prev_snwd_ti:]

                    site_snfl_val_cm = wdb_snfl_val_cm[wdb_snfl_si]

                    flag, ref_ind = \
                        qc_durre_snwd_snfl(site_snwd_val_cm,
                                           site_prev_snwd_val_cm,
                                           site_prev_snwd_qc,
                                           site_snfl_val_cm)

                    if flag:
 
                        # print('***** station: {} ({})'.
                        #       format(site_snwd_station_id, site_snwd_obj_id))
                        # print('***** prev snwd: {}'.format(site_prev_snwd_val_cm))
                        # print('***** snwd: {}'.format(site_snwd_val_cm))
                        # print('***** snfl: {}'.format(site_snfl_val_cm))
                        # print('***** diff: {}'.format(site_snwd_val_cm - site_prev_snwd_val_cm[ref_ind]))
                        # print('***** inconsistency: {}'.
                        #       format(site_snwd_val_cm -
                        #              site_prev_snwd_val_cm[ref_ind] -
                        #              site_snfl_val_cm))
                        # print('***** flag: {} {}'.format(flag, ref_ind))
                        # xxx = input()

                        logger.debug('Flagging snow depth change ' +
                                     '{} '.
                                     format(site_prev_snwd_val_cm[ref_ind]) +
                                     'to {} '.format(site_snwd_val_cm) +
                                     'at station {} '.
                                     format(site_snwd_station_id) +
                                     '({}) '.format(site_snwd_obj_id) +
                                     '("{}").'.format(qc_test_name))

                        # Turn on the QC bit for this test.
                        qcdb_snwd_qc_flag[qcdb_si, qcdb_ti] = \
                            qcdb_snwd_qc_flag[qcdb_si, qcdb_ti] | (1 << qc_bit)

                        if flag_sd_change_snfl_low_value:

                            # Flag previous value as well if that time fits
                            # into the database.
                            ref_ind_db = qcdb_ti - num_hrs_prev_tair + ref_ind
                            if ref_ind_db >= 0:
                                ref_datetime = \
                                    num2date(qcdb_var_time[0] + ref_ind_db,
                                             units=qcdb_var_time_units,
                                             only_use_cftime_datetimes=False)
                                logger.debug('Also flagging ' +
                                             'observation {} at {}.'.
                                             format(site_prev_snwd_val_cm[ref_ind],
                                                    ref_datetime))
                                # First make sure the previous value is not
                                # flagged.
                                if qcdb_snwd_qc_flag[qcdb_si, ref_ind_db] & \
                                   (1 << qc_bit) != 0:
                                    print('ERROR: (PROGRAMMING) ' +
                                          'reference value was ' +
                                          'previously flagged and should ' +
                                          'not have been used.',
                                          file=sys.stderr)
                                    qcdb.close()
                                    sys.exit(1)
                                # Flag the previous value.
                                qcdb_snwd_qc_flag[qcdb_si, ref_ind_db] = \
                                    qcdb_snwd_qc_flag[qcdb_si,ref_ind_db] | \
                                    (1 << qc_bit)
                                # Identify the previous value as having been
                                # through this check, even though this is
                                # only indirectly the case. Most likely it has
                                # already been tested, and passed, but now is
                                # associated with the later problematic report.
                                qcdb_snwd_qc_chkd[qcdb_si, ref_ind_db] = \
                                    qcdb_snwd_qc_chkd[qcdb_si, ref_ind_db] \
                                    | (1 << qc_bit)

                        num_flagged_sd_sf_cons_this_time += 1
                        num_flagged_sd_sf_cons += 1

                    if flag is not None:

                        # Turn on the QC checked bit for this test, regardless
                        # of whether the observation was flagged.
                        qcdb_snwd_qc_chkd[qcdb_si, qcdb_ti] = \
                            qcdb_snwd_qc_chkd[qcdb_si, qcdb_ti] | (1 << qc_bit)

                else:

                    # Test has already been performed.
                    flag_value = qcdb_snwd_qc_flag[qcdb_si, qcdb_ti] & \
                                 (1 << qc_bit)
                    if flag_value == 0:
                        flag_str = 'not flagged'
                    else:
                        flag_str = 'flagged'
                    if args.verbose:
                        logger.info('Check "{}" '.format(qc_test_name) +
                                    'already done for site {} ({}) '.
                                    format(site_snwd_station_id,
                                           site_snwd_obj_id) +
                                    'value {} ({})'.
                                    format(site_snwd_val_cm, flag_str))

            #######################################################
            # Perform precipitation-snow depth consistency check. #
            #######################################################

            # This is an adaptation of the test described in Durre (2010)
            # Table 3 (internal and temporal consistency checks) as
            # "SNWD increase with 0 PRCP".

            qc_test_name = 'precip_consistency'

            if wdb_snwd_prcp_si is not None and \
               wdb_prev_snwd_si is not None:

                # Previous snow depth data is available, making this test
                # possible.

                # Identify the QC bit for the test.
                ind = snwd_qc_test_names.index(qc_test_name)
                qc_bit = snwd_qc_test_bits[ind]
                if snwdc_qc_test_names[ind] != qc_test_name:
                    print('ERROR: inconsistent qc_test_names data in ' +
                          'QC database.',
                          file=sys.stderr)
                    qcdb.close()
                    exit(1)
                if snwdc_qc_test_bits[ind] != snwd_qc_test_bits[ind]:
                    print('ERROR: inconsistent qc_test_bits data in ' +
                          'QC database.',
                          file=sys.stderr)
                    qcdb.close()
                    exit(1)

                if not qcdb_snwd_qc_chkd[qcdb_si, qcdb_ti] & (1 << qc_bit):

                    # Test has not been performed for this observation.

                    # Indices for locating this station:
                    # 1. wdb_snwd_si      = the station index in
                    #                       wdb_snwd_val_cm
                    # 2. qcdb_si          = the station index in the QC
                    #                       database which is covered by these
                    #                       arrays:
                    #                       qcdb_prev_snwd_qc_flag
                    #                       qcdb_snwd_qc_chkd
                    #                       qcdb_snwd_qc_flag
                    # 3. wdb_prev_snwd_si = the station index in
                    #                       prev_sd and wdb_prev_snwd_obj_id
                    # 4. wdb_snwd_prcp_si = the station index in
                    #                       wdb_snwd_prcp_val_mm

                    prev_snwd_ti = num_hrs_prev_snwd - num_hrs_prcp

                    site_prev_snwd_val_cm = \
                        wdb_prev_snwd_val_cm[wdb_prev_snwd_si, prev_snwd_ti:]

                    site_prev_snwd_qc = \
                        qcdb_prev_snwd_qc_flag[qcdb_si, prev_snwd_ti:]

                    site_prcp_val_mm = wdb_snwd_prcp_val_mm[wdb_snwd_prcp_si]
                    if not np.isscalar(site_prcp_val_mm):
                        print('---')
                        print(type(wdb_snwd_prcp_val_mm))
                        print(type(site_prcp_val_mm))
                        print(type(wdb_snwd_prcp_si))
                        qcdb.close()
                        sys.exit(1)

                    flag, ref_ind = \
                        qc_durre_snwd_prcp(site_snwd_val_cm,
                                           site_prev_snwd_val_cm,
                                           site_prev_snwd_qc,
                                           site_prcp_val_mm)

                    if flag:

                        logger.debug('Flagging snow depth change ' +
                                     '{} '.
                                     format(site_prev_snwd_val_cm[ref_ind]) +
                                     'to {} '.format(site_snwd_val_cm) +
                                     'at station {} '.
                                     format(site_snwd_station_id) +
                                     '({}) '.format(site_snwd_obj_id) +
                                     '("{}").'.format(qc_test_name))

                        # Turn on the QC bit for this test.
                        qcdb_snwd_qc_flag[qcdb_si, qcdb_ti] = \
                            qcdb_snwd_qc_flag[qcdb_si, qcdb_ti] | (1 << qc_bit)

                        if flag_sd_change_prcp_low_value:

                            # Flag previous value as well if that time fits
                            # into the database.
                            ref_ind_db = qcdb_ti - num_hrs_prev_snwd + ref_ind
                            if ref_ind_db >= 0:
                                ref_datetime = \
                                    num2date(qcdb_var_time[0] + ref_ind_db,
                                             units=qcdb_var_time_units,
                                             only_use_cftime_datetimes=False)
                                logger.debug('Also flagging ' +
                                             'observation {} at {}.'.
                                             format(site_prev_snwd_val_cm[ref_ind],
                                                    ref_datetime))
                                # First make sure the previous value is not
                                # flagged.
                                if qcdb_snwd_qc_flag[qcdb_si, ref_ind_db] & \
                                   (1 << qc_bit) != 0:
                                    print('ERROR: (PROGRAMMING) ' +
                                          'reference value was ' +
                                          'previously flagged and should ' +
                                          'not have been used.',
                                          file=sys.stderr)
                                    qcdb.close()
                                    sys.exit(1)
                                # Flag the previous value.
                                qcdb_snwd_qc_flag[qcdb_si, ref_ind_db] = \
                                    qcdb_snwd_qc_flag[qcdb_si,ref_ind_db] | \
                                    (1 << qc_bit)
                                # Identify the previous value as having been
                                # through this check, even though this is
                                # only indirectly the case. Most likely it has
                                # already been tested, and passed, but now is
                                # associated with the later problematic report.
                                qcdb_snwd_qc_chkd[qcdb_si, ref_ind_db] = \
                                    qcdb_snwd_qc_chkd[qcdb_si, ref_ind_db] \
                                    | (1 << qc_bit)

                        num_flagged_sd_pr_cons_this_time += 1
                        num_flagged_sd_pr_cons += 1

                    if flag is not None:

                        # Turn on the QC checked bit for this test, regardless
                        # of whether the observation was flagged.
                        qcdb_snwd_qc_chkd[qcdb_si, qcdb_ti] = \
                            qcdb_snwd_qc_chkd[qcdb_si, qcdb_ti] | (1 << qc_bit)

                else:

                    # Test has already been performed.
                    flag_value = qcdb_snwd_qc_flag[qcdb_si, qcdb_ti] & \
                                 (1 << qc_bit)
                    if flag_value == 0:
                        flag_str = 'not flagged'
                    else:
                        flag_str = 'flagged'
                    if args.verbose:
                        logger.info('Check "{}" '.format(qc_test_name) +
                                    'already done for site {} ({}) '.
                                    format(site_snwd_station_id,
                                           site_snwd_obj_id) +
                                    'value {} ({})'.
                                    format(site_snwd_val_cm, flag_str))

            #############################################################
            # Perform precipitation-snow depth ratio consistency check. #
            #############################################################

            # This is an adaptation of the test described in Durre (2010)
            # Table 3 (internal and temporal consistency checks) as
            # "SNWD/PRCP ratio".

            qc_test_name = 'precip_ratio'

            if wdb_snwd_prcp_si is not None and \
               wdb_prev_snwd_si is not None:

                # Previous snow depth data is available, making this test
                # possible.

                # Identify the QC bit for the test.
                try:
                    ind = snwd_qc_test_names.index(qc_test_name)
                except ValueError:
                    # Try backward compatible option "depth_precip_ratio".
                    qc_test_name = 'depth_precip_ratio'
                    ind = snwd_qc_test_names.index(qc_test_name)

                qc_bit = snwd_qc_test_bits[ind]
                if snwdc_qc_test_names[ind] != qc_test_name:
                    print('ERROR: inconsistent qc_test_names data in ' +
                          'QC database.',
                          file=sys.stderr)
                    qcdb.close()
                    exit(1)
                if snwdc_qc_test_bits[ind] != snwd_qc_test_bits[ind]:
                    print('ERROR: inconsistent qc_test_bits data in ' +
                          'QC database.',
                          file=sys.stderr)
                    qcdb.close()
                    exit(1)

                if not qcdb_snwd_qc_chkd[qcdb_si, qcdb_ti] & (1 << qc_bit):

                    # Test has not been performed for this observation.

                    # Indices for locating this station:
                    # 1. wdb_snwd_si    = the station index in
                    #                     wdb_snwd_val_cm
                    # 2. qcdb_si        = the station index in the QC database
                    #                     which is covered by these arrays:
                    #                     qcdb_prev_snwd_qc_flag
                    #                     qcdb_snwd_qc_chkd
                    #                     qcdb_snwd_qc_flag
                    # 3. wdb_prev_snwd_si = the station index in
                    #                     prev_sd and wdb_prev_snwd_obj_id
                    # 4. wdb_snwd_prcp_si    = the station index in wdb_snwd_prcp_val_mm

                    prev_snwd_ti = num_hrs_prev_snwd - num_hrs_prcp

                    site_prev_snwd_val_cm = \
                        wdb_prev_snwd_val_cm[wdb_prev_snwd_si, prev_snwd_ti:]

                    site_prev_snwd_qc = \
                        qcdb_prev_snwd_qc_flag[qcdb_si, prev_snwd_ti:]

                    site_prcp_val_mm = wdb_snwd_prcp_val_mm[wdb_snwd_prcp_si]

                    flag, ref_ind = \
                        qc_durre_snwd_prcp_ratio(site_snwd_val_cm,
                                                 site_prev_snwd_val_cm,
                                                 site_prev_snwd_qc,
                                                 site_prcp_val_mm)

                    if flag:
 
                        logger.debug('Flagging snow depth change ' +
                                     '{} '.
                                     format(site_prev_snwd_val_cm[ref_ind]) +
                                     'to {} '.format(site_snwd_val_cm) +
                                     'at station {} '.
                                     format(site_snwd_station_id) +
                                     '({}) '.format(site_snwd_obj_id) +
                                     '("{}").'.format(qc_test_name))

                        # Turn on the QC bit for this test.
                        qcdb_snwd_qc_flag[qcdb_si, qcdb_ti] = \
                            qcdb_snwd_qc_flag[qcdb_si, qcdb_ti] | (1 << qc_bit)

                        if flag_sd_change_prcp_low_value:

                            # Flag previous value as well if that time fits
                            # into the database.
                            ref_ind_db = qcdb_ti - num_hrs_prev_snwd + ref_ind
                            if ref_ind_db >= 0:
                                ref_datetime = \
                                    num2date(qcdb_var_time[0] + ref_ind_db,
                                             units=qcdb_var_time_units,
                                             only_use_cftime_datetimes=False)
                                logger.debug('Also flagging ' +
                                             'observation {} at {}.'.
                                             format(site_prev_snwd_val_cm[ref_ind],
                                                    ref_datetime))
                                # First make sure the previous value is not
                                # flagged.
                                if qcdb_snwd_qc_flag[qcdb_si, ref_ind_db] & \
                                   (1 << qc_bit) != 0:
                                    print('ERROR: (PROGRAMMING) ' +
                                          'reference value was ' +
                                          'previously flagged and should ' +
                                          'not have been used.',
                                          file=sys.stderr)
                                    qcdb.close()
                                    sys.exit(1)
                                # Flag the previous value.
                                qcdb_snwd_qc_flag[qcdb_si, ref_ind_db] = \
                                    qcdb_snwd_qc_flag[qcdb_si,ref_ind_db] | \
                                    (1 << qc_bit)
                                # Identify the previous value as having been
                                # through this check, even though this is
                                # only indirectly the case. Most likely it has
                                # already been tested, and passed, but now is
                                # associated with the later problematic report.
                                qcdb_snwd_qc_chkd[qcdb_si, ref_ind_db] = \
                                    qcdb_snwd_qc_chkd[qcdb_si, ref_ind_db] \
                                    | (1 << qc_bit)

                        num_flagged_sd_pr_cons_this_time += 1
                        num_flagged_sd_pr_cons += 1

                    if flag is not None:

                        # Turn on the QC checked bit for this test, regardless
                        # of whether the observation was flagged.
                        qcdb_snwd_qc_chkd[qcdb_si, qcdb_ti] = \
                            qcdb_snwd_qc_chkd[qcdb_si, qcdb_ti] | (1 << qc_bit)

                else:

                    # Test has already been performed.
                    flag_value = qcdb_snwd_qc_flag[qcdb_si, qcdb_ti] & \
                                 (1 << qc_bit)
                    if flag_value == 0:
                        flag_str = 'not flagged'
                    else:
                        flag_str = 'flagged'
                    if args.verbose:
                        logger.info('Check "{}" '.format(qc_test_name) +
                                    'already done for site {} ({}) '.
                                    format(site_snwd_station_id,
                                           site_snwd_obj_id) +
                                    'value {} ({})'.
                                    format(site_snwd_val_cm, flag_str))

            #######################################################
            # Perform spatial snow-temperature consistency check. #
            #######################################################

            qc_test_name = 'spatial_temperature_consistency'

            num_prev_tair_neighbors = len(nhood_ind[wdb_snwd_si])

            if wdb_prev_snwd_si is not None and \
               num_prev_tair_neighbors >= min_tair_neighbors:

                # Previous snow depth data and neighborhood air temperature
                # data are available.
                # Identify the QC bit for the test.
                ind = snwd_qc_test_names.index(qc_test_name)
                qc_bit = snwd_qc_test_bits[ind]
                if snwdc_qc_test_names[ind] != qc_test_name:
                    print('ERROR: inconsistent qc_test_names data in ' +
                          'QC database.',
                          file=sys.stderr)
                    qcdb.close()
                    exit(1)
                if snwdc_qc_test_bits[ind] != snwd_qc_test_bits[ind]:
                    print('ERROR: inconsistent qc_test_bits data in ' +
                          'QC database.',
                          file=sys.stderr)
                    qcdb.close()
                    exit(1)

                if not qcdb_snwd_qc_chkd[qcdb_si, qcdb_ti] & (1 << qc_bit):

                    # Test has not been performed for this observation.

                    # Indices for locating this station:
                    # 1. wdb_snwd_si    = the station index in
                    #                     wdb_snwd_val_cm
                    # 2. qcdb_si        = the station index in the QC database
                    #                     which is covered by these arrays:
                    #                     qcdb_prev_snwd_qc_flag
                    #                     qcdb_snwd_qc_chkd
                    #                     qcdb_snwd_qc_flag
                    # 3. wdb_prev_snwd_si = the station index in
                    #                       prev_sd and wdb_prev_snwd_obj_id
                    # 4. wdb_prev_tair_si = the station index in
                    #                       wdb_prev_tair
   
                    prev_snwd_ti = num_hrs_prev_snwd - num_hrs_prev_tair

                    site_prev_snwd_val_cm = \
                        wdb_prev_snwd_val_cm[wdb_prev_snwd_si, prev_snwd_ti:]

                    site_prev_snwd_qc = \
                        qcdb_prev_snwd_qc_flag[qcdb_si, prev_snwd_ti:]

                    # Find the minimum neighborhood air temperature from the
                    # current + preceding num_hrs_prev_tair hours.
                    #nhood_tair_min = []
                    nhood_tair_deg_c = np.ma.empty([num_prev_tair_neighbors,
                                                    num_hrs_prev_tair + 1])
                    for nc in range(0, num_prev_tair_neighbors):
                        ind2 = nhood_ind[wdb_snwd_si][nc]
                        nhood_tair_deg_c[nc,:] = wdb_prev_tair_val[ind2,:]

                    flag, ref_ind = \
                        qc_durre_snwd_tair_spatial(site_snwd_val_cm,
                                                   site_prev_snwd_val_cm,
                                                   site_prev_snwd_qc,
                                                   nhood_tair_deg_c)

                    if flag:

                        logger.debug('Flagging snow depth change ' +
                                     '{} '.
                                     format(site_prev_snwd_val_cm[ref_ind]) +
                                     'to {} '.format(site_snwd_val_cm) +
                                     'at station {} '.
                                     format(site_snwd_station_id) +
                                     '({}) '.format(site_snwd_obj_id) +
                                     '("{}").'.format(qc_test_name))

                        # Turn on the QC bit for this test.
                        qcdb_snwd_qc_flag[qcdb_si, qcdb_ti] = \
                            qcdb_snwd_qc_flag[qcdb_si, qcdb_ti] | (1 << qc_bit)

                        if flag_sd_change_tair_low_value:

                            # Flag previous value as well if that time fits
                            # into the database.
                            ref_ind_db = qcdb_ti - num_hrs_prev_tair + ref_ind
                            if ref_ind_db >= 0:
                                ref_datetime = \
                                    num2date(qcdb_var_time[0] + ref_ind_db,
                                             units=qcdb_var_time_units,
                                             only_use_cftime_datetimes=False)
                                logger.debug('Also flagging ' +
                                             'observation {} at {}.'.
                                             format(site_prev_snwd_val_cm[ref_ind],
                                                    ref_datetime))
                                # First make sure the previous value is not
                                # flagged.
                                if qcdb_snwd_qc_flag[qcdb_si, ref_ind_db] & \
                                   (1 << qc_bit) != 0:
                                    print('ERROR: (PROGRAMMING) ' +
                                          'reference value was ' +
                                          'previously flagged and should ' +
                                          'not have been used.',
                                          file=sys.stderr)
                                    qcdb.close()
                                    sys.exit(1)
                                # Flag the previous value.
                                qcdb_snwd_qc_flag[qcdb_si, ref_ind_db] = \
                                    qcdb_snwd_qc_flag[qcdb_si,ref_ind_db] | \
                                    (1 << qc_bit)
                                # Identify the previous value as having been
                                # through this check, even though this is
                                # only indirectly the case. Most likely it has
                                # already been tested, and passed, but now is
                                # associated with the later problematic report.
                                qcdb_snwd_qc_chkd[qcdb_si, ref_ind_db] = \
                                    qcdb_snwd_qc_chkd[qcdb_si, ref_ind_db] \
                                    | (1 << qc_bit)

                        num_flagged_sd_at_spatial_cons_this_time += 1
                        num_flagged_sd_at_spatial_cons += 1

                    if flag is not None:

                        # Turn on the QC checked bit for this test, regardless
                        # of whether the observation was flagged.
                        qcdb_snwd_qc_chkd[qcdb_si, qcdb_ti] = \
                            qcdb_snwd_qc_chkd[qcdb_si, qcdb_ti] | (1 << qc_bit)

                else:

                    # Test has already been performed.
                    flag_value = qcdb_snwd_qc_flag[qcdb_si, qcdb_ti] & \
                                 (1 << qc_bit)
                    if flag_value == 0:
                        flag_str = 'not flagged'
                    else:
                        flag_str = 'flagged'
                    if args.verbose:
                        logger.info('Check "{}" '.format(qc_test_name) +
                                    'already done for site {} ({}) '.
                                    format(site_snwd_station_id,
                                           site_snwd_obj_id) +
                                    'value {} ({})'.
                                    format(site_snwd_val_cm, flag_str))


        #####################
        # SNOW DEPTH QC END #
        #####################


        ########################################
        # SNOW WATER EQUIVALENT (SWE) QC BEGIN #
        ########################################


        # Get snow depth and other observations used for snow depth QC.


        # Get all SWE data for this datetime.
        t1 = dt.datetime.utcnow()
        wdb_swe = wdb0.get_swe_obs(obs_datetime,
                                   obs_datetime,
                                   scratch_dir=args.pkl_dir,
                                   verbose=args.verbose)
        t2 = dt.datetime.utcnow()
        elapsed_time = t2 - t1
        logger.debug('Found {} SWE reports.'.
                     format(wdb_swe['num_stations']))
        logger.debug('Query ran in {} seconds.'.
                     format(elapsed_time.total_seconds()))

        # Previous SWE data is needed for multiple tests.
        # - World record increase exceedance check uses 24 hours.
        # - Streak check uses 15 days.
        # - Gap check uses 15 days.
        # - Precipitation consistency checks use 24 hours.
        num_hrs_prev_swe = max(num_hrs_wre,
                               num_hrs_streak,
                               num_hrs_gap,
                               # num_hrs_prev_tair,
                               # num_hrs_snowfall,
                               num_hrs_prcp)

        if args.check_climatology:

            # Get SNODAS SWE climatology data for the current time.
            wdb_swe_clim_med_mm = \
                snodas_clim.at_loc(swe_clim_dir,
                                   obs_datetime,
                                   wdb_swe['station_lon'],
                                   wdb_swe['station_lat'],
                                   element='swe',
                                   metric='median',
                                   sampling='neighbor')
            wdb_swe_clim_max_mm = \
                snodas_clim.at_loc(swe_clim_dir,
                                   obs_datetime,
                                   wdb_swe['station_lon'],
                                   wdb_swe['station_lat'],
                                   element='swe',
                                   metric='max',
                                   sampling='neighbor')
            wdb_swe_clim_iqr_mm = \
                snodas_clim.at_loc(swe_clim_dir,
                                   obs_datetime,
                                   wdb_swe['station_lon'],
                                   wdb_swe['station_lat'],
                                   element='swe',
                                   metric='iqr',
                                   sampling='neighbor')


        # Get previous num_hrs_prev_swe hours of swe data.
        t1 = dt.datetime.utcnow()
        wdb_prev_swe = \
            wdb0.get_prev_swe_obs(obs_datetime,		
                                  num_hrs_prev_swe,
                                  scratch_dir=args.pkl_dir,
                                  verbose=args.verbose)
        t2 = dt.datetime.utcnow()
        elapsed_time = t2 - t1

        logger.debug('Found {} '.
                     format(wdb_prev_swe['values_mm'].count()) +
                     'preceding swe reports ' +
                     'from {} stations.'.
                     format(wdb_prev_swe['num_stations']))
        logger.debug('Query ran in {} seconds.'.
                     format(elapsed_time.total_seconds()))

        # Extract previous swe values and station object identifiers,
        # for convenience (shorter variable names).
        wdb_prev_swe_val_mm = wdb_prev_swe['values_mm']
        wdb_prev_swe_obj_id = wdb_prev_swe['station_obj_id']

        # Get previous num_hrs_prev_swe hours of swe QC data.
        t1 = obs_datetime - dt.timedelta(hours=num_hrs_prev_swe)
        t1_num = date2num(t1, qcdb_var_time_units)
        t1_ind = int(t1_num - qcdb_var_time[0])
        t2 = obs_datetime - dt.timedelta(hours=1)
        t2_num = date2num(t2, qcdb_var_time_units)
        t2_ind = int(t2_num - qcdb_var_time[0])

        # Remove negative indices.
        left_ind = max(t1_ind, 0)
        right_ind = max(t2_ind + 1, 0)

        qcdb_prev_swe_qc_flag = qcdb_swe_qc_flag[:, left_ind:right_ind]

        # Calculate the number of hours to add at the start of
        # qcdb_prev_swe_qc_flag, for cases where num_hrs_prev_swe extends
        # to times earlier than the time range covered by the database.
        num_pad_hours = min(left_ind - t1_ind, num_hrs_prev_swe)

        # Programming check.
        if qcdb_prev_swe_qc_flag.shape[1] + num_pad_hours != \
           num_hrs_prev_swe:
            print('ERROR: (programming) miscalculation of num_pad_hours.',
                  file=sys.stderr)
            qcdb.close()
            sys.exit(1)

        # If necessary (i.e., if any or all of the previous
        # num_hrs_prev_swe hours fall outside the time domain of the QC
        # database), pad previous swe QC data with zeroes.
        if num_pad_hours > 0 and qcdb_num_stations > 0:
            if args.verbose:
                logger.info('Padding previous swe QC flag ' +
                            'with {} hours.'.
                            format(num_pad_hours))

            new_qc = np.ma.masked_array(np.array([[0] * num_pad_hours] *
                                                 qcdb_num_stations))
            qcdb_prev_swe_qc_flag = \
                np.ma.concatenate([new_qc, qcdb_prev_swe_qc_flag],
                                  axis=1)


        # Get precipitation data associated with SWE observations.
        t1 = dt.datetime.utcnow()
        wdb_swe_prcp = \
            wdb0.get_swe_prcp_obs(obs_datetime,
                                  num_hrs_prcp,
                                  scratch_dir=args.pkl_dir,
                                  verbose=args.verbose)
        t2 = dt.datetime.utcnow()
        elapsed_time = t2 - t1
        logger.debug('Found {} precipitation reports.'.
                     format(wdb_swe_prcp['num_stations']))
        logger.debug('Query ran in {} seconds.'.
                     format(elapsed_time.total_seconds()))

        # Extract precipitation values and station object identifiers, for
        # convenience (shorter variable names).
        wdb_swe_prcp_val_mm = wdb_swe_prcp['values_mm']
        wdb_swe_prcp_obj_id = wdb_swe_prcp['station_obj_id']

        # Initialize counters for the current time.
        # num_stations_added_this_time = 0
        num_flagged_swe_wr_this_time = 0
        num_flagged_swe_change_wr_this_time = 0
        num_flagged_swe_streak_this_time = 0
        num_flagged_swe_gap_this_time = 0
        num_flagged_swe_pr_cons_this_time = 0

        # Rename wdb_swe values for convenience.
        wdb_swe_obj_id = wdb_swe['station_obj_id']
        wdb_swe_station_id = wdb_swe['station_id']
        wdb_swe_val_mm = wdb_swe['values_mm'][:,0]

        logger.debug('Performing SWE QC for {}'.format(obs_datetime))

        ####################################################
        # Loop over all reports for the current date/time. #
        ####################################################

        for wdb_swe_si in range(0, wdb_swe['num_stations']):

            # Extract values of wdb_swe values for this station.
            site_swe_obj_id = wdb_swe_obj_id[wdb_swe_si]
            site_swe_station_id = wdb_swe_station_id[wdb_swe_si]
            site_swe_val_mm = wdb_swe_val_mm[wdb_swe_si]

            if args.check_climatology:
                site_swe_clim_med_mm = wdb_swe_clim_med_mm[wdb_swe_si]
                site_swe_clim_max_mm = wdb_swe_clim_max_mm[wdb_swe_si]
                site_swe_clim_iqr_mm = wdb_swe_clim_iqr_mm[wdb_swe_si]

            # Locate station index in QC database.
            qcdb_si = np.where(qcdb_obj_id_var[:] == site_swe_obj_id)

            debug_this_station = False
            if debug_station_id is not None and \
               site_swe_station_id == debug_station_id:
                debug_this_station = True

            if len(qcdb_si[0]) == 0:

                # New station - get its metadata.

                # Open the web database.
                conn_string = "host='wdb0.dmz.nohrsc.noaa.gov' " + \
                              "dbname='web_data'"
                conn = psycopg2.connect(conn_string)
                conn.set_client_encoding("utf-8")
                cursor = conn.cursor()

                # Read metadata for the current station.
                sql_cmd = "SELECT " + wdb_col_list_str + " " + \
                          "FROM point.allstation " + \
                          "WHERE obj_identifier = {};". \
                          format(site_swe_obj_id)
                cursor.execute(sql_cmd)
                wdb_station_meta = cursor.fetchall()
                cursor.close()
                conn.close()
                if len(wdb_station_meta) != 1:
                    print('ERROR: found {} matches in SQL statement ' +
                          'for station object ID {}; expecting 1.'.
                          format(len(wdb_station_meta), site_swe_obj_id),
                          file=sys.stderr)
                    qcdb.close()
                    exit(1)
                wdb_station_meta = wdb_station_meta[0]

                # For all station variables append or confirm/update data.
                for ind, qcdb_station_var in enumerate(qcdb_station_vars):
                    allstation_column_name = wdb_col_list[ind]

                    if isinstance(wdb_station_meta[ind], dt.datetime):
                        # Format as "YYYY-MM-DD HH:MM:SS"
                        if not qcdb_station_var.dtype is np.str:
                            print('ERROR: NetCDF variable {}'.
                                  format(qcdb_station_var.name) +
                                  'must be of "str" type.',
                                  file=sys.stderr)
                            qcdb.close()
                            exit(1)
                        wdb_allstation_column_data = \
                            wdb_station_meta[ind].strftime('%Y-%m-%d %H:%M:%S')
                    else:
                        wdb_allstation_column_data = wdb_station_meta[ind]
                        if isinstance(wdb_allstation_column_data, str):
                            wdb_allstation_column_data = \
                                wdb_allstation_column_data.strip()

                    if allstation_column_name == 'station_id':
                        if len(qcdb_si[0]) == 0:
                            # This is a new station.
                            if qcdb_num_stations_start > 0:
                                logger.debug('Adding station "{}".'.
                                             format(wdb_allstation_column_data))

                    if len(qcdb_si[0]) == 0:
                        # Station (si) object ID not in QC database.
                        # Append. THIS ADDS 1 TO THE STATION DIMENSION.
                        # if ind == 0:
                        #     print(qcdb.dimensions['station'].size)
                        qcdb_station_var[qcdb_num_stations] = \
                            wdb_allstation_column_data
                        # if ind == 0:
                        #     print(qcdb.dimensions['station'].size)

                # Metadata was appended above. Now QC data needs to
                # be appended as well.
                qcdb_si = qcdb_num_stations
                qcdb_station_is_new = True
                qcdb_num_stations += 1
                num_stations_added += 1
                num_stations_added_this_time += 1
                if qcdb_num_stations_start > 0:
                    logger.debug('QC database now includes {} stations.'.
                                 format(qcdb_num_stations))
                # Initialize qc variables to 0 for this (new) station.
                qcdb_snwd_qc_chkd[qcdb_si,:] = 0
                qcdb_snwd_qc_flag[qcdb_si,:] = 0
                qcdb_swe_qc_chkd[qcdb_si,:] = 0
                qcdb_swe_qc_flag[qcdb_si,:] = 0

                # Add artificial qc data to qcdb_prev_swe_qc_flag for
                # the new station.
                # ??? Should we also generate/expand ???
                # ??? qcdb_prev_snwd_qc_flag here    ???
                #     Pretty sure we do not need to.
                new_row = np.ma.masked_array(np.array([[0] *
                                                       num_hrs_prev_swe]))
                if qcdb_prev_swe_qc_flag.shape[0] == 0:
                    qcdb_prev_swe_qc_flag = new_row
                else:
                    qcdb_prev_swe_qc_flag = \
                        np.ma.concatenate([qcdb_prev_swe_qc_flag, new_row],
                                          axis=0)
                new_row = None

            else:                  

                qcdb_si = qcdb_si[0][0]
                qcdb_station_is_new = False

            ########################################################
            # Locate station index relative to all data needed for #
            # performing QC tests.                                 #
            ########################################################

            # Locate station index in previous swe data.
            # TODO: fix this brute force method. Maybe try the index method
            # with a try/except arrangement.
            wdb_prev_swe_si = []
            for ind, val in enumerate(wdb_prev_swe_obj_id):
                if val == site_swe_obj_id:
                    wdb_prev_swe_si.append(ind)

            if len(wdb_prev_swe_si) != 0:
                # The object ID for this station was found in the
                # preceding swe data.
                if len(wdb_prev_swe_si) > 1:
                    print('ERROR: multiple matches for station ' +
                          'object ID {} '.format(site_swe_obj_id) +
                          'in preceding swe data.',
                          file=sys.stderr)
                    qcdb.close()
                    exit(1)
                wdb_prev_swe_si = wdb_prev_swe_si[0]
            else:
                wdb_prev_swe_si = None


            # Locate station index in precipitation data.
            wdb_swe_prcp_si = []
            for ind, val in enumerate(wdb_swe_prcp_obj_id):
                if val == site_swe_obj_id:
                    wdb_swe_prcp_si.append(ind)

            if len(wdb_swe_prcp_si) != 0:
                # The object ID for this station was found in the
                # precipitation data.
                if len(wdb_swe_prcp_si) > 1:
                    print('ERROR: multiple matches for station ' +
                          'object ID {} '.format(site_prcp_obj_id) +
                          'in precipitation data.',
                          file=sys.stderr)
                    qcdb.close()
                    exit(1)
                wdb_swe_prcp_si = wdb_swe_prcp_si[0]
            else:
                wdb_swe_prcp_si = None


            ####################################################
            # Perform SWE QC tests on the current observation. #
            ####################################################

            # Get QC test names and bits for the QC flag and the "QC checked"
            # flag.
            swe_qc_test_names = qcdb_swe_qc_flag. \
                                getncattr('qc_test_names')
            swe_qc_test_bits = qcdb_swe_qc_flag. \
                               getncattr('qc_test_bits')
            swec_qc_test_names = qcdb_swe_qc_chkd. \
                                 getncattr('qc_test_names')
            swec_qc_test_bits = qcdb_swe_qc_chkd. \
                                getncattr('qc_test_bits')


            ##################################################
            # Perform the SWE world record exceedance check. #
            ##################################################

            # Identify the QC bit for the test.
            qc_test_name = 'world_record_exceedance'
            ind = swe_qc_test_names.index(qc_test_name)
            qc_bit = swe_qc_test_bits[ind]
            if swe_qc_test_names[ind] != qc_test_name:
                print('ERROR: inconsistent qc_test_names data in ' +
                      'QC database.',
                      file=sys.stderr)
                qcdb.close()
                exit(1)
            if swec_qc_test_bits[ind] != swe_qc_test_bits[ind]:
                print('ERROR: inconsistent qc_test_bits data in ' +
                      'QC database.',
                      file=sys.stderr)
                qcdb.close()
                exit(1)

            if not qcdb_swe_qc_chkd[qcdb_si, qcdb_ti] & (1 << qc_bit):

                if qc_durre_swe_wre(site_swe_val_mm):
                    # Value has been flagged.
                    logger.debug('Flagging SWE value {} '.
                                 format(site_swe_val_mm) +
                                 'at station {} '.
                                 format(site_swe_station_id) +
                                 '({}) '.format(site_swe_obj_id) +
                                 '("{}").'.format(qc_test_name))

                    # Turn on the QC bit for this test.
                    qcdb_swe_qc_flag[qcdb_si, qcdb_ti] = \
                        qcdb_swe_qc_flag[qcdb_si, qcdb_ti] \
                        | (1 << qc_bit)

                    num_flagged_swe_wr_this_time += 1
                    num_flagged_swe_wr += 1

                # Turn on the QC checked bit for this test, regardless of
                # whether the observation was flagged.
                qcdb_swe_qc_chkd[qcdb_si, qcdb_ti] = \
                    qcdb_swe_qc_chkd[qcdb_si, qcdb_ti] \
                    | (1 << qc_bit)

            ############################################################
            # Perform world record exceedance check for change in SWE. #
            ############################################################

            qc_test_name = 'world_record_increase_exceedance'
            if debug_this_station:
                print('***** Debugging swe "{}" test for value {} at {} ({}).'.
                      format(qc_test_name,
                             site_swe_val_mm,
                             site_swe_station_id,
                             site_swe_obj_id))

            if wdb_prev_swe_si is not None:

                # Preceding SWE data is available, making this test possible.

                # Identify the QC bit for the test.
                ind = swe_qc_test_names.index(qc_test_name)
                qc_bit = swe_qc_test_bits[ind]
                if swec_qc_test_names[ind] != qc_test_name:
                    print('ERROR: inconsistent qc_test_names data in ' +
                          'QC database.',
                          file=sys.stderr)
                    qcdb.close()
                    exit(1)
                if swec_qc_test_bits[ind] != swe_qc_test_bits[ind]:
                    print('ERROR: inconsistent qc_test_bits data in ' +
                          'QC database.',
                          file=sys.stderr)
                    qcdb.close()
                    exit(1)

                if debug_this_station:
                    print('***** have previous data.')

                if not qcdb_swe_qc_chkd[qcdb_si, qcdb_ti] & (1 << qc_bit):

                    # Test has not been performed for this observation.

                    # Note that there are three indices for locating this
                    # station:
                    # 1. wdb_swe_si    = the station index in
                    #                    wdb_swe_val_mm
                    # 2. qcdb_si        = the station index in the QC database
                    #                     which is covered by these arrays:
                    #                     qcdb_prev_swe_qc_flag
                    #                     qcdb_swe_qc_chkd
                    #                     qcdb_swe_qc_flag
                    # 3. wdb_prev_swe_si = the station index in
                    #                      prev_swe and wdb_prev_swe_obj_id

                    prev_swe_ti = num_hrs_prev_swe - num_hrs_wre

                    site_prev_swe_val_mm = \
                        wdb_prev_swe_val_mm[wdb_prev_swe_si, prev_swe_ti:]

                    site_prev_swe_qc = \
                        qcdb_prev_swe_qc_flag[qcdb_si, prev_swe_ti:]

                    flag, ref_ind = \
                        qc_durre_swe_change_wre(site_swe_val_mm, 	#def 2
                                                 site_prev_swe_val_mm,
                                                 site_prev_swe_qc)

                    if debug_this_station:
                        print('***** values: {} {}'.
                              format(site_prev_swe_val_mm, site_swe_val_mm))
                        print('***** flag: {}'.format(flag))
                        xxx = input()

                    if flag:

                        logger.debug('Flagging SWE change ' + 
                                     '{} '.
                                     format(site_prev_swe_val_mm[ref_ind]) +
                                     'to {} '.format(site_swe_val_mm) +
                                     'at station {} '.
                                     format(site_swe_station_id) +
                                     '({}) '.format(site_swe_obj_id) +
                                     '("{}").'.format(qc_test_name))

                        # Turn on the QC bit for this test.
                        qcdb_swe_qc_flag[qcdb_si, qcdb_ti] = \
                            qcdb_swe_qc_flag[qcdb_si, qcdb_ti] | (1 << qc_bit)

                        if flag_swe_change_wre_low_value:

                            # Flag previous (low) value as well if that time
                            # fits into the database.
                            ref_ind_db = qcdb_ti - num_hrs_wre + ref_ind
                            if ref_ind_db >= 0:
                                ref_datetime = \
                                    num2date(qcdb_var_time[0] + ref_ind_db,
                                             units=qcdb_var_time_units,
                                             only_use_cftime_datetimes=False)
                                logger.debug('Also flagging low-valued ' +
                                             'observation {} at {}.'.
                                             format(site_prev_swe_val_mm[ref_ind],
                                                    ref_datetime))
                                # First make sure the previous value is
                                # not flagged.
                                if qcdb_swe_qc_flag[qcdb_si, ref_ind_db] & \
                                   (1 << qc_bit) != 0:
                                    print('ERROR: (PROGRAMMING) ' +
                                          'reference value was ' +
                                          'previously flagged and ' +
                                          'should not have been used.',
                                          file=sys.stderr)
                                    qcdb.close()
                                    sys.exit(1)
                                # Flag the previous value.
                                qcdb_swe_qc_flag[qcdb_si, ref_ind_db] = \
                                    qcdb_swe_qc_flag[qcdb_si,ref_ind_db] | \
                                    (1 << qc_bit)
                                # Identify the previous value as having been
                                # through this check, even though this is
                                # only indirectly the case. Most likely it has
                                # already been tested, and passed, but now is
                                # associated with the later problematic report.
                                qcdb_swe_qc_chkd[qcdb_si, ref_ind_db] = \
                                    qcdb_swe_qc_chkd[qcdb_si, ref_ind_db] | \
                                    (1 << qc_bit)

                        # Increment flagged-value counters.
                        num_flagged_swe_change_wr_this_time += 1
                        num_flagged_swe_change_wr += 1

                    if flag is not None:

                        # Turn on the QC checked bit for this test, regardless
                        # of whether the observation was flagged.
                        qcdb_swe_qc_chkd[qcdb_si, qcdb_ti] = \
                            qcdb_swe_qc_chkd[qcdb_si, qcdb_ti] | (1 << qc_bit)

                else:

                    if debug_this_station:
                        print('***** SWE increase WRE test done')

                    # Test has already been performed.
                    flag_value = qcdb_swe_qc_flag[qcdb_si, qcdb_ti] & \
                                 (1 << qc_bit)
                    if flag_value == 0:
                        flag_str = 'not flagged'
                    else:
                        flag_str = 'flagged'
                    logger.debug('SWE check "{}" '.format(qc_test_name) +
                                 'already done for site {} ({}) '.
                                 format(site_swe_station_id,
                                        site_swe_obj_id) +
                                 'value {} ({})'.
                                 format(site_swe_val_mm, flag_str))

            #################################
            # Perform streak check for SWE. #
            #################################

            qc_test_name = 'streak'
            if wdb_prev_swe_si is not None:

                # Preceding SWE data is available, making this test possible.

                # Identify the QC bit for the test.
                ind = swe_qc_test_names.index(qc_test_name)
                qc_bit = swe_qc_test_bits[ind]
                if swec_qc_test_names[ind] != qc_test_name:
                    print('ERROR: inconsistent qc_test_names data in ' +
                          'QC database.',
                          file=sys.stderr)
                    qcdb.close()
                    exit(1)
                if swec_qc_test_bits[ind] != swe_qc_test_bits[ind]:
                    print('ERROR: inconsistent qc_test_bits data in ' +
                          'QC database.',
                          file=sys.stderr)
                    qcdb.close()
                    exit(1)

                if not qcdb_swe_qc_chkd[qcdb_si, qcdb_ti] & (1 << qc_bit):

                    # Test has not been performed for this observation.

                    prev_swe_ti = num_hrs_prev_swe - num_hrs_streak

                    site_prev_swe_val_mm = \
                        wdb_prev_swe_val_mm[wdb_prev_swe_si, prev_swe_ti:]

                    site_prev_swe_qc = \
                        qcdb_prev_swe_qc_flag[qcdb_si, prev_swe_ti:]

                    flag = qc_durre_swe_streak(site_swe_val_mm,
                                               site_prev_swe_val_mm,
                                               site_prev_swe_qc)

                    if flag:

                        logger.debug('Flagging SWE observation ' +
                                     'for "{}" check '.
                                     format(qc_test_name) +
                                     'at station {} '.
                                     format(site_swe_station_id) +
                                     '({}), '.format(site_swe_obj_id) +
                                     'value {}.'.
                                     format(site_swe_val_mm))

                        # Turn on the QC bit for this test.
                        qcdb_swe_qc_flag[qcdb_si, qcdb_ti] = \
                            qcdb_swe_qc_flag[qcdb_si, qcdb_ti] | (1 << qc_bit)

                        # TODO:
                        # Flag previous values as well?

                        num_flagged_swe_streak_this_time += 1
                        num_flagged_swe_streak += 1

                    if flag is not None:

                        # Turn on the QC checked bit for this test, regardless
                        # of whether the observation was flagged.
                        qcdb_swe_qc_chkd[qcdb_si, qcdb_ti] = \
                            qcdb_swe_qc_chkd[qcdb_si, qcdb_ti] | (1 << qc_bit)

                else:

                    # Test has already been performed.
                    flag_value = qcdb_swe_qc_flag[qcdb_si, qcdb_ti] & \
                                 (1 << qc_bit)
                    if flag_value == 0:
                        flag_str = 'not flagged'
                    else:
                        flag_str = 'flagged'
                    logger.debug('SWE check "{}" already done '.
                                 format(qc_test_name) +
                                 'for site {} ({}) '.
                                 format(site_swe_station_id,
                                        site_swe_obj_id) +
                                 'value {} ({})'.
                                 format(site_swe_val_mm, flag_str))


            ##############################
            # Perform gap check for SWE. #
            ##############################

            qc_test_name = 'gap'
            if debug_this_station:
                print('***** Debugging SWE "{}" test for value {} at {} ({}).'.
                      format(qc_test_name,
                             site_swe_val_mm,
                             site_swe_station_id,
                             site_swe_obj_id))
            if wdb_prev_swe_si is not None:

                # Preceding SWE data is available, making this test possible.

                # Identify the QC bit for the test.
                ind = swe_qc_test_names.index(qc_test_name)
                qc_bit = swe_qc_test_bits[ind]
                if swec_qc_test_names[ind] != qc_test_name:
                    print('ERROR: inconsistent qc_test_names data in ' +
                          'QC database.',
                          file=sys.stderr)
                    qcdb.close()
                    exit(1)
                if swec_qc_test_bits[ind] != swe_qc_test_bits[ind]:
                    print('ERROR: inconsistent qc_test_bits data in ' +
                          'QC database.',
                          file=sys.stderr)
                    qcdb.close()
                    exit(1)

                if not qcdb_swe_qc_chkd[qcdb_si, qcdb_ti] & (1 << qc_bit):

                    # Test has not been performed for this observation.

                    prev_swe_ti = num_hrs_prev_swe - num_hrs_gap

                    site_prev_swe_val_mm = \
                        wdb_prev_swe_val_mm[wdb_prev_swe_si, prev_swe_ti:]

                    site_prev_swe_qc = \
                        qcdb_prev_swe_qc_flag[qcdb_si, prev_swe_ti:]

                    station_time_series = \
                        np.ma.append(site_prev_swe_val_mm, site_swe_val_mm)

                    if args.check_climatology:

                        swe_ref_ceiling_mm = site_swe_clim_max_mm + \
                                             site_swe_clim_iqr_mm

                        swe_ref_default_mm = site_swe_clim_med_mm

                        ts_flag_ind, ref_obs = \
                            qc_durre_swe_gap(site_swe_val_mm,
                                             site_prev_swe_val_mm,
                                             site_prev_swe_qc,
                                             ref_ceiling_mm=
                                             swe_ref_ceiling_mm,
                                             ref_default_mm=
                                             swe_ref_default_mm,
                                             verbose=args.verbose)

                    else:

                        ts_flag_ind, ref_obs = \
                            qc_durre_swe_gap(site_swe_val_mm,
                                             site_prev_swe_val_mm,
                                             site_prev_swe_qc,
                                             verbose=args.verbose)

                    if ts_flag_ind is None:
                        print('ERROR: SWE gap check failed ' +
                              'for station {} '.format(site_swe_station_id) +
                              '({}).'.format(site_swe_obj_id))
                        qcdb.close()
                        sys.exit(1)

                    for ind, ts_ind in enumerate(ts_flag_ind):

                        # Identify time index of observation in database.
                        ts_ind_db = qcdb_ti - num_hrs_gap + ts_ind

                        if debug_this_station:
                            flagged_obs_datetime = \
                                num2date(qcdb_var_time[0] + ts_ind_db,
                                         units=qcdb_var_time_units,
                                         only_use_cftime_datetimes=False)
                            print('***** SWE gap test flags {} '.
                                  format(station_time_series[ts_ind]) +
                                  'at {}, '.format(flagged_obs_datetime) +
                                  'reference {}'.format(ref_obs[ind]))
                            if ts_ind_db < 0:
                                print('***** (does not fit in QC database)')
                            else:
                                print('***** (fits in QC database)')

                        if ts_ind_db >= 0:

                            # Observation fits in time frame of database.
                            flagged_obs_datetime = \
                                num2date(qcdb_var_time[0] + ts_ind_db,
                                         units=qcdb_var_time_units,
                                         only_use_cftime_datetimes=False)

                            logger.debug('Flagging SWE observation ' +
                                         'for "gap" check ' +
                                         'at station {} '.
                                         format(site_swe_station_id) +
                                         '({}), '.
                                         format(site_swe_obj_id) +
                                         'value {}, '.
                                         format(station_time_series[ts_ind]) +
                                         'time {}.'.
                                         format(flagged_obs_datetime))

                            # Make sure the value is not flagged.
                            if qcdb_swe_qc_flag[qcdb_si, ts_ind_db] & \
                               (1 << qc_bit) != 0:
                                logger.error('(PROGRAMMING) ' +
                                             'reference value was ' +
                                             'previously flagged and should ' +
                                             'not have been used.')
                                qcdb.close()
                                sys.exit(1)

                            if debug_this_station and \
                               ts_ind_db != qcdb_ti:
                                print('***** this is a "previous" value:')
                                print('***** qc_chkd = {}'.
                                      format(qcdb_swe_qc_chkd[qcdb_si,
                                                              ts_ind_db] &
                                             (1 << qc_bit)))
                                print('***** qc_flag = {}'.
                                      format(qcdb_swe_qc_flag[qcdb_si,
                                                              ts_ind_db] &
                                             (1 << qc_bit)))

                            # Turn on the QC bit for this value.
                            qcdb_swe_qc_flag[qcdb_si, ts_ind_db] = \
                                qcdb_swe_qc_flag[qcdb_si,ts_ind_db] | \
                                (1 << qc_bit)
                            
                            # Identify the previous value as having been
                            # through this check, even though this is
                            # only indirectly the case. Most likely it has
                            # already been tested, and passed, but in the
                            # current context it is being flagged.
                            # if debug_this_station:
                            #     print('***** qc_checked future-before: ' +
                            #           '{}, {}'.
                            #           format(ts_ind_db,
                            #                  qcdb_swe_qc_chkd[qcdb_si,
                            #                                    ts_ind_db] & \
                            #                  (1 << qc_bit)))
                            qcdb_swe_qc_chkd[qcdb_si, ts_ind_db] = \
                                qcdb_swe_qc_chkd[qcdb_si, ts_ind_db] \
                                | (1 << qc_bit)
                            # if debug_this_station:
                            #     print('***** qc_checked future-after: {}'.
                            #           format(qcdb_swe_qc_chkd[qcdb_si,
                            #                                    ts_ind_db] & \
                            #                  (1 << qc_bit)))

                            num_flagged_swe_gap_this_time += 1
                            num_flagged_swe_gap += 1

                    # Turn on the QC checked bit for this test, regardless of
                    # whether the observation was flagged.
                    # if debug_this_station:
                    #     print('***** qc_checked before: {}, {}'.
                    #           format(qcdb_ti,
                    #                  qcdb_swe_qc_chkd[qcdb_si, qcdb_ti] & \
                    #                  (1 << qc_bit)))
                    qcdb_swe_qc_chkd[qcdb_si, qcdb_ti] = \
                        qcdb_swe_qc_chkd[qcdb_si, qcdb_ti] \
                        | (1 << qc_bit)
                    # if debug_this_station:
                    #     print('***** qc_checked after: {}'.
                    #           format(qcdb_swe_qc_chkd[qcdb_si, qcdb_ti] & \
                    #                  (1 << qc_bit)))

                else:

                    # Test has already been performed.
                    flag_value = qcdb_swe_qc_flag[qcdb_si, qcdb_ti] \
                             & (1 << qc_bit)
                    if flag_value == 0:
                        flag_str = 'not flagged'
                    else:
                        flag_str = 'flagged'
                    if args.verbose:
                        print('INFO: check "{}" '.format(qc_test_name) +
                              'already done for site {} ({}) '.
                              format(site_swe_station_id,
                                     site_swe_obj_id) +
                              'value {} ({})'.
                              format(site_swe_val_mm, flag_str))

            ################################################
            # Perform precipitation-SWE consistency check. #
            ################################################

            # This is an adaptation of the test described in Durre (2010)
            # Table 3 (internal and temporal consistency checks) as
            # "SNWD increase with 0 PRCP".

            qc_test_name = 'precip_consistency'

            if wdb_swe_prcp_si is not None and \
               wdb_prev_swe_si is not None:

                # Previous swe data is available, making this test
                # possible.

                # Identify the QC bit for the test.
                ind = swe_qc_test_names.index(qc_test_name)
                qc_bit = swe_qc_test_bits[ind]
                if swec_qc_test_names[ind] != qc_test_name:
                    print('ERROR: inconsistent qc_test_names data in ' +
                          'QC database.',
                          file=sys.stderr)
                    qcdb.close()
                    exit(1)
                if swec_qc_test_bits[ind] != swe_qc_test_bits[ind]:
                    print('ERROR: inconsistent qc_test_bits data in ' +
                          'QC database.',
                          file=sys.stderr)
                    qcdb.close()
                    exit(1)

                if not qcdb_swe_qc_chkd[qcdb_si, qcdb_ti] & (1 << qc_bit):

                    # Test has not been performed for this observation.

                    # Indices for locating this station:
                    # 1. wdb_swe_si      = the station index in
                    #                      wdb_swe_val_mm
                    # 2. qcdb_si         = the station index in the QC database
                    #                      which is covered by these arrays:
                    #                      qcdb_prev_swe_qc_flag
                    #                      qcdb_swe_qc_chkd
                    #                      qcdb_swe_qc_flag
                    # 3. wdb_prev_swe_si = the station index in
                    #                      prev_swe and wdb_prev_swe_obj_id
                    # 4. wdb_swe_prcp_si = the station index in wdb_prcp_val_mm

                    prev_swe_ti = num_hrs_prev_swe - num_hrs_prcp

                    site_prev_swe_val_mm = \
                        wdb_prev_swe_val_mm[wdb_prev_swe_si, prev_swe_ti:]

                    site_prev_swe_qc = \
                        qcdb_prev_swe_qc_flag[qcdb_si, prev_swe_ti:]

                    site_prcp_val_mm = wdb_swe_prcp_val_mm[wdb_swe_prcp_si]
                    if not np.isscalar(site_prcp_val_mm):
                        print('---')
                        print(type(wdb_swe_prcp_val_mm))
                        print(type(site_prcp_val_mm))
                        print(type(wdb_swe_prcp_si))
                        qcdb.close()
                        sys.exit(1)

                    flag, ref_ind = \
                        qc_durre_swe_prcp(site_swe_val_mm,
                                          site_prev_swe_val_mm,
                                          site_prev_swe_qc,
                                          site_prcp_val_mm)

                    if flag:

                        logger.debug('Flagging SWE change ' +
                                     '{} '.
                                     format(site_prev_swe_val_mm[ref_ind]) +
                                     'to {} '.format(site_swe_val_mm) +
                                     'at station {} '.
                                     format(site_swe_station_id) +
                                     '({}) '.format(site_swe_obj_id) +
                                     '("{}").'.format(qc_test_name))

                        # Turn on the QC bit for this test.
                        qcdb_swe_qc_flag[qcdb_si, qcdb_ti] = \
                            qcdb_swe_qc_flag[qcdb_si, qcdb_ti] | (1 << qc_bit)

                        if flag_swe_change_prcp_low_value:

                            # Flag previous value as well if that time fits
                            # into the database.
                            ref_ind_db = qcdb_ti - num_hrs_prev_swe + ref_ind
                            if ref_ind_db >= 0:
                                ref_datetime = \
                                    num2date(qcdb_var_time[0] + ref_ind_db,
                                             units=qcdb_var_time_units,
                                             only_use_cftime_datetimes=False)
                                logger.debug('Also flagging ' +
                                             'observation {} at {}.'.
                                             format(site_prev_swe_val_mm[ref_ind],
                                                    ref_datetime))
                                # First make sure the previous value is not
                                # flagged.
                                if qcdb_swe_qc_flag[qcdb_si, ref_ind_db] & \
                                   (1 << qc_bit) != 0:
                                    print('ERROR: (PROGRAMMING) ' +
                                          'reference value was ' +
                                          'previously flagged and should ' +
                                          'not have been used.',
                                          file=sys.stderr)
                                    qcdb.close()
                                    sys.exit(1)
                                # Flag the previous value.
                                qcdb_swe_qc_flag[qcdb_si, ref_ind_db] = \
                                    qcdb_swe_qc_flag[qcdb_si,ref_ind_db] | \
                                    (1 << qc_bit)
                                # Identify the previous value as having been
                                # through this check, even though this is
                                # only indirectly the case. Most likely it has
                                # already been tested, and passed, but now is
                                # associated with the later problematic report.
                                qcdb_swe_qc_chkd[qcdb_si, ref_ind_db] = \
                                    qcdb_swe_qc_chkd[qcdb_si, ref_ind_db] \
                                    | (1 << qc_bit)

                        num_flagged_swe_pr_cons_this_time += 1
                        num_flagged_swe_pr_cons += 1

                    if flag is not None:

                        # Turn on the QC checked bit for this test, regardless
                        # of whether the observation was flagged.
                        qcdb_swe_qc_chkd[qcdb_si, qcdb_ti] = \
                            qcdb_swe_qc_chkd[qcdb_si, qcdb_ti] | (1 << qc_bit)

                else:

                    # Test has already been performed.
                    flag_value = qcdb_swe_qc_flag[qcdb_si, qcdb_ti] & \
                                 (1 << qc_bit)
                    if flag_value == 0:
                        flag_str = 'not flagged'
                    else:
                        flag_str = 'flagged'
                    if args.verbose:
                        print('INFO: check "{}" already done for site {} ({}) '.
                              format(qc_test_name,
                                     site_swe_station_id,
                                     site_swe_obj_id) +
                              'value {} ({})'.
                              format(site_swe_val_mm, flag_str))

            ######################################################
            # Perform precipitation-SWE ratio consistency check. #
            ######################################################

            # This is an adaptation of the test described in Durre (2010)
            # Table 3 (internal and temporal consistency checks) as
            # "SNWD/PRCP ratio".

            qc_test_name = 'precip_ratio'

            if wdb_swe_prcp_si is not None and \
               wdb_prev_swe_si is not None:

                # Previous swe data is available, making this test
                # possible.

                # Identify the QC bit for the test.
                try:
                    ind = swe_qc_test_names.index(qc_test_name)
                except ValueError:
                    # Try backward compatible option "depth_precip_ratio".
                    qc_test_name = 'depth_precip_ratio'
                    ind = swe_qc_test_names.index(qc_test_name)

                qc_bit = swe_qc_test_bits[ind]
                if swec_qc_test_names[ind] != qc_test_name:
                    print('ERROR: inconsistent qc_test_names data in ' +
                          'QC database.',
                          file=sys.stderr)
                    qcdb.close()
                    exit(1)
                if swec_qc_test_bits[ind] != swe_qc_test_bits[ind]:
                    print('ERROR: inconsistent qc_test_bits data in ' +
                          'QC database.',
                          file=sys.stderr)
                    qcdb.close()
                    exit(1)

                if not qcdb_swe_qc_chkd[qcdb_si, qcdb_ti] & (1 << qc_bit):

                    # Test has not been performed for this observation.

                    # Indices for locating this station:
                    # 1. wdb_swe_si    = the station index in
                    #                     wdb_swe_val_mm
                    # 2. qcdb_si        = the station index in the QC database
                    #                     which is covered by these arrays:
                    #                     qcdb_prev_swe_qc_flag
                    #                     qcdb_swe_qc_chkd
                    #                     qcdb_swe_qc_flag
                    # 3. wdb_prev_swe_si = the station index in
                    #                     prev_swe and wdb_prev_swe_obj_id
                    # 4. wdb_swe_prcp_si    = the station index in wdb_swe_prcp_val_mm

                    prev_swe_ti = num_hrs_prev_swe - num_hrs_prcp

                    site_prev_swe_val_mm = \
                        wdb_prev_swe_val_mm[wdb_prev_swe_si, prev_swe_ti:]

                    site_prev_swe_qc = \
                        qcdb_prev_swe_qc_flag[qcdb_si, prev_swe_ti:]

                    site_prcp_val_mm = wdb_swe_prcp_val_mm[wdb_swe_prcp_si]

                    flag, ref_ind = \
                        qc_durre_swe_prcp_ratio(site_swe_val_mm,
                                                 site_prev_swe_val_mm,
                                                 site_prev_swe_qc,
                                                 site_prcp_val_mm)

                    if flag:
 
                        logger.debug('Flagging SWE change ' +
                                     '{} '.
                                     format(site_prev_swe_val_mm[ref_ind]) +
                                     'to {} '.format(site_swe_val_mm) +
                                     'at station {} '.
                                     format(site_swe_station_id) +
                                     '({}) '.format(site_swe_obj_id) +
                                     '("{}").'.format(qc_test_name))

                        # Turn on the QC bit for this test.
                        qcdb_swe_qc_flag[qcdb_si, qcdb_ti] = \
                            qcdb_swe_qc_flag[qcdb_si, qcdb_ti] | (1 << qc_bit)

                        if flag_swe_change_prcp_low_value:

                            # Flag previous value as well if that time fits
                            # into the database.
                            ref_ind_db = qcdb_ti - num_hrs_prev_swe + ref_ind
                            if ref_ind_db >= 0:
                                ref_datetime = \
                                    num2date(qcdb_var_time[0] + ref_ind_db,
                                             units=qcdb_var_time_units,
                                             only_use_cftime_datetimes=False)
                                logger.debug('INFO: also flagging ' +
                                             'observation {} at {}.'.
                                             format(site_prev_swe_val_mm[ref_ind],
                                                    ref_datetime))
                                # First make sure the previous value is not
                                # flagged.
                                if qcdb_swe_qc_flag[qcdb_si, ref_ind_db] & \
                                   (1 << qc_bit) != 0:
                                    print('ERROR: (PROGRAMMING) ' +
                                          'reference value was ' +
                                          'previously flagged and should ' +
                                          'not have been used.',
                                          file=sys.stderr)
                                    qcdb.close()
                                    sys.exit(1)
                                # Flag the previous value.
                                qcdb_swe_qc_flag[qcdb_si, ref_ind_db] = \
                                    qcdb_swe_qc_flag[qcdb_si,ref_ind_db] | \
                                    (1 << qc_bit)
                                # Identify the previous value as having been
                                # through this check, even though this is
                                # only indirectly the case. Most likely it has
                                # already been tested, and passed, but now is
                                # associated with the later problematic report.
                                qcdb_swe_qc_chkd[qcdb_si, ref_ind_db] = \
                                    qcdb_swe_qc_chkd[qcdb_si, ref_ind_db] \
                                    | (1 << qc_bit)

                        num_flagged_swe_pr_cons_this_time += 1
                        num_flagged_swe_pr_cons += 1

                    if flag is not None:

                        # Turn on the QC checked bit for this test, regardless
                        # of whether the observation was flagged.
                        qcdb_swe_qc_chkd[qcdb_si, qcdb_ti] = \
                            qcdb_swe_qc_chkd[qcdb_si, qcdb_ti] | (1 << qc_bit)

                else:

                    # Test has already been performed.
                    flag_value = qcdb_swe_qc_flag[qcdb_si, qcdb_ti] & \
                                 (1 << qc_bit)
                    if flag_value == 0:
                        flag_str = 'not flagged'
                    else:
                        flag_str = 'flagged'
                    if args.verbose:
                        print('INFO: check "{}" already done for site {} ({}) '.
                              format(qc_test_name,
                                     site_swe_station_id,
                                     site_swe_obj_id) +
                              'value {} ({})'.
                              format(site_swe_val_mm, flag_str))


        ######################################
        # SNOW WATER EQUIVALENT (SWE) QC END #
        ######################################


        ############################################
        # QC checks finished for the current time. #
        ############################################

        logger.info('Added {} '.format(num_stations_added_this_time) +
                    'stations to the database at {}.'.format(obs_datetime))
        logger.debug('Flagged {} '.format(num_flagged_sd_wr_this_time) +
                     'snow depth obs. at {} '.format(obs_datetime) +
                     'for world record exceedance.')
        logger.debug('Flagged {} '.
                     format(num_flagged_sd_change_wr_this_time) +
                     'snow depth obs. at {} '.format(obs_datetime) +
                     'for world record change exceedance.')
        logger.debug('Flagged {} '.format(num_flagged_sd_streak_this_time) +
                     'snow depth obs. at {} '.format(obs_datetime) +
                     'for streak.')
        logger.debug('Flagged {} '.format(num_flagged_sd_gap_this_time) +
                     'snow depth obs. at {} '.format(obs_datetime) +
                     'for gap.')
        logger.debug('Flagged {} '.
                     format(num_flagged_sd_at_cons_this_time) +
                     'snow depth obs. at {} '.format(obs_datetime) +
                     'for snow depth/air temperature consistency.')
        logger.debug('Flagged {} '.
                     format(num_flagged_sd_sf_cons_this_time) +
                     'snow depth obs. at {} '.format(obs_datetime) +
                     'for snow depth/snowfall consistency.')
        logger.debug('Flagged {} '.
                     format(num_flagged_sd_pr_cons_this_time) +
                     'snow depth obs. at {} '.format(obs_datetime) +
                     'for snow depth/precipitation consistency.')

        # Update the "last_datetime_updated" attribute.
        # NOTE: Possibly only do this if the obs_datetime is earlier than the
        # current time(dt.datetime.utcnow) by more than e.g. 3 days.
        # min_days_of_latency = 2
        # if current_update_datetime - obs_datetime > \
        #    dt.timedelta(days=min_days_of_latency):
        qcdb.setncattr_string('last_datetime_updated',
                              obs_datetime.strftime('%Y-%m-%d %H:%M:%S UTC'))
        num_hrs_updated += 1

        if num_hrs_updated % database_commit_period == 0:

            # Close the temporary database copy.
            qcdb.close()

            # Move the database copy into place.
            try:
                shutil.move(temp_database_path, args.database_path)
            except:
                logger.error('Failed to replace {} '.
                             format(args.database_path) +
                             'with temporary copy {}.'.
                             format(temp_database_path))
                sys.exit(1)
            temp_database_exists = False
            logger.debug('Committed updates through {} '.
                         format(obs_datetime.
                                strftime('%Y-%m-%d %H:%M:%S UTC')) +
                         'to {}'.format(args.database_path))

            logger.debug('Time index {}, last index {}'.
                         format(qcdb_ti, qcdb_update_time_ind[-1]))
            if args.max_update_hours is not None:
                logger.debug('# hours updated {}, max. update hours {}'.
                             format(num_hrs_updated, args.max_update_hours))

            # if qcdb_ti == qcdb_update_time_ind[-1] or \
            #    (args.max_update_hours is not None and
            #     num_hrs_updated == args.max_update_hours):
            #     logger.debug('Making a new temporary database is not ' +
            #                  'necessary here.')

            # logger.debug(qcdb_si != qcdb_update_time_ind[-1])
            # logger.debug(args.max_update_hours is None or
            #              num_hrs_updated != args.max_update_hours)
            if qcdb_ti != qcdb_update_time_ind[-1] and \
               (args.max_update_hours is None or
                num_hrs_updated != args.max_update_hours):

                # We are not finished yet.
                # Create a new copy of the QC database.
                temp_database_path = station_qc_db_copy(args.database_path,
                                                        verbose=args.verbose)

                # Open the new copy of the QC database.
                try:
                    qcdb = Dataset(temp_database_path, 'r+')
                except:
                    logger.error('Failed to open QC database {}.'
                                 .format(temp_database_path))
                    sys.exit(1)
                temp_database_exists = True

            else:

                logger.debug('A new temporary database is not necessary. ' +
                             'The update is finished.')

        #     just_committed = True

        # else:

        #     just_committed = False

        if args.max_update_hours is not None:
            if num_hrs_updated >= args.max_update_hours:
                break

        # - For all snow depth obs:
        #   - Fetch station metadata from wdb0
        #   - If obj_id of obs is not in qcdb:
        #     - Add metadata for this station to qcdb
        #   - Else
        #     - Verify metadata for this station; change if necessary.

    logger.info('Added a total of {} '.format(num_stations_added) +
                'stations to the database.')
    logger.debug('Flagged {} snow depth obs. for world record exceedance.'.
                 format(num_flagged_sd_wr))
    logger.debug('Flagged {} snow depth changes '.
                 format(num_flagged_sd_change_wr) +
                 'for world record exceedance.')
    logger.debug('Flagged {} snow depth obs. for streak.'.
                 format(num_flagged_sd_streak))
    logger.debug('Flagged {} snow depth obs. for gap.'.
                 format(num_flagged_sd_gap))
    logger.debug('Flagged {} snow depth obs.'.
                 format(num_flagged_sd_at_cons) +
                 'for snow depth/air\ temp. consistency.')
    logger.debug('Flagged {} snow depth obs.'.
                 format(num_flagged_sd_sf_cons) +
                 'for snow depth/snowfall consistency.')
    logger.debug('Flagged {} snow depth obs.'.
                 format(num_flagged_sd_pr_cons) +
                 'for snow depth/precipitation consistency.')

    # if args.check_climatology:
    #     for i, id in enumerate(sd_gap_station_id):
    #         csv_file.write('{},{},{},{},{},{},{},{},{}\n'.
    #                        format(id,
    #                               sd_gap_station_obj_id[i],
    #                               sd_gap_date[i],
    #                               sd_gap_val_cm[i],
    #                               sd_gap_ob_med_val_cm[i],
    #                               sd_gap_ref_val_cm[i],
    #                               sd_gap_cl_med_val_cm[i],
    #                               sd_gap_cl_max_val_cm[i],
    #                               sd_gap_cl_iqr_val_cm[i]))

    # if args.check_climatology:
    #     csv_file.close()

    logger.info('Database updated to {}.'.
                format(obs_datetime.strftime('%Y-%m-%d %H:%M:%S UTC')))

    # if just_committed:
    #
    #     # The temporary copy of the QC database was opened and closed with no
    #     # modifications. Delete it.
    #     try:
    #         os.remove(temp_database_path)
    #     except:
    #         logger.error('Failed to delete unmodified database copy {}.'.
    #                      format(temp_database_path))
    #         sys.exit(1)
    #     logger.debug('Deleted unmodified database copy {}.'.
    #                  format(temp_database_path))
    # else:
    if temp_database_exists:

        # Close the temporary database copy.
        qcdb.close()

        # Move the database copy into place.
        try:
            shutil.move(temp_database_path, args.database_path)
        except:
            print('ERROR: Failed to replace {} '.
                  format(args.database_path) +
                  'with temporary copy {}.'.format(temp_database_path),
                  file=sys.stderr)
            sys.exit(1)
        logger.debug('Database copy {} moved to {}.'.
                     format(temp_database_path, args.database_path))

    # try:
    #     shutil.move(temp_database_path, args.database_path)
    # except:
    #     print('ERROR: Failed to replace {} '.format(args.database_path) +
    #           'with temporary copy {}.'.format(temp_database_path),
    #           file=sys.stderr)
    #     sys.exit(1)

    # if args.verbose:
    #     print('INFO: Database copy {} moved to {}.'.
    #           format(temp_database_path, args.database_path))

    logger.info('So far so good.')


if __name__ == '__main__':
    main()
