#!/usr/bin/python3.6
'''
  Clear out old data unlikely to be needed for NWM v2.0 database development.
'''

import argparse
import datetime as dt
import glob
import re
import sys
import os
import time
import errno
import shutil
import pyproj
import pandas as pd
import numpy as np
from netCDF4 import Dataset, num2date
#from netCDF4 import Dataset, date2num, num2date
#import math


def timedelta_to_int_hours(delta_time):

    """
    Converts a datetime timedelta object to an integer number of hours.
    """

    delta_time_hours = delta_time.days * 24 + \
                       delta_time.seconds // 3600
    # delta_time.total_seconds() // 3600 also works
    return delta_time_hours


def main():

    """
    """

    ref_datetime = dt.datetime.utcnow()

    # Search for NWM data to sample for this database file.
    nwm_archive_dir = '/nwcdev/archive/NWM_v2.0_archive'

    # Regex for extended analysis.
    ana_ext_str = 'analysis_assim_extend'
    regex_str = '^nwm\.[0-9]{8}\.t[0-9]{2}z\.' + \
                ana_ext_str + '\.[^.]+\.tm[0-9]{2}\.conus\.nc$'
    ana_ext_pattern = re.compile(regex_str)

    ana_str = 'analysis_assim'
    regex_str = '^nwm.[0-9]{8}\.t[0-9]{2}z\.' + \
                ana_str + '\.[^.]+\.tm[0-9]{2}\.conus\.nc$'
    ana_pattern = re.compile(regex_str)

    nwm_file_paths = []
    nwm_file_names = []
    nwm_file_time_minus_hours = []
    nwm_file_datetimes = []
    nwm_file_cycle_datetimes = []
    nwm_file_cycle_types = []

    # Loop over all NWM files of supported types; determine which
    # to include in this update.
    for root, dirs, files in os.walk(nwm_archive_dir, followlinks=True):

        if len(files) == 0:
            continue

        for file_name in files:

            if ana_ext_pattern.fullmatch(file_name) is None and \
               ana_pattern.fullmatch(file_name) is None:
                continue

            nwm_file_name = file_name
            nwm_file_path = os.path.join(root, file_name)

            # Determine the datetime of the data in the current NWM
            # file.
            cycle_yyyymmdd = re.findall('[0-9]{8}', nwm_file_name)[0]
            cycle_hh = re.findall('\.t[0-9]{2}z\.', nwm_file_name)[0][2:-2]
            time_minus_hours = int(re.findall('\.tm[0-9]{2}\.',
                                              nwm_file_name)[0][3:-1])
            cycle_datetime = dt.datetime.strptime(cycle_yyyymmdd + cycle_hh,
                                                  '%Y%m%d%H')
            nwm_file_datetime = cycle_datetime - \
                                dt.timedelta(hours=time_minus_hours)

            # Get the cycle type from the filename and convert to
            # a numerical value based on the cycle_type_themes
            # table.
            nwm_cycle_type_file = \
                re.findall('^nwm\.[0-9]{8}\.t[0-9]{2}z\.[^.]+\.',
                           nwm_file_name)[0][18:-1]

            if nwm_cycle_type_file == 'analysis_assim_extend':
                nwm_cycle_type = -28
            elif nwm_cycle_type_file == 'analysis_assim':
                nwm_cycle_type = -3
            else:
                print('WARNING: Unsupported cycle type ' +
                      '"{}" '.format(nwm_cycle_type_file) +
                      ' in NWM file {}.'.format(nwm_file_name),
                      file=sys.stderr)
                continue

            nwm_group = re.findall('\.[^.]+\.tm[0-9]{2}\.conus\.nc$',
                                   nwm_file_name)[0][1:-14]
            nwm_time_minus_hours = int(re.findall('\.tm[0-9]{2}\.',
                                                  nwm_file_name)[0][3:-1])

            # Combine files to be processed
            nwm_file_paths.append(nwm_file_path)
            nwm_file_names.append(nwm_file_name)
            nwm_file_time_minus_hours.append(time_minus_hours)
            nwm_file_datetimes.append(nwm_file_datetime)
            nwm_file_cycle_datetimes.append(cycle_datetime)
            nwm_file_cycle_types.append(nwm_cycle_type)


    if len(nwm_file_names) == 0:
        print('INFO: No archived NWM files found.')
        sys.exit(0)

    # Zip NWM file lists for group sorting.
    zipped = zip(nwm_file_names,
                 nwm_file_paths,
                 nwm_file_cycle_types,
                 nwm_file_time_minus_hours,
                 nwm_file_datetimes,
                 nwm_file_cycle_datetimes)

    # Sort NWM files in ascending order of nwm_cycle_type.
    zipped = sorted(zipped, key=lambda nwm_file_list: nwm_file_list[2],
                    reverse=False)
    # Sort NWM files in descending order of nwm_file_time_minus_hours.
    zipped = sorted(zipped, key=lambda nwm_file_list: nwm_file_list[3],
                    reverse=True)
    # Sort NWM files in order of nwm_file_datetime.
    zipped = sorted(zipped, key=lambda nwm_file_list: nwm_file_list[4],
                    reverse=False)
    # Unzip the sorted lists.
    nwm_file_names, \
        nwm_file_paths, \
        nwm_file_cycle_types, \
        nwm_file_time_minus_hours, \
        nwm_file_datetimes, \
        nwm_file_cycle_datetimes = zip(*zipped)

    # The NWM files are now sorted in chronological order, with files
    # having larger "tm" values first, whenever multiple files cover
    # the same datetime. This reflects the preference for larger "tm"
    # values (when the files for smaller "tm" values are encountered
    # they will be skipped since data with a higher "tm" value has
    # already been processed), and avoids processing data that would
    # just be overwritten immediately, which is wasteful.

    # Loop over all NWM files.
    num_nwm_files_read = 0
    prev_nwm_file_datetime = dt.datetime.strptime('1970010100',
                                                  '%Y%m%d%H')
    for nfi, a_nwm_file_name in enumerate(nwm_file_names):

        # Determine the cycle type from the filename.
        nwm_cycle_type_file = \
            re.findall('^nwm\.[0-9]{8}\.t[0-9]{2}z\.[^.]+\.',
                       a_nwm_file_name)[0][18:-1]
        if nwm_cycle_type_file == 'analysis_assim_extend':
            nwm_cycle_type = -28
        elif nwm_cycle_type_file == 'analysis_assim':
            nwm_cycle_type = -3
        else:
            print('WARNING: Unsupported cycle type ' +
                  '"{}" '.format(nwm_cycle_type_file) +
                  ' in NWM file {}.'.format(a_nwm_file_name),
                  file=sys.stderr)
            continue

        # Identify whether this file is a "land" or "forcing" file
        # based on the filename.
        # Truth: used this filename-parsing approach before
        # discovering the "model_output_type" global attribute,
        # which is a far better way to get the information. Keeping
        # the filename-based approach for reference. It does no harm
        # since it now just verifies the "model_output_type" used
        # after the NWM file is opened (below).
        nwm_group = re.findall('\.[^.]+\.tm[0-9]{2}\.conus\.nc$',
                               a_nwm_file_name)[0][1:-14]

        # Get time information as interpreted from the NWM file name.
        nwm_time_minus_hours = nwm_file_time_minus_hours[nfi]
        nwm_file_datetime = nwm_file_datetimes[nfi]
        nwm_file_cycle_datetime = nwm_file_cycle_datetimes[nfi]

        ref_time_lag = ref_datetime - nwm_file_datetime
        if ref_time_lag.days < 64:
            continue

        if nwm_file_datetime != prev_nwm_file_datetime:

            print('new datetime {}'.format(nwm_file_datetime))
            prev_nwm_file_datetime = nwm_file_datetime

            land_ref_this_datetime = False
            forcing_ref_this_datetime = False

        if nwm_group == 'land' and land_ref_this_datetime is False:
            print('land ref for {} = {}'.
                  format(nwm_file_datetime, a_nwm_file_name))
            land_ref_this_datetime = True
            continue

        if nwm_group == 'forcing' and forcing_ref_this_datetime is False:
            print('forcing ref for {} = {}'.
                  format(nwm_file_datetime, a_nwm_file_name))
            forcing_ref_this_datetime = True
            continue

        if nwm_time_minus_hours == 0:
            print('init file {}'.format(a_nwm_file_name))
            continue

        #print('DELETE {}'.format(a_nwm_file_name))
        print('DELETE {}'.format(nwm_file_paths[nfi]))
        os.remove(nwm_file_paths[nfi])


if __name__ == '__main__':
    main()
