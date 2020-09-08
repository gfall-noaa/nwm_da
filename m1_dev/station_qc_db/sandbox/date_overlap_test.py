#!/usr/bin/python3

import datetime as dt
import sys
import os
import logging
import socket

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', '..',
                             'lib'))
import wdb0
import local_logger


logger = local_logger.init(logging.INFO)

pkl_dir = '/net/scratch/nwm_snow_da/wdb0_pkl'

time1 = dt.datetime.strptime('2020-01-10 11:00:00 UTC',
                             '%Y-%m-%d %H:%M:%S UTC')
time2 = dt.datetime.strptime('2020-01-11 11:00:00 UTC',
                             '%Y-%m-%d %H:%M:%S UTC')
time_range = time2 - time1
num_hours = time_range.days * 24 + time_range.seconds // 3600 + 1
prev_obs_datetime = [time1 +
                     dt.timedelta(hours=i) for i in range(num_hours)]

# Get "prior" air temperatures. Okay to get them from a .pkl file.
current_datetime = dt.datetime.utcnow()
# wdb_prior_tair = \
#     wdb0.get_air_temp_obs(time1, time2, verbose=True)
wdb_prior_tair = \
    wdb0.get_air_temp_obs(time1, time2, scratch_dir=pkl_dir, verbose=True)
wdb_prev_tair_datetime = dt.datetime.utcnow()
elapsed_time = wdb_prev_tair_datetime - current_datetime
print('INFO: query ran in {} seconds.'.
      format(elapsed_time.total_seconds()))

begin_datetime = dt.datetime.strptime('2020-01-10 12:00:00 UTC',
                                      '%Y-%m-%d %H:%M:%S UTC')
end_datetime = dt.datetime.strptime('2020-01-11 15:00:00 UTC',
                                    '%Y-%m-%d %H:%M:%S UTC')
time_range = end_datetime - begin_datetime
num_hours = time_range.days * 24 + time_range.seconds // 3600 + 1
obs_datetime = [begin_datetime +
                dt.timedelta(hours=i) for i in range(num_hours)]

prev_begin_datetime = prev_obs_datetime[0]
prev_end_datetime = prev_obs_datetime[-1]

keep_going = True

dt_in_both = sorted(list(set(prev_obs_datetime) & set(obs_datetime)))
if len(dt_in_both) == 0:
    keep_going = False

if keep_going:
    obs_in_prev_obs = [obs_datetime.index(i) for i in dt_in_both]
    if (obs_in_prev_obs[0] != 0) and (obs_in_prev_obs[-1] != (num_hours - 1)):
        # Multiple SELECT statements (or a more complex select statement)
        # would be required to fill out the data.
        keep_going = False

if keep_going:
    dt_needed = sorted(list(set(obs_datetime) - set(prev_obs_datetime)))
#     new_data_begin_datetime = dt_needed[0]
#     new_data_end_datetime = dt_needed[-1]

# print(keep_going)
# print(obs_in_prev_obs)
# print(new_data_begin_datetime)
# print(new_data_end_datetime)

# Get new temperatures - leave out "scratch_dir=pkl_dir" so the function has
# to do the work.
t1 = dt.datetime.utcnow()
print('calling get_air_temp_obs')
wdb_new_tair = \
    wdb0.get_air_temp_obs(begin_datetime,
                          end_datetime,
                          verbose=True,
                          read_pkl=False,
                          prev_obs_air_temp=wdb_prior_tair)
t2 = dt.datetime.utcnow()
elapsed_time = t2 - t1
print('INFO: query ran in {} seconds.'.
      format(elapsed_time.total_seconds()))

# print([need[i].strftime('%Y%m%d%H') for i in range(len(need))])

# print([prev_obs_datetime[i].strftime('%Y%m%d%H')
#        for i in range(len(prev_obs_datetime))])
# print([obs_datetime[i].strftime('%Y%m%d%H')
#        for i in range(len(obs_datetime))])
# print([dt_in_both[i].strftime('%Y%m%d%H')
#        for i in range(len(dt_in_both))])

if __name__ == 'main':
    main()
