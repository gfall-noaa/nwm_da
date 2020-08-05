#!/usr/bin/python3

import datetime as dt

time1 = dt.datetime.strptime('2020-01-10 13:00:00 UTC',
                             '%Y-%m-%d %H:%M:%S UTC')
time2 = dt.datetime.strptime('2020-01-10 19:00:00 UTC',
                             '%Y-%m-%d %H:%M:%S UTC')
time_range = time2 - time1
num_hours = time_range.days * 24 + time_range.seconds // 3600 + 1
prev_obs_datetime = [time1 +
                     dt.timedelta(hours=i) for i in range(num_hours)]

begin_datetime = dt.datetime.strptime('2020-01-10 12:00:00 UTC',
                                      '%Y-%m-%d %H:%M:%S UTC')
end_datetime = dt.datetime.strptime('2020-01-10 18:00:00 UTC',
                                    '%Y-%m-%d %H:%M:%S UTC')
time_range = end_datetime - begin_datetime
num_hours = time_range.days * 24 + time_range.seconds // 3600 + 1
obs_datetime = [begin_datetime +
                dt.timedelta(hours=i) for i in range(num_hours)]

prev_begin_datetime = prev_obs_datetime[0]
prev_end_datetime = prev_obs_datetime[-1]

keep_going = True

both = sorted(list(set(prev_obs_datetime) & set(obs_datetime)))
if len(both) == 0:
    keep_going = False

if keep_going:
    obs_in_prev_obs = [obs_datetime.index(dt_in_both) for dt_in_both in both]
    if (obs_in_prev_obs[0] != 0) and (obs_in_prev_obs[-1] != (num_hours - 1)):
        # Multiple SELECT statements (or a more complex select statement)
        # would be required to fill out the data.
        keep_going = False

if keep_going:
    need = sorted(list(set(obs_datetime) - set(prev_obs_datetime)))
    print(need)
    new_data_begin_datetime = need[0]
    new_data_end_datetime = need[-1]


print(keep_going)
print(obs_in_prev_obs)
print(new_data_begin_datetime)
print(new_data_end_datetime)
# print([need[i].strftime('%Y%m%d%H') for i in range(len(need))])

# print([prev_obs_datetime[i].strftime('%Y%m%d%H')
#        for i in range(len(prev_obs_datetime))])
# print([obs_datetime[i].strftime('%Y%m%d%H')
#        for i in range(len(obs_datetime))])
# print([both[i].strftime('%Y%m%d%H')
#        for i in range(len(both))])

if __name__ == 'main':
    main()
