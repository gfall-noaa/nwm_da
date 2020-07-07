import datetime as dt
import pandas as pd
import time
import calendar

def string_to_datetime(date_str, date_format='%Y-%m-%d %H:%M:%S'):
    '''
    Convert a string time in the default format of %Y-%m-%d %H:%M:%S
    to a datetime unless a different date_format is given
    '''
    return dt.datetime.strptime(date_str, date_format)


def datetime_to_string(date_dt, date_format='%Y-%m-%d %H:%M:%S'):
    '''
    Convert a datetime time info to a string of %Y-%m-%d %H:%M:%S
    as default unless a different date_format is given.
    '''
    return dt.datetime.strftime(date_dt, date_format)


def datetime_to_utc_epoch(datetime_in):
    """
    Convert a (UTC) datetime into seconds since 1970-01-01 00:00:00.
    """
    return calendar.timegm(datetime_in.timetuple())


def utc_epoch_to_datetime(datetime_epoch):
    """
    Convert a (UTC) date/time expressed as seconds since 1970-01-01 00:00:00
    to a datetime type.
    """
    return \
        dt.datetime.strptime('1970-01-01 00:00:00', '%Y-%m-%d %H:%M:%S') + \
        dt.timedelta(seconds=datetime_epoch)


def utc_epoch_to_string(datetime_epoch, date_format='%Y-%m-%d %H:%M:%S'):
    """
    Convert a (UTC) date/time expressed as seconds since 1970-01-01 00:00:00
    to a string in the default form "YYYY-MM-DD HH:MM:SS". If date_foramt
    is given, use the given format.
    """
    return dt.datetime.strftime(utc_epoch_to_datetime(datetime_epoch),
                                date_format)


def string_to_utc_epoch(date_str, date_format='%Y-%m-%d %H:%M:%S'):
    '''
    Convert a string time in the default format of %Y-%m-%d %H:%M:%S
    to utc_epoch seconds via a datetime unless a date_format is given
    '''
    date_dt = dt.datetime.strptime(date_str, date_format)
    return calendar.timegm(date_dt.timetuple())


def timedelta_to_int_hours(delta_time):

    """
    Converts a datetime timedelta object to an integer number of hours.
    """

    delta_time_hours = delta_time.days * 24 + \
                       delta_time.seconds // 3600
    # delta_time.total_seconds() // 3600 also works
    return delta_time_hours


def datetime_series_to_epoch(dts):
    '''From a single value of datetime series to epoch seconds'''
    #numpy.datetime64
    dt64 = pd.to_datetime(dts).values[0]
    #Timestamp
    dt_ts = pd.to_datetime(dt64)
    #datetime
    dtime = pd.to_datetime(dt_ts).to_pydatetime()
    datetime_ep = datetime_to_utc_epoch(dtime)
    return datetime_ep


def ymdh_from_epoch(dt_ep):
    '''extract year, month, day, and hour values from a epoch seconds'''
    year = time.gmtime(dt_ep).tm_year
    month = time.gmtime(dt_ep).tm_mon
    day = time.gmtime(dt_ep).tm_mday
    hour = time.gmtime(dt_ep).tm_hour
    return year, month, day, hour


def ymdh_from_datetime(dtime):
    '''extract year, month, day, and hour values from a datetime'''
    datetime_idx = pd.DatetimeIndex(dtime)
    year = datetime_idx.year[0]
    month = datetime_idx.month[0]
    day = datetime_idx.day[0]
    hour = datetime_idx.hour[0]
    return year, month, day, hour


def day_from_datetime(dtime):
    '''extract day value from a datetime'''
    datetime_idx = pd.DatetimeIndex(dtime)
    day = datetime_idx.day[0]
    return day


def hour_from_datetime(dtime):
    '''extract hour value from a datetime'''
    datetime_idx = pd.DatetimeIndex(dtime)
    hour = datetime_idx.hour[0]
    return hour
