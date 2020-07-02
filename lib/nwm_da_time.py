import datetime as dt

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
