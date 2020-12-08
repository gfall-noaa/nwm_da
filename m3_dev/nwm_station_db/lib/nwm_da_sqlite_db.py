import os
import sys
import re
import glob
import datetime as dt

sys.path.append(os.path.join(os.path.dirname(__file__),
                             '..', '..', '..','lib'))
import nwm_da_time as ndt

def attach_databases(conn,
                     forcing_single_db,
                     land_single_db):
    '''
    Attach companion land and forcing data databases.
    '''

    if os.path.isfile(forcing_single_db):
        conn.execute('ATTACH DATABASE "' + forcing_single_db + '" AS forcing_single')
    else:
        print('Database file {} does not exist. Need to create it first'.format(forcing_single_db))
        sys.exit(1)

    if os.path.isfile(land_single_db):
        conn.execute('ATTACH DATABASE "' + land_single_db + '" AS land_single')
    else:
        print('Database file {} does not exist. Need to create it first'.format(land_single_db))
        sys.exit(1)


def verify_base_database(base_name, db_dir):
    '''
    Locate and verify the base database files to be updated.
    '''
    if "_arch" in base_name or '_archive' in base_name:
        pattern = \
            os.path.join(db_dir, base_name +
                         '_[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]' +
                         '_to_' +
                         '[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]' +
                         '_base.db')
        oper = False
    elif "_oper" in base_name:
        pattern = os.path.join(db_dir, base_name + '_base.db')
        oper = True
    else:
        print('ERROR: Check your base name of the database to be updated')
        sys.exit(1)
    db_paths = glob.glob(pattern)
    db_paths.sort()
    #print('debug: (db_paths/patteren)', db_paths, pattern)

    return oper, db_paths


def get_start_finish_date_from_name(db_file, oper):
    '''
    Get start and finish date info from the given database file.
    '''
    dates = re.findall('[0-9]{10}', db_file)
    if len(dates) > 0 and oper is False:
        db_start_date_from_name = dates[-2]
        db_start_datetime_from_name_dt = \
            dt.datetime.strptime(db_start_date_from_name, '%Y%m%d%H')
        db_start_datetime_from_name_ep = \
            ndt.datetime_to_utc_epoch(db_start_datetime_from_name_dt)
        db_finish_date_from_name = dates[-1]
        db_finish_datetime_from_name_dt = \
            dt.datetime.strptime(db_finish_date_from_name, '%Y%m%d%H')
        db_finish_datetime_from_name_ep = \
            ndt.datetime_to_utc_epoch(db_finish_datetime_from_name_dt)
    else:
        db_start_datetime_from_name_dt = None
        db_finish_datetime_from_name_dt = None
        db_start_datetime_from_name_ep = None
        db_finish_datetime_from_name_ep = None

    return db_start_datetime_from_name_ep, db_finish_datetime_from_name_ep


def get_start_finish_date_from_db(conn):
    '''
    Get start and finish date info from the given database file.
    '''
    db_start_datetime_from_db_ep = \
        conn.execute("SELECT start_date FROM databases_info").fetchone()[0]

    db_finish_datetime_from_db_ep = \
        conn.execute("SELECT finish_date FROM databases_info").fetchone()[0]

    return db_start_datetime_from_db_ep, db_finish_datetime_from_db_ep


def get_data_column_names(conn, land_layer):
    '''
    Get column names in the orginal order from data databases.
    '''
    tb_info = conn.execute("PRAGMA forcing_single. \
                            table_info('nwm_forcing_single_layer')").fetchall()
    column_names = list(zip(*tb_info))[1]
    forcing_single_layer_col_names = ','.join(column_names)

    tb_info = conn.execute("PRAGMA \
                            land_single.table_info('nwm_land_single_layer')") \
                            .fetchall()
    column_names = list(zip(*tb_info))[1]
    land_single_layer_col_names = ','.join(column_names)

    if len(land_layer) != 0:
        tb_info = conn.execute("PRAGMA \
                                land_snow.table_info('nwm_land_snow_layers')") \
                               .fetchall()
        column_names = list(zip(*tb_info))[1]
        land_snow_layer_col_names = ','.join(column_names)
    else:
        land_snow_layer_col_names = []

    return forcing_single_layer_col_names, \
           land_single_layer_col_names, \
           land_snow_layer_col_names

