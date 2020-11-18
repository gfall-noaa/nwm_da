#!/usr/bin/python3
'''
  This program generates initial set of databases that will be updated
  by the update program. All prameters and control options are
  defined in the configuration file which is either given in the
  command line or use a default file called config.py
'''
import argparse
import datetime as dt
import calendar
import sys
import os
import pathlib
import sqlite3
import importlib
import logging

def parse_args():

    """
    Parse command line argument: now only configuration file.
    """

    # Get the current date.
    #system_datetime = dt.datetime.utcnow()

    help_message = 'Create the NWM analysis station sqlite3 database.'
    parser = argparse.ArgumentParser(description=help_message)
    #parser.add_argument('--config_path',
    parser.add_argument('-c', '--config_path',
                        type=str,
                        metavar='Configure file name and its path',
                        nargs='?',
                        help='Configure file name: ie, config.py or /path/to/file/config.py')
    args = parser.parse_args()

    if args.config_path:
        config_path = args.config_path
        fpath = pathlib.Path(config_path)
        if not fpath.exists():
            print('\nConfig file {} does not exist.'.format(config_path))
            print('Must provide a correct configuration file info.')
            sys.exit(1)
    else:
        print('No configuration file provided!')
        print('Default configuration config.py file will be used.')
        config_path = []

    return args.config_path

#Load user given module or a default module
def load_module(pyfilepath):
    '''
    Load user given module or a default module.
    The default module is called config.py
    '''
    if pyfilepath is None:
       pyfilepath = 'config.py'   
       print('Default module config.py is used')
    dirname, basename = os.path.split(pyfilepath) 
    sys.path.append(dirname)
    module_name = os.path.splitext(basename)[0]
    module = importlib.import_module(module_name)
    return module
    '''
    Now you can directly use the namespace of the imported module, like this:
    a = module.myvar
    b = module.myfunc(a)
    '''

def check_config(cfg):
    '''
    Check and complete database configuration.
    '''

    logger = logging.getLogger()
    
    attr_name = 'RUN_OPTION'
        if not hasattr(cfg, attr_name):
            logger.error('Configuration is missing attribute "{}".'.
                         format(att_name))
            sys.exit(1)
    if cfg.RUN_OPTION == 1:
        attr_name = 'NUM_DAYS_TO_CURRENT_TIME'
        if not hasattr(cfg, attr_name):
            logger.error('Configuration is missing attribute "{}".'.
                         format(att_name))
            sys.exit(1)
        finish_datetime = dt.datetime.utcnow()
        start_datetime = finish_datetime - dt.timedelta(days=NUM_DAYS_TO_CURRENT_TIME)
        cfg.START_DATE = start_datetime.strftime('%Y%m%d%H')
        #start_datetime = None
        cfg.FINISH_DATE = finish_datetime.strftime('%Y%m%d%H')
        #finish_datetime = None
    elif cfg.RUN_OPTION == 2:
        attr_name = 'START_DATE'
        if not hasattr(cfg, attr_name):
            logger.error('Configuration is missing attribute "{}".'.
                         format(att_name))
            sys.exit(1)
        attr_name = 'FINISH_DATE'
        if not hasattr(cfg, attr_name):
            logger.error('Configuration is missing attribute "{}".'.
                         format(att_name))
            sys.exit(1)
        cfg.NUM_DAYS_TO_CURRENT_TIME = 0
    else:
        logger.error('Invalid {} = {} in configuration.'.
                     format(attr_name, getattr(cfg, attr_name)))
        sys.exit(1)
    print('Need to specify a run option!')
    sys.exit(1)

    return cfg


def check_database_table_info(conn, table_name):

    '''
    Checking/Printing table info of a database.

    '''
    pragma_table_info = 'PRAGMA table_info(' + table_name + ')'
    tb_info = conn.execute(pragma_table_info).fetchall()
    column_names = list(zip(*tb_info))[1]
    default_values = list(zip(*tb_info))[4]
    select_str = 'SELECT * from ' + table_name
    row_info = conn.execute(select_str).fetchall()
    print('\nTable <{}> created with following information:'.format(table_name))

    if len(default_values) > 0 and len(row_info) == 0:
        print('   Columns and default values:')
        for col_counter, col_item in enumerate(column_names):
            print('  {}. {}:  {}'.format(col_counter+1, col_item, default_values[col_counter]))
    elif len(default_values) > 0 and len(row_info) == 1:
        print('   Columns and values:')
        for col_counter, col_item in enumerate(column_names):
            print('  {}. {}:  {}'.format(col_counter+1, col_item, row_info[0][col_counter]))
    else:
        print('   Columns:')
        for col_counter, col_item in enumerate(column_names):
            print('  {}. {}'.format(col_counter+1, col_item))

    if len(row_info) > 1:
        print('\n   Row Entries:')
        for row_counter, row_item in enumerate(row_info):
            print('  {}. {}'.format(row_counter+1, row_item))
    else:
        print('   No row info for table {} yet.'.format(table_name))


def main():

    """
    Create a SQLite database for NWM extended
    analysis data sampled at observing stations.
    """

    # Note the system time.
    #time_now = dt.datetime.now()

    # Initialize logger.
    logger = local_logger.init(logging.WARNING)
    if sys.stdout.isatty():
        logger.setLevel(logging.INFO)

    # Read command line arguments.
    cmd_opt = parse_args()
    cfg = load_module(cmd_opt)
    '''
    if cmd_opt is not None:
        config_path = cmd_opt
        print('Using the configuration file: ', cmd_opt)
        dirname, basename = os.path.split(config_path) 
        sys.path.append(dirname)
        module_name = os.path.splitext(basename)[0]
        module = importlib.import_module(module_name)
        cfg = module
    else:
        config_path = 'config.py'
        print('The default configuration file used is: ', config_path)
        dirname, basename = os.path.split(config_path) 
        sys.path.append(dirname)
        module_name = os.path.splitext(basename)[0]
        module = importlib.import_module(module_name)
        cfg = module
        #cfg = importlib.import_module(config_path)
    '''

    #Now load the configuration file
    #print('Loading ', config_path)
    #print(pathlib.Path(config_path).stem)
    #cfg = importlib.import_module(pathlib.Path(config_path).stem)
    #cfg = importlib.import_module(config_path)

    #============================================================
    # Print/Get some info from the configuration file

    print(cfg.START_DATE, cfg.FINISH_DATE)
    print('box:', cfg.MIN_LON, cfg.MAX_LON, cfg.MIN_LAT, cfg.MAX_LAT)


    #Variables for each type
    vars_forcing = cfg.VARS_FORCINGS
    vars_land = cfg.VARS_LAND
    vars_all = vars_forcing + vars_land
    #names = list(zip(*vars_all))[0]
    #print('All variables:', names)

    #============================================================

    # Define/Create NWM extended analysis station databases.

    sqldb_file = os.path.join(cfg.DATABASES['path'], cfg.DATABASES['base_file'])
    print('Base database file: ', sqldb_file)

    fpath = pathlib.Path(sqldb_file)
    if fpath.exists():
        print('INFO: Dababase ', sqldb_file, ' exists.')
        os.rename(sqldb_file, sqldb_file+'.bak')
        print('      It is renamed as ', sqldb_file+'.bak')
        print('\nINFO: New database {} will be created.'.format(sqldb_file))
    else:
        print('INFO: Database {} will be created.'.format(sqldb_file))

    #conn = sqlite3.connect(sqldb_file, detect_types=sqlite3.PARSE_DECLTYPES)
    conn = sqlite3.connect(sqldb_file, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
    # detect_types here is for recognize datetime (type timestamp)
    sql_c = conn.cursor()
    sql_c.execute('PRAGMA encoding = "UTF-8"')


    # Create a station meta table which contains relationships
    # of data columns and sqlite database/table columns
    station_meta_columns = ('sqlite_name text',
                            'postgres_name text',
                            'type text')
    data_col_names = [('obj_identifier', 'obj_identifier', 'integer'),
                      ('id', 'station_id', 'text'),
                      ('name', 'name', 'text'),
                      ('source', 'source', 'text'),
                      ('type', 'station_type', 'text'),
                      ('longitude', 'coordinates[0]', 'real'),
                      ('latitude', 'coordinates[1]', 'real'),
                      ('elevation', 'elevation', 'integer'),
                      ('recorded_elevation', 'recorded_elevation', 'integer'),
                      ('details', 'details', 'text'),
                      ('vendor', 'vendor', 'text'),
                      ('vendor_date', 'vendor_date', 'integer'),
                      ('use', 'use', 'integer'),
                      ('start_date', 'start_date', 'integer'),
                      ('stop_date', 'stop_date', 'integer'),
                      ('added_date', 'added_date', 'integer')]
                      #('vendor_date', 'vendor_date', 'timestamp'),
                      #('use', 'use', 'integer'),
                      #('start_date', 'start_date', 'timestamp'),
                      #('stop_date', 'stop_date', 'timestamp'),
                      #('added_date', 'added_date', 'timestamp')]
    conn.execute("DROP TABLE IF EXISTS station_meta")
    try:
        sql_c.execute('CREATE TABLE IF NOT EXISTS station_meta(%s)' % ', '.join(station_meta_columns))
        conn.commit()
    except sqlite3.OperationalError:
        print('Table station_meta couldn\'t be created.')
    sql_c.executemany("INSERT INTO station_meta VALUES (?,?,?)", data_col_names)
    conn.commit()
    check_database_table_info(sql_c, 'station_meta')




    #Creating a meta table for all forcing/land variables
    # The meta table contains variable names, units,
    # sampling methods, and dimensions, etc
    conn.execute("DROP TABLE IF EXISTS nwm_meta")
    #conn.commit()

    nwm_meta_columns = ('var_name text',
                        'file_type text',
                        'nwm_var_name text',
                        'long_name text',
                        'standard_name text',
                        'units text',
                        'dims text')
                    #'sampling text',
    try:
        sql_c.execute('CREATE TABLE IF NOT EXISTS nwm_meta(%s)' % ', '.join(nwm_meta_columns))
        conn.commit()
    except sqlite3.OperationalError:
        print('Table nwm_meta couldn\'t be created.')

    sql_c.executemany("INSERT INTO nwm_meta VALUES (?,?,?,?,?,?,?)", vars_all)
    conn.commit()

    # print out meta table info for checking purpose
    check_database_table_info(sql_c, 'nwm_meta')

    # Get the sampling info for each variable which will be checked
    # against sampling use option below.
    var_and_sampling_methods = sql_c.execute("SELECT var_name from nwm_meta").fetchall()


    # Create tables for different types forcing and land variables
    # This should base on the variables defined/given in meta table
    # Four tables will need to be created for holding four type of variables
    #  1. nwm_forcing_single_layer  -- no vertical layers for forcing variables for now
    #  2. nwm_land_soil_layers  -- for those containing '_soil_' and '_by_layer'
    #  3. nwm_land_snow_layers  -- for those containing '_snow_' and '_by_layer'
    #  4. nwm_land_single_layer -- for rest of land type of variables

    #common_cols_str = '( station_obj_identifier integer ' + \
    common_cols_str = '(station_obj_identifier integer DEFAULT ' + \
                       str(cfg.ENUM_NDV['int32 missing']) + \
                      ', datetime integer ' + \
                      ', cycle_datetime integer ' + \
                      ', cycle_type integer)'
                      #', datetime timestamp ' + \
                      #', cycle_datetime timestamp ' + \
                      #', sampling text )'
                      #', ensemble integer )'
                      #', cycle_datetime_minus_hours integer ' +\
                      #', value real )'  move to individual var column
                      #', time timestamp CHECK(time >= ' + "'" + str(opt.start_datetime) + "'" + \
                      #'  AND time <=' + "'" + str(opt.finish_datetime) + "')" + \
    #print(common_cols_str)
    #print(str(opt.start_datetime))


    #Retrieve variable names defined/given in the nwm_meta table
    sql_c.execute("SELECT var_name FROM nwm_meta WHERE file_type='forcing'")
    forcing_var_names = sql_c.fetchall()
    sql_c.execute("SELECT var_name FROM nwm_meta WHERE file_type='land'")
    land_var_names = sql_c.fetchall()

    #conn.commit()
    conn.execute("DROP TABLE IF EXISTS nwm_file_update_info")
    # Create a nwm file processed record table.
    sql_c.execute('CREATE TABLE nwm_file_update_info ' \
                 '(files_read text ' +
                 ', datetime integer ' + \
                 ', cycle_datetime integer ' + \
                 ', time_minus_hours integer ' + \
                 ', cycle_type integer ' + \
                 ', nwm_group text ' + \
                 ', is_reference integer)')
                 #', datetime timestamp ' + \
                 #', cycle_datetime timestamp ' + \
                 #'(id integer PRIMARY KEY AUTOINCREMENT NOT NULL,' + \
                 # 'nwm_files_read text NOT NULL)')
    conn.commit()
    check_database_table_info(sql_c, 'nwm_file_update_info')

    conn.execute("DROP TABLE IF EXISTS station_control")
    # last_update_datetime is in this control table - station_control
    sql_c.execute('CREATE TABLE station_control ' \
                 '(last_update_datetime integer)')
                 #'(last_update_datetime timestamp)')
#                 '(last_update_datetime timestamp' + \
#                ', status text DEFAULT updated)')
    conn.commit()
    check_database_table_info(sql_c, 'station_control')


    conn.execute("DROP TABLE IF EXISTS stations")
    # Create a station meta data table  - stations
    sql_c.execute('CREATE TABLE stations ' \
             '( obj_identifier integer DEFAULT ' + str(cfg.ENUM_NDV['int32 missing']) + \
             ', id text' + \
             ', name text' + \
             ', source text' + \
             ', type text' + \
             ', longitude real DEFAULT ' + str(cfg.ENUM_NDV['float64 missing']) + \
             ', latitude real DEFAULT ' + str(cfg.ENUM_NDV['float64 missing']) + \
             ', elevation integer DEFAULT ' + str(cfg.ENUM_NDV['int32 missing']) + \
             ', recorded_elevation integer DEFAULT ' + str(cfg.ENUM_NDV['int32 missing']) + \
             ', details text' + \
             ', vendor text' + \
             ', vendor_date integer' + \
             ', use integer DEFAULT ' + str(cfg.ENUM_NDV['int32 missing']) + \
             ', start_date integer' + \
             ', stop_date integer' + \
             ', added_date integer' + \
             ', nwm_grid_column real DEFAULT ' + str(cfg.ENUM_NDV['float64 missing']) + \
             ', nwm_grid_row real DEFAULT ' + str(cfg.ENUM_NDV['float64 missing']) + ')')
             #', vendor_date timestamp' + \
             #', start_date timestamp' + \
             #', stop_date timestamp' + \
             #', added_date timestamp' + \
            # ', nwm_grid_row real DEFAULT ' + str(ENUM_NDV['float64 missing']) + \
            # ', network text)')
    conn.commit()
    check_database_table_info(sql_c, 'stations')

    x_left_edge = cfg.PROJ_COORD['x_left_center'] - 0.5 * cfg.PROJ_COORD['x_resolution_meters']
    y_top_edge = cfg.PROJ_COORD['y_bottom_center'] + \
                (cfg.PROJ_COORD['number_of_rows'] - 0.5) * cfg.PROJ_COORD['y_resolution_meters']
    conn.execute("DROP TABLE IF EXISTS coordinate_system")
    # Create nwm_crs table --> coordinate_system
    sql_c.execute('CREATE TABLE coordinate_system ' + \
                  '( grid_mapping_name text DEFAULT '+ cfg.PROJ_COORD['grid_mapping_name'] + \
                  ', standard_parallel_lat1 real DEFAULT ' + str(cfg.PROJ_COORD['lat_sec_1']) + \
                  ', standard_parallel_lat2 real DEFAULT ' + str(cfg.PROJ_COORD['lat_sec_2']) + \
                  ', latitude_of_projection_origin real DEFAULT ' + str(cfg.PROJ_COORD['lat_d']) + \
                  ', longitude_of_central_meridian real DEFAULT ' + str(cfg.PROJ_COORD['lon_v']) + \
                  ', earth_radius real  DEFAULT ' + str(cfg.PROJ_COORD['earth_radius_m']) + \
                  ', false_easting real DEFAULT 0.0' + \
                  ', false_northing real DEFAULT 0.0' + \
                  ', x_resolution_meters real DEFAULT ' + \
                  str(cfg.PROJ_COORD['x_resolution_meters']) + \
                  ', y_resolution_meters real DEFAULT ' + \
                  str(cfg.PROJ_COORD['y_resolution_meters']) + \
                  ', x_left_center real DEFAULT ' + str(cfg.PROJ_COORD['x_left_center']) + \
                  ', y_bottom_center real DEFAULT ' + str(cfg.PROJ_COORD['y_bottom_center']) + \
                  ', number_of_rows integer DEFAULT ' + str(cfg.PROJ_COORD['number_of_rows']) + \
                  ', number_of_columns integer DEFAULT ' + \
                  str(cfg.PROJ_COORD['number_of_columns']) + \
                  ', GeoTransform_x_left_edge real DEFAULT ' + str(x_left_edge) + \
                  ', GeoTransform_dx real DEFAULT ' + str(cfg.PROJ_COORD['x_resolution_meters']) + \
                  ', GeoTransform_xzero real DEFAULT 0.0 ' + \
                  ', GeoTransform_y_top_edge real DEFAULT ' + str(y_top_edge) + \
                  ', GeoTransform_yzero real DEFAULT 0.0 ' + \
                  ', GeoTransform_dy real DEFAULT ' + \
                  str(-cfg.PROJ_COORD['y_resolution_meters']) + \
                  ', proj4_proj text DEFAULT ' + str(cfg.PROJ_COORD['proj4_proj']) + \
                  ', proj4_R real DEFAULT ' + str(cfg.PROJ_COORD['earth_radius_m']) + \
                  ', proj4_lon_0 real DEFAULT ' + str(cfg.PROJ_COORD['lon_v']) + \
                  ', proj4_lat_0 real DEFAULT ' + str(cfg.PROJ_COORD['lat_d']) + \
                  ', proj4_lon_1 real DEFAULT ' + str(cfg.PROJ_COORD['lat_sec_1']) + \
                  ', proj4_lat_1 real DEFAULT ' + str(cfg.PROJ_COORD['lat_sec_2']) + \
                  ', proj4_units text DEFAULT ' + str(cfg.PROJ_COORD['proj4_units']) + \
                  ', bounding_box_minimum_longitude real DEFAULT ' + str(cfg.MIN_LON) + \
                  ', bounding_box_maximum_longitude real DEFAULT ' + str(cfg.MAX_LON) + \
                  ', bounding_box_minimum_latitude real DEFAULT ' + str(cfg.MIN_LAT) + \
                  ', bounding_box_maximum_latitude real DEFAULT ' + str(cfg.MAX_LAT) + \
                  ')')

    conn.commit()
    check_database_table_info(sql_c, 'coordinate_system')

    # Some predefined parameters
    #num_ens = 1
    #num_samp = 2
    #num_snow_layers = 3
    #num_soil_layers = 4
    #num_samp = cfg.NUM_SAMPLING
    num_snow_layers = cfg.NUM_SNOW_LAYERS
    if num_snow_layers != 3:
        print('WARNING: The number of snow layers is assumed as 3')
    num_soil_layers = cfg.NUM_SOIL_LAYERS
    if num_soil_layers != 4:
        print('WARNING: The number of soil layers is assumed as 4')
    conn.execute("DROP TABLE IF EXISTS parameters")
    sql_c.execute('CREATE TABLE parameters' + \
             '(num_snow_layers integer' + \
             ', num_soil_layers integer)')
             #'( num_samp integer' + \
             #'( num_ens integer' + \
    sql_c.execute('INSERT INTO parameters VALUES (?,?)',
                  (num_snow_layers, num_soil_layers))
    conn.commit()
    check_database_table_info(sql_c, 'parameters')

    # values for cycle types
    #ext_ana = -28
    #ana = -3
    #short_range = 18
    #medium_range_ens = 240
    #long_range_ens = 720
    ext_ana_val = cfg.EXT_ANA
    ana_val = cfg.ANA
    short_range_val = cfg.SHORT_RANGE
    medium_range_ens_val = cfg.MEDIUM_RANGE_ENS
    long_range_ens_val = cfg.LONG_RANGE_ENS

    conn.execute("DROP TABLE IF EXISTS cycle_type_themes")
    sql_c.execute('CREATE TABLE cycle_type_themes' + \
             '( type text' + \
             ', value integer' + \
             ', file_str text' + \
             ', option integer)')
    sql_c.execute('INSERT INTO cycle_type_themes VALUES (?,?,?,?)',
                  ('extended analysis', ext_ana_val, cfg.EXT_ANA_STR,
                    cfg.EXT_ANA_OPT))
    sql_c.execute('INSERT INTO cycle_type_themes VALUES (?,?,?,?)',
                  ('analysis', ana_val, cfg.ANA_STR, cfg.ANA_OPT))
    sql_c.execute('INSERT INTO cycle_type_themes VALUES (?,?,?,?)',
                  ('short range', short_range_val, cfg.SHORT_RANGE_STR,
                    cfg.SHORT_RANGE_OPT))
    sql_c.execute('INSERT INTO cycle_type_themes VALUES (?,?,?,?)',
                  ('medium range ensemble', medium_range_ens_val,
                    cfg.MEDIUM_RANGE_ENS_STR, cfg.MEDIUM_RANGE_ENS_OPT))
    sql_c.execute('INSERT INTO cycle_type_themes VALUES (?,?,?,?)',
                  ('long range ensemble', long_range_ens_val,
                    cfg.LONG_RANGE_ENS_STR, cfg.LONG_RANGE_ENS_OPT))
    conn.commit()
    check_database_table_info(sql_c, 'cycle_type_themes')

    # themes table for sampling
    if cfg.SAMPLING_METHOD == 1:
        bilinear_use = 'NO'
        nearest_neighbor_use = 'YES'
    elif cfg.SAMPLING_METHOD == 2:
        bilinear_use = 'YES'
        nearest_neighbor_use = 'NO'
    else:
        print('Unrecognized Sampling Option.')
        sys.exit(1)

    #Short symbols for sampling
    bilinear_symb = 'B'
    nearest_neighbor_symb = 'N'
    conn.execute("DROP TABLE IF EXISTS sampling_themes")
    sql_c.execute('CREATE TABLE sampling_themes' + \
             '( method text' + \
             ', value text' + \
             ', details text' + \
             ', use text)')
    #if sampling_method == 1:
    #   sql_c.execute('INSERT INTO sampling_themes VALUES (?,?,?,?)',
    #                ('neighbor', 'N',
    #                 'nearest neighbor sampling', 'YES')

    sql_c.execute('INSERT INTO sampling_themes VALUES (?,?,?,?)',
                  ('bilinear', bilinear_symb,
                   'bilinear sampling', bilinear_use))
    sql_c.execute('INSERT INTO sampling_themes VALUES (?,?,?,?)',
                  ('neighbor', nearest_neighbor_symb,
                   'nearest neighbor sampling', nearest_neighbor_use))

    conn.commit()
    check_database_table_info(sql_c, 'sampling_themes')

    #Create/Add a databases_info table to hold all database names
    conn.execute("DROP TABLE IF EXISTS databases_info")
    sql_c.execute('CREATE TABLE databases_info' + \
                  '( base_db_name text' + \
                  ', forcing_single_db_name text' + \
                  ', land_single_db_name text' + \
                  ', num_days_update integer' + \
                  ', start_date integer' + \
                  ', finish_date integer' + \
                  ', last_updated_date integer' + \
                  ', nwm_archive_dir text)')
                  #', last_updated text)')
                  #', land_soil_db_name text' + \
                  #', land_snow_db_name text)')
    
    start_datetime_dt = dt.datetime.strptime(cfg.START_DATE,'%Y%m%d%H')
    start_datetime_ep = calendar.timegm(start_datetime_dt.timetuple())
    finish_datetime_dt = dt.datetime.strptime(cfg.FINISH_DATE,'%Y%m%d%H')
    finish_datetime_ep = calendar.timegm(finish_datetime_dt.timetuple())
    default_update_datetime_dt = \
        dt.datetime.strptime('1970-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
    default_update_datetime_ep = \
        calendar.timegm(default_update_datetime_dt.timetuple())
    sql_c.execute('INSERT INTO databases_info VALUES (?,?,?,?,?,?,?,?)',
                  (cfg.DATABASES['base_file'],
                   cfg.DATABASES['forcing_file'],
                   cfg.DATABASES['land_file'],
                   cfg.NUM_DAYS_TO_CURRENT_TIME,
                   start_datetime_ep,
                   finish_datetime_ep,
                   default_update_datetime_ep,
                   cfg.NWM_ARCHIVE_DIR))
                   #cfg.START_DATE,
                   #cfg.FINISH_DATE,
                   #'1970010100'))
    conn.commit()
    check_database_table_info(sql_c, 'databases_info')


    #Finished creating/checking all tables within the base database
    conn.close()

    #Four additional databases will need to be created.
    # 1. Create database for housing forcing single layer variables
    forcing_single_db = os.path.join(cfg.DATABASES['path'], cfg.DATABASES['forcing_file'])

    fpath = pathlib.Path(forcing_single_db)
    if fpath.exists():
        print('\nINFO: Dababase ', forcing_single_db, 'exists.')
        os.rename(forcing_single_db, forcing_single_db +'.bak')
        print('      It is renamed as ', sqldb_file+'.bak')
        print('INFO: New database {} will be created.'.format(forcing_single_db))
    else:
        print('INFO: Database {} will be created.'.format(forcing_single_db))

    conn = sqlite3.connect(forcing_single_db,
                           detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)

    conn.execute("DROP TABLE IF EXISTS nwm_forcing_single_layer")
    conn.execute('CREATE TABLE nwm_forcing_single_layer '+ common_cols_str)
    for var_name in forcing_var_names:
        if '_by_layer' in var_name[0]:
            print('Data with layers in forcing has not been implemented yet')
            sys.exit(1)
        else:
            conn.execute('ALTER TABLE nwm_forcing_single_layer ADD COLUMN ' +\
                           var_name[0] + ' real')
    conn.commit()
    # Display table details
    check_database_table_info(conn, 'nwm_forcing_single_layer')
    conn.close()

    # 2. Create database for housing land single layer variables
    land_single_db = os.path.join(cfg.DATABASES['path'], cfg.DATABASES['land_file'])

    fpath = pathlib.Path(land_single_db)
    if fpath.exists():
        print('\nINFO: Dababase ', land_single_db, 'exists.')
        os.rename(land_single_db, land_single_db +'.bak')
        print('      It is renamed as ', land_single_db+'.bak')
        print('INFO: New database {} will be created.'.format(land_single_db))
    else:
        print('INFO: Database {} will be created.'.format(land_single_db))
    conn = sqlite3.connect(land_single_db,
                           detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
    conn.execute("DROP TABLE IF EXISTS nwm_land_single_layer")
    conn.execute('CREATE TABLE nwm_land_single_layer '+ common_cols_str)
    for var_name in land_var_names:
        if '_by_layer' not in var_name[0]:
            conn.execute('ALTER TABLE nwm_land_single_layer ADD COLUMN ' +\
                           var_name[0] + ' real')
    conn.commit()
    # Display table details
    check_database_table_info(conn, 'nwm_land_single_layer')
    conn.close()

    #Print out warnings if SAMPLING_METHOD option is not in sync with their sampling methods
    #print(type(var_and_sampling_methods),var_and_sampling_methods)
    ##NOTE SInce the sampling method is not included in the meta table, below are now
    ## commented
    #print('\nChecking selected sampling method for each variable:')
    #if cfg.SAMPLING_METHOD == 1:
    #    print('nearest_neighbor sampling is selected.')
    #    for var, samp in var_and_sampling_methods:
    #        if 'neighbor' not in samp:
    #            print('WARNING***: Included {} has no nearest neighbor sampling method'.format(var))
    #        else:
    #            print('{} has the nearest_neighbor sampling method. Checked OK.'.format(var))
    #if cfg.SAMPLING_METHOD == 2:
    #    print('bilinear sampling is selected.')
    #    for var, samp in var_and_sampling_methods:
    #        if 'bilinear' not in samp:
    #            print('WARNING***: Included {} has no bilinear  sampling method'.format(var))
    #        else:
    #            print('{} has the bilinear sampling method. Checked OK.'.format(var))

           #for sc in range(len(samp.split(','))):
           #    print('Var name: {}, Sampling: {}'.format(var, samp.split(',')[sc]))

if __name__ == '__main__':
    main()
