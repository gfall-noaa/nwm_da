def extract_data_for_station__update(station_obj_id,
                             var_names,
                             oper,
                             base_db,
                             land_single_db,
                             forcing_single_db):
    '''Extract data and reference files for a specified station'''
    try:
        if os.path.isfile(base_db):
            sqldb_conn = sqlite3.connect(base_db)
        else:
            print('Database file {} does not exist.'.format(base_db))
            sys.exit(1)
        #sqldb_cur = sqldb_conn.cursor()
    except sqlite3.OperationalError:
        print('ERROR: Failed to open database file "{}".'.format(base_db),
              file=sys.stderr)
        sys.exit(1)

    try:
        if os.path.isfile(land_single_db):
            sqldb_conn.execute('ATTACH DATABASE "' + land_single_db + '" AS land_single')
    except:
        print('Database file {} does not exist.'.format(land_single_db))
        sys.exit(1)

    try:
        if os.path.isfile(forcing_single_db):
            sqldb_conn.execute('ATTACH DATABASE "' + forcing_single_db + '" AS forcing_single')
    except:
        print('Database file {} does not exist.'.format(forcing_single_db))
        sys.exit(1)

    db_dir, db_file = os.path.split(base_db)

    username=getpass.getuser()
    file_select_str = "SELECT files_read, datetime, cycle_datetime, " + \
                      "cycle_type FROM nwm_file_update_info" + \
                      " WHERE nwm_group='land' and is_reference=1"
    files = sqldb_conn.execute(file_select_str).fetchall()
    files_df = pd.DataFrame(files, columns=['files_read','datetime', \
                                            'cycle_datetime','cycle_type'])
    if oper:
        file_df_name = 'reference_files_oper_' + username + '.csv'
    else:
        file_df_name = 'reference_files_archive_' + username + '.csv'
    file_df_name = os.path.join(db_dir, file_df_name)
    files_df['datetime_str'] = \
        change_datetime_to_readable_string(files_df['datetime'],
                                           'datetime')
    files_df['cycle_datetime_str'] = \
        change_datetime_to_readable_string(files_df['cycle_datetime'],
                                           'cycle_datetime')
    files_df = files_df.reindex(columns=['files_read',
                                         'datetime',
                                         'datetime_str',
                                         'cycle_datetime',
                                         'cycle_datetime_str',
                                         'cycle_type'])
    files_df.to_csv(file_df_name, encoding='utf-8', index=False)

    for var_name in var_names:
        if var_name[0] == 'land':
            data_select_str = "SELECT datetime, cycle_datetime, " + \
                              "cycle_type, " + var_name[1] + \
                              " FROM land_single.nwm_land_single_layer WHERE " + \
                              "station_obj_identifier=" + str(station_obj_id)
        elif var_name[0] == 'forcing':
            data_select_str = "SELECT datetime, cycle_datetime, " + \
                              "cycle_type, " + var_name[1] + \
                              " FROM forcing_single.nwm_forcing_single_layer WHERE " + \
                              "station_obj_identifier=" + str(station_obj_id)
        else:
            print('Wrong data type given: {}.'.format(var_name[0]))
            sys.exit(1)


        data = sqldb_conn.execute(data_select_str).fetchall()
        data_df = pd.DataFrame(data, columns=['datetime', 'cycle_datetime', \
                                              'cycle_type', var_name[1]])
                                              #'cycle_type', 'values'])
        if len(data_df) == 0:
            print('ERROR: No data extracted, check station ID.')
            sys.exit(1)

        #Add time_minus_hours column to the data_df
        minus_hours = (data_df['cycle_datetime'] - data_df['datetime'])/3600
        data_df['time_minus_hours'] = minus_hours

        #Convert time in epoch seconds to readable string
        #and add to the dataframe
        data_df['datetime_str'] = \
            change_datetime_to_readable_string(data_df['datetime'],
                                               'datetime')
        data_df['cycle_datetime_str'] = \
            change_datetime_to_readable_string(data_df['cycle_datetime'],
                                           'cycle_datetime')

        #write data to files
        if oper:
            data_df_name = var_name[1] + '_obj_id' + str(station_obj_id) + \
                           '_oper_' + username + '.csv'
        else:
            data_df_name = var_name[1] + '_obj_id' + str(station_obj_id) + \
                           '_archive_' + username + '.csv'
        data_df_name = os.path.join(db_dir, data_df_name)
        data_df = data_df.reindex(columns=['datetime',
                                           'datetime_str',
                                           'cycle_datetime',
                                           'cycle_datetime_str',
                                           'time_minus_hours',
                                           var_name[1]])

        data_df.to_csv(data_df_name, encoding='utf-8', index=False)

        try:
            data_df.plot.scatter(x='datetime', y=var_name[1])
            plt.show()
        except:
            print('WARNING: Plot was not successful!')

    sqldb_conn.close()


def plot_data__update(df, y_col_name):
    '''Plot one column of data from a dataframe'''
    df.plot(x='datetime', y=y_col_name, kind='scatter')


def extract_data_for_station__preprocessor(sqldb_conn,
                             oper,
                             station_obj_id,
                             var_names,
                             base_db,
                             land_single_db,
                             forcing_single_db):
    '''Extract data and reference files for a specified station'''

    db_dir, db_file = os.path.split(base_db)
    #print(db_dir, db_file)

    username = getpass.getuser()
    file_select_str = "SELECT files_read, datetime, cycle_datetime, " + \
                      "cycle_type FROM nwm_file_update_info" + \
                      " WHERE nwm_group='land' and is_reference=1"
    files = sqldb_conn.execute(file_select_str).fetchall()
    files_df = pd.DataFrame(files, columns=['files_read', 'datetime', \
                                            'cycle_datetime', 'cycle_type'])
    if oper:
        file_df_name = 'reference_files_oper_' + username + '.csv'
    else:
        file_df_name = 'reference_files_archive_' + username + '.csv'

    files_df['datetime_str'] = \
        change_datetime_to_readable_string(files_df['datetime'],
                                           'datetime')
    files_df['cycle_datetime_str'] = \
        change_datetime_to_readable_string(files_df['cycle_datetime'],
                                           'cycle_datetime')
    file_df_name = os.path.join(db_dir, file_df_name)

    files_df = files_df.reindex(columns=['files_read',
                                         'datetime',
                                         'datetime_str',
                                         'cycle_datetime',
                                         'cycle_datetime_str',
                                         'cycle_type'])
    files_df.to_csv(file_df_name, encoding='utf-8', index=False)

    for var_name in var_names:
        if var_name[0] == 'land':
            data_select_str = "SELECT datetime, cycle_datetime, " + \
                              "cycle_type, " + var_name[1] + \
                              " FROM land_single.nwm_land_single_layer WHERE " + \
                              "station_obj_identifier=" + str(station_obj_id)
        elif var_name[0] == 'forcing':
            data_select_str = "SELECT datetime, cycle_datetime, " + \
                              "cycle_type, " + var_name[1] + \
                              " FROM forcing_single.nwm_forcing_single_layer WHERE " + \
                              "station_obj_identifier=" + str(station_obj_id)
        else:
            print('Wrong data type given: {}.'.format(var_name[0]))
            sys.exit(1)


        data = sqldb_conn.execute(data_select_str).fetchall()
        data_df = pd.DataFrame(data, columns=['datetime', 'cycle_datetime', \
                                              'cycle_type', var_name[1]])
        if len(data_df) == 0:
            print('ERROR: No data extracted, check station ID.')
            sys.exit(1)
        #Add time_minus_hours column to the data_df
        minus_hours = (data_df['cycle_datetime'] - data_df['datetime'])/3600
        data_df['time_minus_hours'] = minus_hours

        #Convert time in epoch seconds to readable string
        #and add to the dataframe
        data_df['datetime_str'] = \
            change_datetime_to_readable_string(data_df['datetime'],
                                               'datetime')
        data_df['cycle_datetime_str'] = \
            change_datetime_to_readable_string(data_df['cycle_datetime'],
                                               'cycle_datetime')
        #write data to files
        if oper:
            data_df_name = var_name[1] + '_obj_id' + str(station_obj_id) + \
                           '_oper_' + username + '.csv'
        else:
            data_df_name = var_name[1] + '_obj_id' + str(station_obj_id) + \
                           '_archive_' + username + '.csv'
        data_df_name = os.path.join(db_dir, data_df_name)
        data_df = data_df.reindex(columns=['datetime',
                                           'datetime_str',
                                           'cycle_datetime',
                                           'cycle_datetime_str',
                                           'time_minus_hours',
                                           var_name[1]])
        data_df.to_csv(data_df_name, encoding='utf-8', index=False)

        try:
            data_df.plot.scatter(x='datetime', y=var_name[1])
            #plot_data(data_df, var_name[1])
            plt.show()
            plt.savefig(var_name[1] + '.png')
        except:
            print('Plot was not successful!')

def plot_data__preprocessor(df, y_col_name):
    '''Plot one column of data from a dataframe'''
    df.plot(x='datetime', y=y_col_name, kind='scatter')



