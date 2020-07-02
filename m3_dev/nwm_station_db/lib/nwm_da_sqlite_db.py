import os
import sys

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
