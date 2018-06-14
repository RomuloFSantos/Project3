import sqlite3


def connect_db(db_name, logger):

    try:
        conn = sqlite3.connect(db_name + '.db')
        logger.info(f'Connection stablished with DB: {db_name}.db')

        return conn

    except sqlite3.OperationalError:

        logger.error(f'Unable to connect with {db_name}.db. Please, check if the DB name is correct.')


def create_table(conn, logger):

    c = conn.cursor()

    try:
        c.execute('CREATE TABLE IF NOT EXISTS Chip_seq( '
                  'cell_type_category TEXT NOT NULL,  '
                  'cell_type TEXT NOT NULL, '
                  'cell_type_track_name TEXT NOT NULL, '
                  'cell_type_short TEXT NOT NULL, '
                  'assay_category TEXT NOT NULL, '
                  'assay TEXT NOT NULL, '
                  'assay_track_name TEXT NOT NULL, '
                  'assay_short TEXT NOT NULL, '
                  'donor TEXT NOT NULL, '
                  'time_point TEXT NOT NULL, '
                  'view TEXT NOT NULL, '
                  'track_name TEXT NOT NULL, '
                  'track_type TEXT NOT NULL, '
                  'track_density TEXT NOT NULL, '
                  'provider_institution TEXT NOT NULL, '
                  'source_server TEXT NOT NULL, '
                  'source_path_to_file TEXT NOT NULL, '
                  'server TEXT NOT NULL, '
                  'path_to_file TEXT NOT NULL, '
                  'new_file_name TEXT NOT NULL);')

        logger.info('Table Chip_seq was created')

    except sqlite3.OperationalError:
        logger.error('Table Chip_seq could not be created')


def insert_data(conn, list_of_data, logger):

    c = conn.cursor()

    try:
        with conn:

            for data in list_of_data:
                c.execute('INSERT INTO Chip_seq VALUES'
                          '(:cell_type_category, '
                          ':cell_type, '
                          ':cell_type_track_name, '
                          ':cell_type_short, '
                          ':assay_category, '
                          ':assay, '
                          ':assay_track_name, '
                          ':assay_short, '
                          ':donor, '
                          ':time_point, '
                          ':view, '
                          ':track_name, '
                          ':track_type, '
                          'track_density, '
                          ':provider_institution, '
                          ':source_server,'
                          ':source_path_to_file,'
                          ':server,'
                          ':path_to_file,'
                          ':new_file_name)', data)

            logger.info('Data was inserted on the DB')

    except sqlite3.OperationalError:
        logger.error('Data could not be inserted on the DB')


def select_cell_types(conn, logger):

    c = conn.cursor()

    try:
        with conn:
            c.execute("SELECT cell_type FROM Chip_seq")
            all_cell_types = c.fetchall()

            logger.info(f'Selected cell_types')
            return all_cell_types

    except sqlite3.OperationalError:
        logger.error(f'Could not Select cell_types. Check if the tables exists.')


def select_tracks_assay(conn, assay, logger):

    c = conn.cursor()

    try:
        with conn:
            c.execute("SELECT track_name, track_type, track_density FROM Chip_seq WHERE assay = :assay", {"assay": assay})
            all_tracks_assay = c.fetchall()
            print(all_tracks_assay)

            logger.info(f'Selected tracks from assay')

            return all_tracks_assay

    except sqlite3.OperationalError:
        logger.error(f'Could not Select tracks. Check if the table exists.')


def select_track_name(conn, assay_track_name, logger):

    c = conn.cursor()

    try:
        with conn:
            c.execute("SELECT track_name FROM Chip_seq WHERE assay_track_name = :assay_track_name", {'assay_track_name': assay_track_name})
            all_track_name = c.fetchall()

            logger.info(f'Selected track_name with assay_track_name: {assay_track_name}')

            return all_track_name

    except sqlite3.OperationalError:
        logger.error(f'Could not select track_name')


def select_assay_cell_type(conn, assay, logger):
    c = conn.cursor()

    try:
        with conn:
            c.execute("SELECT cell_type FROM chipSeq WHERE assay = :assay", {"assay": assay})
            all_cell_type = c.fetchall()

            logger.info(f'Selected cell_type with assay: {assay}')

            return all_cell_type

    except sqlite3.OperationalError:
        logger.error(f'Could not Select cell_type.')


def update_assay(conn, assay, assay_new, logger):

    c = conn.cursor()

    try:
        with conn:
            c.execute("UPDATE Chip_seq SET assay = :assay_new  WHERE assay = :assay", {'assay': assay, 'assay_new': assay_new})
            logger.info(f'assay:{assay} was updated for assay_new: {assay_new}')

    except sqlite3.OperationalError:
        logger.error(f'COLD NOT UPDATE assay:{assay} for assay_new: {assay_new}')


def update_donor(conn, donor, donor_new, logger):

    c = conn.cursor()

    try:
        with conn:
            c.execute("UPDATE Chip_seq SET donor = :donor_new  WHERE donor = :donor", {'donor': donor, 'donor_new': donor_new})
            logger.info(f'donor:{donor} was updated for donor_new: {donor_new}')

    except sqlite3.OperationalError:
        logger.error(f'COLD NOT UPDATE assay:{donor} for assay_new: {donor_new}')


def delete_track_name(conn, track_name, logger):
    c = conn.cursor()

    try:
        with conn:
            c.execute("DELETE FROM Chip_seq WHERE track_name = :track_name", {"track_name": track_name})

            logger.info(f'Rows where track_name is: "{track_name}",  were deleted')

    except sqlite3.OperationalError:
        logger.error(f'Could not delete {track_name}')