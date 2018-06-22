#!/opt/python/bin/python2.7

import argparse
import json
import sys
import colorlog
import requests
import MySQLdb

# Database
SQL = {
    'MAC': "SELECT microscope,MIN(create_date),MAX(create_date),COUNT(1) " +
           "FROM image_data_mv WHERE microscope IS NOT NULL AND microscope " +
           "LIKE '%-%-%-%-%-%' GROUP BY 1",
    'SCOPE': "SELECT display_name FROM cv_term_vw WHERE cv='microscope' " +
             "AND cv_term=%s",
    'UPDATE1': "UPDATE image_property SET value=%s WHERE type_id=" +
               "getCVTermID('light_imagery','microscope',NULL) AND value=%s",
    'UPDATE2': "UPDATE image_data_mv SET microscope=%s WHERE mac_address=%s",
}
CONN = dict()
CURSOR = dict()
# Configuration
CONFIG = {'config': {'url': 'http://config.int.janelia.org/'}}


# -----------------------------------------------------------------------------
def sql_error(err):
    """ Log a critical SQL error and exit """
    try:
        logger.critical('MySQL error [%d]: %s', err.args[0], err.args[1])
    except IndexError:
        logger.critical('MySQL error: %s', err)
    sys.exit(-1)


def db_connect(db):
    """ Connect to a database
        Keyword arguments:
        db: database dictionary
    """
    logger.info("Connecting to %s on %s", db['name'], db['host'])
    try:
        conn = MySQLdb.connect(host=db['host'], user=db['user'],
                               passwd=db['password'], db=db['name'])
    except MySQLdb.Error as e:
        sql_error(e)
    try:
        cursor = conn.cursor()
        return(conn, cursor)
    except MySQLdb.Error as e:
        sql_error(e)


def call_responder(server, endpoint):
    url = CONFIG[server]['url'] + endpoint
    try:
        req = requests.get(url)
    except requests.exceptions.RequestException as err:
        logger.critical(err)
        sys.exit(-1)
    if req.status_code == 200:
        return req.json()
    else:
        logger.error('Status: %s', str(req.status_code))
        sys.exit(-1)


def initialize_program():
    """ Initialize database """
    global CONFIG
    data = call_responder('config', 'config/rest_services')
    CONFIG = data['config']
    data = call_responder('config', 'config/db_config')
    (CONN['sage'], CURSOR['sage']) = db_connect(data['config']['sage']['prod'])


def process_scopes():
    """ Find image entries without microscope names
        and repair if possible.
    """
    db = 'sage'
    try:
        CURSOR[db].execute(SQL['MAC'],)
    except MySQLdb.Error as e:
        sql_error(e)
    rows = CURSOR[db].fetchall()
    if CURSOR[db].rowcount:
        for r in rows:
            logger.debug('%s: date range %s - %s, %d image(s)' % (r))
            try:
                CURSOR[db].execute(SQL['SCOPE'], [r[0]])
            except MySQLdb.Error as e:
                sql_error(e)
            row = CURSOR[db].fetchone()
            if row:
                logger.info("MAC address %s maps to microscope %s",
                            r[0], row[0])
                try:
                    logger.debug(SQL['UPDATE1'], row[0], r[0])
                    CURSOR[db].execute(SQL['UPDATE1'], (row[0], r[0]))
                    logger.info("Rows updated for %s in image_property: %d",
                                r[0], CURSOR[db].rowcount)
                    logger.debug(SQL['UPDATE2'], row[0], r[0])
                    CURSOR[db].execute(SQL['UPDATE2'], (row[0], r[0]))
                    logger.info("Rows updated for %s in image_data_mv: %d",
                                r[0], CURSOR[db].rowcount)
                except MySQLdb.Error as e:
                    sql_error(e)
            else:
                logger.warning(
                    "Could not find microscope name for %s", r[0])
        if ARG.WRITE:
            CONN[db].commit()
    else:
        print "All MAC addresses are mapped to microscope names"


# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description='Find and index/discover newly tmogged imagery')
    PARSER.add_argument('--verbose', action='store_true',
                        dest='VERBOSE', default=False,
                        help='Turn on verbose output')
    PARSER.add_argument('--debug', action='store_true',
                        dest='DEBUG', default=False,
                        help='Turn on debug output')
    PARSER.add_argument('--write', action='store_true', dest='WRITE',
                        default=False,
                        help='Actually write changes to database')
    ARG = PARSER.parse_args()

    logger = colorlog.getLogger()
    if ARG.DEBUG:
        logger.setLevel(colorlog.colorlog.logging.DEBUG)
    elif ARG.VERBOSE:
        logger.setLevel(colorlog.colorlog.logging.INFO)
    else:
        logger.setLevel(colorlog.colorlog.logging.WARNING)
    HANDLER = colorlog.StreamHandler()
    HANDLER.setFormatter(colorlog.ColoredFormatter())
    logger.addHandler(HANDLER)

    initialize_program()
    process_scopes()
    sys.exit(0)
