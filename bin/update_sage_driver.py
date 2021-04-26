''' This program will update image drivers on SAGE
'''

import argparse
from os import path, remove
import sys
from time import strftime
import colorlog
import requests
import MySQLdb
from tqdm import tqdm


# Configuration
CONFIG = {'config': {'url': 'http://config.int.janelia.org/'}}
COUNT = {'images': 0, 'inserted': 0, 'skipped': 0, 'updated': 0}
# Database
CONN = dict()
CURSOR = dict()
READ = {"CURRENT": "SELECT id,value FROM image_property_vw WHERE type='driver' AND image_id=%s",
        "DRIVER": "SELECT value FROM line_property_vw WHERE type='flycore_project' AND name=%s",
        "IMAGES": "SELECT id,line FROM image_data_mv WHERE data_set IS NOT NULL and driver IS NULL"
       }
WRITE = {"INSERT": "INSERT INTO image_property (image_id,type_id,value) VALUES "
                   + "(%s,getCvTermId('light_imagery','driver',''),%s)",
         "UPDATE": "UPDATE image_property SET value=%s WHERE id=%s",
         "UPDATEMV": "UPDATE image_data_mv SET driver=%s WHERE id=%s"
        }

# pylint: disable=W0703

# -----------------------------------------------------------------------------

def sql_error(err):
    """ Log a critical SQL error and exit
        Keyword arguments:
          err: error object
    """
    try:
        LOGGER.critical('MySQL error [%d]: %s', err.args[0], err.args[1])
    except IndexError:
        LOGGER.critical('MySQL error: %s', err)
    sys.exit(-1)


def db_connect(dbd):
    """ Connect to a database
        Keyword arguments:
          dbd: database dictionary
        Returns:
          connector and cursor
    """
    LOGGER.info("Connecting to %s on %s", dbd['name'], dbd['host'])
    try:
        conn = MySQLdb.connect(host=dbd['host'], user=dbd['user'],
                               passwd=dbd['password'], db=dbd['name'])
    except MySQLdb.Error as err:
        sql_error(err)
    try:
        cursor = conn.cursor(MySQLdb.cursors.DictCursor)
        return conn, cursor
    except MySQLdb.Error as err:
        sql_error(err)


def call_responder(server, endpoint):
    """ Call a responder and return JSON
        Keyword arguments:
          server: server
          endpoint: endpoint
        Returns:
          JSON
    """
    url = CONFIG[server]['url'] + endpoint
    try:
        req = requests.get(url)
    except requests.exceptions.RequestException as err:
        LOGGER.critical(err)
        sys.exit(-1)
    if req.status_code == 200:
        return req.json()
    LOGGER.error('Status: %s', str(req.status_code))
    sys.exit(-1)


def initialize_program():
    """ Initialize program
    """
    global CONFIG  # pylint: disable=W0603
    data = call_responder('config', 'config/rest_services')
    CONFIG = data['config']
    data = call_responder('config', 'config/db_config')
    (CONN['sage'], CURSOR['sage']) = db_connect(data['config']['sage']['prod'])


def update_driver(iid, driver):
    """ Update driver in image_property and image_data_mv
        Keyword arguments:
          iid: image ID
          driver: driver
        Returns:
          None
    """
    try:
        CURSOR['sage'].execute(READ['CURRENT'], (iid,))
        row = CURSOR['sage'].fetchone()
    except Exception as err:
        sql_error(err)
    if row:
        if row['value'] != driver:
            if ARG.WRITE:
                try:
                    CURSOR['sage'].execute(WRITE['UPDATE'], (driver, iid))
                except Exception as err:
                    sql_error(err)
            CHANGES.write("Changed driver from %s to %s for image ID %s\n" \
                          % (row['value'], driver, iid))
            COUNT['updated'] += 1
    else:
        if ARG.WRITE:
            try:
                CURSOR['sage'].execute(WRITE['INSERT'], (iid, driver))
            except Exception as err:
                sql_error(err)
        CHANGES.write("Added driver %s for image ID %s\n" % (driver, iid))
        COUNT['inserted'] += 1
    if ARG.WRITE:
        try:
            CURSOR['sage'].execute(WRITE['UPDATEMV'], (driver, iid))
        except Exception as err:
            sql_error(err)


def process_images():
    """ Update driver for images in SAGE
        Keyword arguments:
          None
        Returns:
          None
    """
    try:
        CURSOR['sage'].execute(READ['IMAGES'])
        rows = CURSOR['sage'].fetchall()
    except Exception as err:
        sql_error(err)
    driver = dict()
    LOGGER.info("Images missing drivers: %d", len(rows))
    for row in tqdm(rows):
        COUNT['images'] += 1
        if row['line'] not in driver:
            try:
                CURSOR['sage'].execute(READ['DRIVER'], (row['line'],))
                drow = CURSOR['sage'].fetchone()
            except Exception as err:
                sql_error(err)
            if not drow:
                LOGGER.warning("No flycore_project for %s", row['line'])
                COUNT['skipped'] += 1
                driver[row['line']] = None
                continue
            driver[row['line']] = drow['value']
        if driver[row['line']]:
            update_driver(row['id'], driver[row['line']])
        else:
            COUNT['skipped'] += 1
    if ARG.WRITE:
        CONN['sage'].commit()
    print("Images found:     %d" % (COUNT['images']))
    print("Drivers inserted: %d" % (COUNT['inserted']))
    print("Drivers updated:  %d" % (COUNT['updated']))
    print("Images skipped:   %d" % (COUNT['skipped']))
    if CHANGES:
        CHANGES.close()
        if path.getsize(CHANGE_FILE) < 1:
            remove(CHANGE_FILE)


# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Update driver image property on SAGE")
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Actually write to Mongo')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()

    LOGGER = colorlog.getLogger()
    if ARG.DEBUG:
        LOGGER.setLevel(colorlog.colorlog.logging.DEBUG)
    elif ARG.VERBOSE:
        LOGGER.setLevel(colorlog.colorlog.logging.INFO)
    else:
        LOGGER.setLevel(colorlog.colorlog.logging.WARNING)
    HANDLER = colorlog.StreamHandler()
    HANDLER.setFormatter(colorlog.ColoredFormatter())
    LOGGER.addHandler(HANDLER)

    initialize_program()
    TIMESTAMP = strftime("%Y%m%dT%H%M%S")
    CHANGE_FILE = 'driver_changes_%s.txt' % (TIMESTAMP)
    CHANGES = open(CHANGE_FILE, 'w')
    process_images()
    sys.exit(0)
