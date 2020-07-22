#!/opt/python/bin/python2.7

import argparse
import sys
import colorlog
import requests
import MySQLdb

# Database
READ = {'unsynced': "SELECT id.id,value FROM image_data_mv id JOIN " +
                    "image_property_vw ip ON (ip.image_id=id.id " +
                    "AND  ip.type='%s') WHERE %s IS NULL AND " +
                    "id.update_date IS NOT NULL",
        'deleted': "SELECT id,%s FROM image_data_mv WHERE id NOT IN " +
                   "(SELECT image_id FROM image_property_vw WHERE " +
                   "type='%s') AND " +
                   "%s IS NOT NULL ORDER BY 1",
       }
WRITE = {'update': "UPDATE image_data_mv SET %s='%s' WHERE id=%s",
         'refresh': "UPDATE image_property SET update_date=NOW() " +
                    "WHERE image_id=%s",
        }
CONN = dict()
CURSOR = dict()

# Configuration
CONFIG = {'config': {'url': 'http://config.int.janelia.org/'}}
COUNT = {'triggered': 0, 'update': 0}


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
    logger.debug("Connecting to %s on %s", db['name'], db['host'])
    try:
        conn = MySQLdb.connect(host=db['host'], user=db['user'],
                               passwd=db['password'], db=db['name'])
    except MySQLdb.Error as err:
        sql_error(err)
    try:
        cursor = conn.cursor()
        return(conn, cursor)
    except MySQLdb.Error as err:
        sql_error(err)


def call_responder(server, endpoint):
    """ Call a responder
        Keyword arguments:
        server: server
        endpoint: REST endpoint
    """
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
    """ Initialize databases """
    global CONFIG
    dbc = call_responder('config', 'config/db_config')
    data = dbc['config']
    (CONN['sage'], CURSOR['sage']) = db_connect(data['sage']['prod'])
    dbc = call_responder('config', 'config/rest_services')
    CONFIG = dbc['config']


def update_lineprops(imageprop):
    """ Update image_data_mv column to match image property
        Keyword arguments:
        imageprop: image property (CV term)
    """
    db = 'sage'
    j = call_responder('sage', 'cvterms?cv_term=' + imageprop)
    if len(j['cvterm_data']) < 1:
        logger.critical("Could not find line property %s", imageprop)
        sys.exit(-1)
    cursor = READ['unsynced'] % (imageprop, imageprop)
    print("Syncing image properties for %s (%s) on %s" %
          (imageprop, j['cvterm_data'][0]['display_name'], db))
    try:
        CURSOR[db].execute(cursor)
        rows = CURSOR[db].fetchall()
    except MySQLdb.Error as err:
        sql_error(err)
    for row in rows:
        image_id = row[0]
        value = row[1]
        logger.debug("Missing %s (%s) for image %s", imageprop,
                     value, image_id)
        cursor2 = WRITE['update'] % (imageprop, value, image_id)
        logger.debug(cursor2)
        try:
            CURSOR[db].execute(cursor2)
        except MySQLdb.Error as err:
            logger.error("Could not update row in image_data_mv")
            sql_error(err)
        COUNT['update'] += 1
        if ARG.TRIGGER:
            logger.debug(WRITE['refresh'] % (image_id))
            try:
                CURSOR[db].execute(WRITE['refresh'] % (image_id))
            except MySQLdb.Error as err:
                logger.error("Could not update rows in image_property")
                sql_error(err)
            COUNT['triggered'] += CURSOR[db].rowcount
    cursor = READ['deleted'] % (imageprop, imageprop, imageprop)
    try:
        CURSOR[db].execute(cursor)
        rows = CURSOR[db].fetchall()
    except MySQLdb.Error as err:
        sql_error(err)
    for row in rows:
        logger.warning("Image ID %s has a %s (%s) in image_data_mv, but not in image_property", row[0], imageprop, row[1])
    if ARG.WRITE:
        CONN[db].commit()
    print("Unsynced records: %d" % (len(rows)))
    print("Updated records: %d" % (COUNT['update']))
    print("Triggered updates: %d" % (COUNT['triggered']))


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(description="Update image properties in materialized view")
    PARSER.add_argument('--imageprop', dest='IMAGEPROP', action='store',
                        default='bits_per_sample', help='Image property (CV term) to sync')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False,
                        help='Flag, Actually modify database')
    PARSER.add_argument('--trigger', dest='TRIGGER', action='store_true',
                        default=False, help='Trigger complete sync for reported ids')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
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
    update_lineprops(ARG.IMAGEPROP)
