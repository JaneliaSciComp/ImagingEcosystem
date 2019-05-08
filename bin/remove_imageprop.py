#!/opt/python/bin/python2.7

import argparse
import sys
import colorlog
import requests
import MySQLdb

# Database
READ = {'image': "SELECT id,name FROM image WHERE name LIKE '%%%s%%'"
       }
WRITE = {'imageprop': "DELETE FROM image_property WHERE image_id=%s AND type_id=%s",
         'imagedatamv': "UPDATE image_data_mv SET %s=NULL WHERE id=%s",
        }
CONN = dict()
CURSOR = dict()

# Configuration
CONFIG = {'config': {'url': 'http://config.int.janelia.org/'}}
COUNT = {'imageprop': 0, 'imagedatamv': 0, 'multiple': 0, 'notfound': 0, 'read': 0}


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
    typeid = str(j['cvterm_data'][0]['id'])
    input = open(ARG.FILE, "r")
    for line in input:
        line = line.strip()
        iname = line.split('_')[-1]
        logger.info(iname)
        cursor = READ['image'] % (iname)
        COUNT['read'] += 1
        try:
            CURSOR[db].execute(cursor)
        except MySQLdb.Error as err:
            sql_error(err)
        rows = CURSOR[db].fetchall()
        if len(rows) > 1:
            logger.error("Non-unique UID %s for image %s", iname, line)
            COUNT['multiple'] += 1
            continue
        elif len(rows) == 0:
            logger.error("No image found for UID %s image %s", iname, line)
            COUNT['notfound'] += 1
            continue
        for row in rows:
            image_id = row[0]
            cursor2 = WRITE['imageprop'] % (image_id, typeid)
            logger.debug(cursor2)
            try:
                CURSOR[db].execute(cursor2)
                COUNT['imageprop'] += 1
            except MySQLdb.Error as err:
                logger.error("Could not update row in image_data_mv")
                sql_error(err)
            cursor2 = WRITE['imagedatamv'] % (ARG.IMAGEPROP, image_id)
            logger.debug(cursor2)
            try:
                CURSOR[db].execute(cursor2)
                COUNT['imagedatamv'] += 1
            except MySQLdb.Error as err:
                logger.error("Could not update row in image_data_mv")
                sql_error(err)
    if ARG.WRITE:
        CONN[db].commit()
    print("Images read: %d" % (COUNT['read']))
    print("Images not found: %d" % (COUNT['notfound']))
    print("Images with nonspecific UIDs: %d" % (COUNT['multiple']))
    if ARG.WRITE or ARG.DEBUG:
        print("Rows deleted from image_property: %d" % (COUNT['imageprop']))
        print("Rows updated in image_data_mv: %d" % (COUNT['imagedatamv']))


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(description="Delete image properties from a set of images")
    PARSER.add_argument('--imageprop', dest='IMAGEPROP', action='store',
                        default='data_set', help='Image property (CV term) to delete')
    PARSER.add_argument('--file', dest='FILE', action='store',
                        help='File containing image names')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False,
                        help='Flag, Actually modify database')
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

    if not ARG.FILE:
        logger.critical("You must specify an input file")
        sys.exit(-1)

    initialize_program()
    update_lineprops(ARG.IMAGEPROP)
