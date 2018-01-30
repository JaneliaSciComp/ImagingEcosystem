#!/opt/python/bin/python2.7

import argparse
import json
import sys
import colorlog
import requests
import MySQLdb

# Database
SQL = {'LINES': "SELECT l.id,l.name,lp.value FROM line l JOIN " +
                "line_property_vw lp ON (l.id=lp.line_id AND lp.type='fragment')" +
                " WHERE l.name LIKE 'BJD_1%' and l.name not in (SELECT DISTINCT name " +
                "FROM line_property_vw WHERE type='vt_line' AND name LIKE 'BJD_1%') " +
                "ORDER BY 2",
       'IMAGES': "SELECT line,i.id,value FROM image_data_mv i " +
                 "JOIN line_property_vw l ON (line=l.name AND type='vt_line') WHERE " +
                 "line LIKE 'BJD_1%' AND vt_line IS NULL ORDER BY 1,2",
       'INSERT_LP': "INSERT INTO line_property (line_id,type_id,value) " +
                    "VALUES (%s,getCvTermId('light_imagery','vt_line',''),%s)",
       'INSERT_IP': "INSERT INTO image_property (image_id,type_id,value) " +
                    "VALUES (%s,getCvTermId('light_imagery','vt_line',''),%s)",
      }
CONN = dict()
CURSOR = dict()

# Configuration
CONFIG_FILE = '/groups/scicomp/informatics/data/rest_services.json'
CONFIG = {}


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


def call_sage_responder(endpoint):
    """ Call the SAGE responder
        Keyword arguments:
        endpoint: REST endpoint
    """
    url = CONFIG['sage']['url'] + endpoint
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
    try:
        json_data = open(CONFIG_FILE).read()
        CONFIG = json.loads(json_data)
    except Exception, err:
        logger.critical(err)
        sys.exit(-1)
    dc = call_sage_responder('database_configuration')
    data = dc['config']
    (CONN['sage'], CURSOR['sage']) = db_connect(data['sage']['prod'])


def find_lines():
    COUNT = {'lf': 0, 'le': 0, 'if': 0, 'ie': 0}
    # Line properties
    logger.debug("Finding lines with missing VT lines")
    try:
        CURSOR['sage'].execute(SQL['LINES'])
    except MySQLdb.Error as err:
        sql_error(err)
    for (id, line, vt) in CURSOR['sage']:
        if vt[0:2] == 'VT':
            logger.info("%d\t%s\t%s", int(id), line, vt)
            try:
                CURSOR['sage'].execute(SQL['INSERT_LP'], [id, vt])
                COUNT['lf'] += 1
            except MySQLdb.Error, err:
                sql_error(err)
        else:
            logger.warning("%d\t%s\t%s", int(id), line, vt)
            COUNT['le'] += 1
    if ARG.WRITE:
        CONN['sage'].commit()
    # Image properties
    logger.debug("Finding images with missing VT lines")
    try:
        CURSOR['sage'].execute(SQL['IMAGES'])
    except MySQLdb.Error as err:
        sql_error(err)
    for (line, id, vt) in CURSOR['sage']:
        if vt[0:2] == 'VT':
            logger.info("%s\t%d\t%s", line, int(id), vt)
            try:
                CURSOR['sage'].execute(SQL['INSERT_IP'], [id, vt])
                COUNT['if'] += 1
            except MySQLdb.Error, err:
                sql_error(err)
        else:
            logger.warning("%s\t%d\t%s", line, int(id), vt)
            COUNT['ie'] += 1
    if ARG.WRITE:
        CONN['sage'].commit()
    print "Lines fixed: %d" % (COUNT['lf'])
    print "Lines not fixed (unknown VT): %d" % (COUNT['le'])
    print "Images fixed: %d" % (COUNT['if'])
    print "Images not fixed (unknown VT): %d" % (COUNT['ie'])


# -----------------------------------------------------------------------------


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description='Add VT line to Dickson Vienna (BJD) lines')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    PARSER.add_argument('--write', action='store_true', dest='WRITE',
                        default=False, help='Write to database')
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
    find_lines()
    sys.exit(0)
