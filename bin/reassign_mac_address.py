#!/opt/python/bin/python2.7

import argparse
import colorlog
import json
import MySQLdb
import sys
import urllib
import urllib2

# Command line parms
write = 0
# Database
SQL = {
  'MAC': "SELECT microscope,MIN(create_date),MAX(create_date),COUNT(1) FROM image_data_mv WHERE microscope IS NOT NULL AND microscope LIKE '%-%-%-%-%-%' GROUP BY 1",
  'SCOPE': "SELECT display_name FROM cv_term_vw WHERE cv='microscope' AND cv_term=%s",
  'UPDATE1': "UPDATE image_property SET value=%s WHERE type_id=getCVTermID('light_imagery','microscope',NULL) AND value=%s",
  'UPDATE2': "UPDATE image_data_mv SET microscope=%s WHERE mac_address=%s",
}
conn = dict()
cursor = dict()
# Configuration
CONFIG_FILE = '/groups/scicomp/informatics/data/rest_services.json'
CONFIG = {}


# -----------------------------------------------------------------------------
def sqlError(e):
    try:
        logger.critical('MySQL error [%d]: %s' % (e.args[0], e.args[1]))
    except IndexError:
        logger.critical('MySQL error: %s' % e)
    sys.exit(-1)


def dbConnect(db):
    logger.info("Connecting to %s on %s" % (db['name'], db['host']))
    try:
        conn = MySQLdb.connect(host=db['host'], user=db['user'],
                               passwd=db['password'], db=db['name'])
    except MySQLdb.Error as e:
        sqlError(e)
    try:
        cursor = conn.cursor()
        return(conn, cursor)
    except MySQLdb.Error as e:
        sqlError(e)


def callREST(mode, server='jacs', post=False):
    url = CONFIG[server]['url'] + mode
    req = urllib2.Request(url)
    req.add_header('Content-Type', 'application/json')
    try:
        if post:
            response = urllib2.urlopen(req, urllib.urlencode({}))
        else:
            response = urllib2.urlopen(req)
    except urllib2.HTTPError, e:
        print 'Call to %s failed: %s.' % (url, e.code)
        sys.exit(-1)
    else:
        return json.load(response)


def initializeProgram():
    global CONFIG
    json_data = open(CONFIG_FILE).read()
    CONFIG = json.loads(json_data)
    dc = callREST('database_configuration', 'sage')
    data = dc['config']
    (conn['sage'], cursor['sage']) = dbConnect(data['sage']['prod'])


def processScopes():
    db = 'sage'
    try:
        cursor[db].execute(SQL['MAC'],)
    except MySQLdb.Error as e:
        sqlError(e)
    rows = cursor[db].fetchall()
    if cursor[db].rowcount:
        for r in rows:
            logger.debug('%s: date range %s - %s, %d image(s)' % (r))
            try:
                cursor[db].execute(SQL['SCOPE'], [r[0]])
            except MySQLdb.Error as e:
                sqlError(e)
            row = cursor[db].fetchone()
            if row:
                logger.info("MAC address %s maps to microscope %s"
                             % (r[0], row[0]))
                try:
                    logger.debug(SQL['UPDATE1'] % (row[0], r[0]))
                    cursor[db].execute(SQL['UPDATE1'], (row[0], r[0]))
                    logger.info("Rows updated for %s in image_property: %d"
                                 % (r[0], cursor[db].rowcount))
                    logger.debug(SQL['UPDATE2'] % (row[0], r[0]))
                    cursor[db].execute(SQL['UPDATE2'], (row[0], r[0]))
                    logger.info("Rows updated for %s in image_data_mv: %d"
                                 % (r[0], cursor[db].rowcount))
                except MySQLdb.Error as e:
                    sqlError(e)
            else:
                logger.warning("Could not find microscope name for %s" % (r[0]))
        if write:
            conn[db].commit()
    else:
        print "All MAC addresses are mapped to microscope names"


# -----------------------------------------------------------------------------

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Find and index/discover newly tmogged imagery')
    parser.add_argument('--verbose',action='store_true',dest='VERBOSE',default=False,help='Turn on verbose output')
    parser.add_argument('--debug',action='store_true',dest='DEBUG',default=False,help='Turn on debug output')
    parser.add_argument('--write',action='store_true',dest='WRITE',default=False,help='Actually write changes to database')
    arg = parser.parse_args()

    logger = colorlog.getLogger()
    if arg.DEBUG:
        logger.setLevel(colorlog.colorlog.logging.DEBUG)
    elif arg.VERBOSE:
        logger.setLevel(colorlog.colorlog.logging.INFO)
    else:
        logger.setLevel(colorlog.colorlog.logging.WARNING)
    handler = colorlog.StreamHandler()
    handler.setFormatter(colorlog.ColoredFormatter())
    logger.addHandler(handler)

    if arg.WRITE:
        write = 1

    initializeProgram()
    processScopes()
    sys.exit(0)
