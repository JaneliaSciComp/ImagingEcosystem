import argparse
import json
import sys
import colorlog
import requests
import MySQLdb

# Database
READ = {'cimages': "SELECT cross_barcode,line,COUNT(1) FROM image_data_mv WHERE cross_barcode=%s GROUP BY 1,2",
        'limages': "SELECT NULL,line,COUNT(1) FROM image_data_mv WHERE line=%s",
        'cassays': "SELECT line,GROUP_CONCAT(DISTINCT s.type) FROM session_vw s JOIN session_property_vw sp ON (sp.session_id=s.id AND sp.type='cross_barcode') and sp.value=%s GROUP BY 1",
        'lassays': "SELECT line,sessions FROM line_summary_vw WHERE line=%s"
       }
CONN = dict()
CURSOR = dict()

# Configuration
CONFIG = {'config': {'url': 'http://config.int.janelia.org/'}}


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


def process_file(filename):
    try:
        filehandle = open(filename, "r") if filename else sys.stdin
    except Exception as e:
        logger.critical('Failed to open input: '+ str(e))
        sys.exit(-1)
    header = 1
    for line in filehandle:
        selector = line.rstrip()
        logger.debug("Read %s" % selector)
        if selector.isdigit():
            stmt = 'cimages'
            barcode = selector
            line = ''
        else:
            stmt = 'limages'
            line = selector
            barcode = ''
        try:
            CURSOR['sage'].execute(READ[stmt], (selector,))
        except MySQLdb.Error as err:
            sql_error(err)
        row = CURSOR['sage'].fetchone()
        images = ''
        if row:
            line = row[1]
            images = row[2]
        if selector.isdigit():
            stmt = 'cassays'
        else:
            stmt = 'lassays'
        try:
            CURSOR['sage'].execute(READ[stmt], (selector,))
        except MySQLdb.Error as err:
            sql_error(err)
        row = CURSOR['sage'].fetchone()
        assays = ''
        if row:
            line = row[0]
            assays = row[1]
        if images or assays:
            if header:
                print "%-10s\t%-20s\t%-6s\t%6s" % ('Cross', 'Line', 'Images', 'Assays')
                header = 0
            print "%-10s\t%-20s\t%-6s\t%6s" % (barcode, line, images, assays)
    if filehandle is not sys.stdin:
        filehandle.close()


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(description="Check lines/cross barcodes for associated imagery and behavioral data")
    PARSER.add_argument('--file', dest='FILE', action='store',
                        default='', help='File containing lines or cross barcodes')
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
    process_file(ARG.FILE)
    sys.exit(0)
