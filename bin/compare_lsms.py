''' This program will do stuff
'''

import argparse
import sys
import colorlog
import requests
import MySQLdb


# Configuration
CONFIG = {'config': {'url': 'http://config.int.janelia.org/'}}
COUNT = {'read': 0, 'match': 0, 'mismatch': 0}
CONN = dict()
CURSOR = dict()

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
    """ Initialize
    """
    global CONFIG  # pylint: disable=W0603
    data = call_responder('config', 'config/rest_services')
    CONFIG = data['config']
    data = call_responder('config', 'config/db_config')
    dbn = ARG.DATABASE
    (CONN[dbn], CURSOR[dbn]) = db_connect(data['config'][dbn][ARG.MANIFOLD])
    (CONN['sage'], CURSOR['sage']) = db_connect(data['config']['sage']['prod'])


def analyze_sample(sid, slide_code=False):
    stmt = "SELECT name,workstation_sample_id,slide_code FROM image_data_mv WHERE "
    stmt += 'slide_code' if slide_code else 'workstation_sample_id'
    stmt += "=%s ORDER BY 1"
    select = slide_code if slide_code else sid
    CURSOR['sage'].execute(stmt, (select,))
    rows = CURSOR['sage'].fetchall()
    if not rows:
        LOGGER.critical("%s was not found in SAGE", sid)
        sys.exit(1)
    response = call_responder('jacs', 'data/sample/lsms?sampleId=' + sid)
    if not response:
        LOGGER.critical("%s was not found in JACS", sid)
        sys.exit(1)
    lsms = list()
    for rec in response:
        lsm = rec['name'].replace(".bz2", "")
        lsms.append(lsm)
    okay = True
    LOGGER.info("%s: SAGE has %d LSMs, JACS has %d", sid, len(rows), len(lsms))
    for row in rows:
        lsm = row['name'].split("/")[1]
        if lsm not in lsms:
            okay = False
            LOGGER.warning("%s is in SAGE but not in JACS for %s", row['name'], sid)
    COUNT['match' if okay else 'mismatch'] += 1


def process_samples():
    if not ARG.FILE:
        LOGGER.critical("A file of sample IDs is required")
        sys.exit(-1)
    handle = open(ARG.FILE, 'r')
    for row in handle:
        COUNT['read'] += 1
        field = row.rstrip().split("\t")
        if len(field) > 1:
            analyze_sample(field[0], field[1])
        else:
            analyze_sample(field[0])
    handle.close()
    print(COUNT)


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Compare LSMs between SAGE and JACS")
    PARSER.add_argument('--database', dest='DATABASE', action='store',
                        default='gen1mcfo', help='database')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='staging', help='manifold')
    PARSER.add_argument('--file', dest='FILE', action='store',
                        default='', help='File of samle IDs')
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
    process_samples()
    sys.exit(0)
