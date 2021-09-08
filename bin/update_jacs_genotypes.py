''' This program will update the genotype on JACS to match SAGE
'''

import argparse
import subprocess
import sys
import colorlog
import requests
import MySQLdb
from pymongo import MongoClient
from tqdm import tqdm

# Configuration
CONFIG = {'config': {'url': 'http://config.int.janelia.org/'}}
# Database
CONN = dict()
CURSOR = dict()
DBM = ''
READ = {"GENOTYPES": "SELECT name AS line FROM line l JOIN publishing_name p ON "
                     + "(l.id=line_id) WHERE display_genotype=1"
       }
# JACS call details
PREFIX = "action=invokeOp&name=ComputeServer%3Aservice%3DSampleDataManager" \
         + "&methodIndex=17&arg0="
SUFFIX = '&argType=java.lang.String" http://jacs-data8.int.janelia.org:8180/' \
         + 'jmx-console/HtmlAdaptor'

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
    global CONFIG, DBM  # pylint: disable=W0603
    data = call_responder('config', 'config/rest_services')
    CONFIG = data['config']
    data = call_responder('config', 'config/db_config')
    (CONN['sage'], CURSOR['sage']) = db_connect(data['config']['sage']['prod'])
    # Connect to Mongo
    rwp = 'read'
    try:
        client = MongoClient(data['config']['jacs-mongo'][ARG.MANIFOLD][rwp]['host'])
        DBM = client.jacs
        if ARG.MANIFOLD != 'dev':
            DBM.authenticate(data['config']['jacs-mongo'][ARG.MANIFOLD][rwp]['user'],
                             data['config']['jacs-mongo'][ARG.MANIFOLD][rwp]['password'])
    except Exception as err:
        LOGGER.error('Could not connect to Mongo: %s', err)
        sys.exit(-1)


def process_line(line, codes):
    """ Find slide codes for a given line
        Keyword arguments:
          line: line name
          codes: list of slide codes
        Returns:
          None
    """
    payload = {'line': line, 'sageSynced': True}
    try:
        cursor = DBM.sample.find(payload)
    except Exception as err:
        LOGGER.error('Could not get samples from FlyPortal: %s', err)
        sys.exit(-1)
    if not cursor:
        LOGGER.error("Line %s was not found in JACS", line)
        return
    for dset in cursor:
        codes.append(str(dset["slideCode"]))


def process_genotypes():
    """ Update genotypes for lines on JACS
        Keyword arguments:
          None
        Returns:
          None
    """
    try:
        CURSOR['sage'].execute(READ['GENOTYPES'])
        rows = CURSOR['sage'].fetchall()
    except Exception as err:
        sql_error(err)
    LOGGER.info("Lines to check: %d", len(rows))
    codes = list()
    for row in tqdm(rows):
        process_line(row["line"], codes)
        break
    LOGGER.info("Slide codes to update: %d", len(codes))
    sent = 0
    for code in tqdm(codes):
        command = 'wget -v --post-data="%s%s%s' % (PREFIX, code, SUFFIX)
        if ARG.WRITE:
            subprocess.run(command, shell=True, check=True, stdout=subprocess.DEVNULL,
                           stderr=subprocess.DEVNULL)
        else:
            print(command)
        sent += 1
    print("Slide codes processed: %d" % (sent))

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Update line genotypes on JACS")
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        choices=['dev', 'prod'], default='prod', help='Manifold')
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
    process_genotypes()
    sys.exit(0)
