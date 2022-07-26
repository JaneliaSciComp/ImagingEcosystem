''' Read LSM names from a file, then update their path/URL from JACS
'''

import argparse
import json
import os
from os.path import exists
import pprint
import sys
import colorlog
from pymongo import MongoClient
import requests
from tqdm import tqdm
import MySQLdb

# Database
CONN = dict()
CURSOR = dict()
DBM = ""
READ = {"PRIMARY": "SELECT * FROM image WHERE path LIKE '/groups/flylight/flylight%'",
       }
# Configuration
CONFIG = {'config': {'url': os.environ.get('CONFIG_SERVER_URL')}}
# General
COUNT = {"input": 0, "mongo": 0, "not_archived": 0, "missing": 1}


def sql_error(err):
    """ Log a critical SQL error and exit """
    try:
        LOGGER.critical('MySQL error [%d]: %s', err.args[0], err.args[1])
    except IndexError:
        LOGGER.critical('MySQL error: %s', err)
    sys.exit(-1)


def db_connect(dbd):
    """ Connect to a database
        Keyword arguments:
          dbd: database dictionary
    """
    LOGGER.debug("Connecting to %s on %s", dbd['name'], dbd['host'])
    try:
        conn = MySQLdb.connect(host=dbd['host'], user=dbd['user'],
                               passwd=dbd['password'], db=dbd['name'])
    except MySQLdb.Error as err:
        sql_error(err)
    try:
        cursor = conn.cursor(MySQLdb.cursors.DictCursor)
        return(conn, cursor)
    except MySQLdb.Error as err:
        sql_error(err)


def call_responder(server, endpoint):
    """ Call a REST API
        Keyword arguments:
          server: server name
          endpoint: endpoint
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
    global CONFIG, DBM # pylint: disable=W0603
    data = call_responder('config', 'config/rest_services')
    CONFIG = data['config']
    data = call_responder('config', 'config/db_config')
    (CONN['sage'], CURSOR['sage']) = db_connect(data['config']['sage']['prod'])
    # Connect to Mongo
    try:
        client = MongoClient('mongodb3.int.janelia.org:27017')
        DBM = client.jacs
        DBM.authenticate('flyportalRead', 'flyportalRead')
    except Exception as err:
        print('Could not connect to Mongo: %s' % (err))
        sys.exit(-1)


def get_image(row):
    name = row['name'].split("/")[-1]
    try:
        mrow = DBM.image.find_one({'name': name})
    except Exception as err:
        print('Could not get sample from FlyPortal: %s' % (err))
        sys.exit(-1)
    if not mrow:
    	return
    COUNT['mongo'] += 1
    if not row['jfs_path']:
    	COUNT['not_archived'] += 1
    if row['path'] == mrow['filepath']:
    	if not exists(row['path']):
    		COUNT['missing'] += 1
    return
    print(row['path'])
    print(row['jfs_path'])
    print(mrow['filepath'])


def cleanup():
    try:
        CURSOR["sage"].execute(READ['PRIMARY'])
        rows = CURSOR["sage"].fetchall()
    except Exception as err:
        sql_error(err)
    for row in tqdm(rows):
        LOGGER.debug(row['name'])
        COUNT['input'] += 1
        if (not row['jfs_path']) or '/groups/flylight/flylight' in row['jfs_path']:
            get_image(row)
    print(COUNT)


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(description="Update ALPS release")
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Write')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = colorlog.getLogger()
    ATTR = colorlog.colorlog.logging if "colorlog" in dir(colorlog) else colorlog
    if ARG.DEBUG:
        LOGGER.setLevel(ATTR.DEBUG)
    elif ARG.VERBOSE:
        LOGGER.setLevel(ATTR.INFO)
    else:
        LOGGER.setLevel(ATTR.WARNING)
    HANDLER = colorlog.StreamHandler()
    HANDLER.setFormatter(colorlog.ColoredFormatter())
    LOGGER.addHandler(HANDLER)
    initialize_program()
    cleanup()
    sys.exit(0)