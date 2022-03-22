''' run_discovery.py
    Run JACS discovery for slide codes in a file
'''

import argparse
import subprocess
import sys
import colorlog
import MySQLdb
import requests
from tqdm import tqdm

# Configuration
CONFIG = {'config': {'url': 'http://config.int.janelia.org/'}}
CONN = dict()
CURSOR = dict()

# JACS call details
PREFIX = 'action=invokeOp&name=ComputeServer%3Aservice%3DSampleDataManager' \
         + '&methodIndex=17&arg0='
SUFFIX = '&argType=java.lang.String" http://jacs-data8.int.janelia.org:8180/jmx-console/HtmlAdaptor'


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


def read_code_file():
    codefile = open(ARG.CODES, "r")
    slide_code = []
    stmt = "SELECT slide_code FROM image_data_mv WHERE workstation_sample_id=%s LIMIT 1"
    for code in codefile:
        code = code.rstrip()
        if code.isdigit():
            try:
                CURSOR['sage'].execute(stmt, (code, ))
                row = CURSOR['sage'].fetchone()
                code = row['slide_code']
            except Exception as err:
                sql_error(err)
        slide_code.append(code)
    codefile.close()
    return slide_code


def read_line_file():
    linefile = open(ARG.LINES, "r")
    slide_code = []
    stmt = "SELECT DISTINCT slide_code FROM image_data_mv WHERE line=%s"
    for line in linefile:
        line = line.rstrip()
        try:
            CURSOR['sage'].execute(stmt, (line, ))
            rows = CURSOR['sage'].fetchall()
            for row in rows:
                slide_code.append(row['slide_code'])
        except Exception as err:
            sql_error(err)
    linefile.close()
    return slide_code


def process_slide_codes():
    if ARG.LINES:
        slide_code = read_line_file()
    else:
        slide_code = read_code_file()
    sent = 0
    for code in tqdm(slide_code):
        command = 'wget -v --post-data="%s%s%s' % (PREFIX, code, SUFFIX)
        if ARG.WRITE:
            subprocess.run(command, shell=True, stdout=subprocess.DEVNULL,
                           stderr=subprocess.DEVNULL)
        else:
            print(command)
        sent += 1
    print("Slide codes processed: %d" % (sent))


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(description='Run discovery for a list of slide codes, sample IDs, or lines')
    PARSER.add_argument('--codes', dest='CODES', action='store',
                        help='File of slide codes')
    PARSER.add_argument('--lines', dest='LINES', action='store',
                        help='File of lines')
    PARSER.add_argument('--server', dest='SERVER', action='store',
                        help='Server # (2-8)')
    PARSER.add_argument('--write', action='store_true', dest='WRITE',
                        default=False, help='Send transaction to Workstation')
    PARSER.add_argument('--verbose', action='store_true', dest='VERBOSE',
                        default=False, help='Turn on verbose output')
    PARSER.add_argument('--debug', action='store_true', dest='DEBUG',
                        default=False, help='Turn on debug output')
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
    if not ARG.CODES and not ARG.LINES:
        LOGGER.error("Must specify file of slide codes or lines")
        sys.exit(-1)
    if ARG.SERVER:
        SUFFIX = SUFFIX.replace('jacs-data8', 'jacs-data' + ARG.SERVER)
    initialize_program()
    process_slide_codes()
    sys.exit(0)
