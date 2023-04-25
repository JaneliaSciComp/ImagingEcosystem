''' This program sync lab project and member for a given release in SAGE and GEN1MCFO
'''

import argparse
from os import remove
import sys
import colorlog
import requests
import MySQLdb
from tqdm import tqdm


# pylint: disable=no-member
# Configuration
CONFIG = {'config': {'url': 'http://config.int.janelia.org/'}}
# Database
CONN = dict()
CURSOR = dict()
READ = {"EFFECTORS": "SELECT cv_term,definition FROM cv_term_vw WHERE cv='effector' "
                     + "AND definition IS NOT NULL",
        "SLIDE_CODES": "SELECT DISTINCT slide_code FROM image_data_mv WHERE "
                       + "alps_release=%s AND cross_barcode IS NOT NULL AND "
                       + "lab_member IS NULL ORDER BY id",
        "IDS": "SELECT id,cross_barcode,effector,slide_code FROM image_data_mv WHERE "
               + "slide_code=%s AND lab_member IS NULL ORDER BY id",
        "IMAGE": "SELECT id FROM image WHERE id=%s",
        "PROPERTY": "SELECT value FROM image_property WHERE image_id=%s AND "
                    + "type_id=getCVTermID('fly',%s,NULL)"
       }
WRITE = {"PROPERTY": "INSERT INTO image_property (image_id,type_id,value) VALUES "
                     + "(%s,getCVTermID('fly',%s,NULL),%s)",
         "MVlab_member": "UPDATE image_data_mv SET lab_member=%s WHERE id=%s",
         "MVlab_project": "UPDATE image_data_mv SET lab_project=%s WHERE id=%s",
         "MVcross_description": "UPDATE image_data_mv SET cross_description=%s WHERE id=%s",
         "MVeffector_description": "UPDATE image_data_mv SET effector_description=%s WHERE id=%s"
        }
# General
IMAGE_PROPS = ['lab_member', 'lab_project', 'cross_description', 'effector_description']
COUNT = {'no cross data': 0, 'read': 0, 'updated': 0, 'rows_updated': 0}


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


def db_connect(dbd, manifold):
    """ Connect to a database
        Keyword arguments:
          dbd: database dictionary
          manifold: manifold
        Returns:
          connector and cursor
    """
    LOGGER.info("Connecting to %s on %s (%s)", dbd['name'], dbd['host'], manifold)
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
    LOGGER.debug(url)
    try:
        req = requests.get(url)
    except requests.exceptions.RequestException as err:
        LOGGER.critical(err)
        sys.exit(-1)
    if req.status_code == 200:
        try:
            return req.json()
        except requests.exceptions.RequestException as err:
            LOGGER.error(err)
            sys.exit(-1)
        except Exception as err: #pylint: disable=W0703
            temp = "An exception of type %s occurred. Arguments:\n%s"
            LOGGER.error(temp, type(err).__name__, err.args)
            sys.exit(-1)
    LOGGER.error('Status: %s', str(req.status_code))
    sys.exit(-1)


def initialize_program():
    """ Initialize
    """
    global CONFIG # pylint: disable=W0603
    data = call_responder('config', 'config/rest_services')
    CONFIG = data['config']
    data = call_responder('config', 'config/db_config')
    dbn = 'sage'
    (CONN[dbn], CURSOR[dbn]) = db_connect(data['config'][dbn]['prod'], 'prod')
    dbn = ARG.DATABASE
    (CONN[dbn], CURSOR[dbn]) = db_connect(data['config'][dbn][ARG.MANIFOLD], ARG.MANIFOLD)


def process_row(row, cross):
    """ Process a single row (image ID)
        Keyword arguments:
          row: row from image_data_mv
          cross: cross data
        Returns:
          NONE
    """
    COUNT['updated'] += 1
    for prop in IMAGE_PROPS:
        if prop not in cross[row['cross_barcode']] or not cross[row['cross_barcode']][prop]:
            continue
        for dbn in ['sage', ARG.DATABASE]:
        #for dbn in ['sage']:
            if dbn != 'sage':
                try:
                    CURSOR[dbn].execute(READ['IMAGE'], (row['id'],))
                    imgtest = CURSOR[dbn].fetchone()
                    if not imgtest:
                        continue
                except MySQLdb.Error as err:
                    sql_error(err)
            try:
                CURSOR[dbn].execute(READ['PROPERTY'], (row['id'], prop))
                proptest = CURSOR[dbn].fetchall()
                if proptest:
                    continue
                LOGGER.debug("Updating %s for %s to %s", prop, dbn,
                             cross[row['cross_barcode']][prop])
                if not ARG.WRITE:
                    COUNT['rows_updated'] += 1
                    continue
                CURSOR[dbn].execute(WRITE['PROPERTY'], (row['id'], prop,
                                                        cross[row['cross_barcode']][prop]))
            except MySQLdb.Error as err:
                sql_error(err)
            COUNT['rows_updated'] += 1
            try:
                CURSOR[dbn].execute(WRITE['MV'+prop], (cross[row['cross_barcode']][prop],
                                                       row['id']))
            except MySQLdb.Error as err:
                sql_error(err)


def get_slide_codes():
    """ Process a single row (image ID)
        Keyword arguments:
          NONE
        Returns:
          List of slide codes
    """
    slide_code = []
    if ARG.SLIDE_CODE:
        slide_code.append(ARG.SLIDE_CODE)
    else:
        print("Selecting slide codes for release %s" % (ARG.RELEASE))
        try:
            CURSOR['sage'].execute(READ['SLIDE_CODES'], (ARG.RELEASE,))
            rows = CURSOR['sage'].fetchall()
        except MySQLdb.Error as err:
            sql_error(err)
        for scode in rows:
            slide_code.append(scode['slide_code'])
        print("Adding %d slide_codes to selection list" % (len(slide_code)))
    codes = []
    for scode in tqdm(slide_code):
        try:
            CURSOR['sage'].execute(READ['IDS'], (scode,))
            rows = CURSOR['sage'].fetchall()
            for row in rows:
                if not row['cross_barcode']:
                    LOGGER.error("Slide code %s image ID %s has no cross barcode",
                                 scode, row['id'])
                else:
                    codes.append(row)
        except MySQLdb.Error as err:
            sql_error(err)
    return codes


def complete_processing(slide_code):
    """ Process a single row (image ID)
        Keyword arguments:
          slide_code: list of slide codes
        Returns:
          NONE
    """
    if ARG.WRITE:
        CONN['sage'].commit()
        CONN[ARG.DATABASE].commit()
    if slide_code:
        for scode in slide_code:
            OUTPUT.write("%s\n" % (scode))
        OUTPUT.close()
    else:
        OUTPUT.close()
        remove(OUTPUT_FILE)
    print(COUNT)


def update_imageprops():
    """ Get info from FlyCore and update databases
    """
    LOGGER.info("Getting release information from: %s", ARG.DATABASE)
    description = dict()
    if 'effector_description' in IMAGE_PROPS:
        try:
            CURSOR['sage'].execute(READ['EFFECTORS'])
            rows = CURSOR['sage'].fetchall()
        except MySQLdb.Error as err:
            sql_error(err)
        for row in rows:
            description[row['cv_term']] = row['definition']
    rows = get_slide_codes()
    cross = dict()
    if rows:
        print("Processing %d image%s" % (len(rows), 's' if len(rows) > 1 else ''))
    else:
        if ARG.SLIDE_CODE:
            LOGGER.critical("No images found for slide code %s", ARG.SLIDE_CODE)
        else:
            LOGGER.critical("No images found for release %s", ARG.RELEASE)
    slide_code = dict()
    for row in tqdm(rows):
        COUNT['read'] += 1
        LOGGER.info("Found image %s with cross barcode %s", row['id'], row['cross_barcode'])
        if row['cross_barcode'] not in cross:
            cdata = call_responder('flycore', '?request=crossdata&cross_barcode='
                                   + row['cross_barcode'])
            cross[row['cross_barcode']] = {'lab_member': cdata['crossdata']['lab_member'],
                                           'lab_project': cdata['crossdata']['Lab_Project'],
                                           'cross_description': cdata['crossdata']['Crossed_Notes']}
            if 'effector_description' in IMAGE_PROPS and row['effector'] in description:
                cross[row['cross_barcode']]['effector_description'] = description[row['effector']]
        if row['cross_barcode'] not in cross:
            LOGGER.error("Could not find cross data for %s", row['cross_barcode'])
            COUNT['no cross data'] += 1
            continue
        slide_code[row['slide_code']] = 1
        process_row(row, cross)
    complete_processing(slide_code)


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Update lab member/project for a given release")
    PARSER.add_argument('--release', dest='RELEASE', action='store',
                        default='Annotator Gen1 MCFO', help='database')
    PARSER.add_argument('--slide_code', dest='SLIDE_CODE', action='store',
                        help='slide code')
    PARSER.add_argument('--database', dest='DATABASE', action='store',
                        default='gen1mcfo', help='database')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='staging', help='manifold')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Actually write to database')
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
    OUTPUT_FILE = 'processed_slide_codes.txt'
    OUTPUT = open(OUTPUT_FILE, 'w')
    update_imageprops()
    sys.exit(0)
