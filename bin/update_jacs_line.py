''' This program will update line properties on JACS
'''

import argparse
import sys
import colorlog
import requests
import MySQLdb
from pymongo import MongoClient
from tqdm import tqdm


# Configuration
CONFIG = {'config': {'url': 'http://config.int.janelia.org/'}}
COUNT = {'images': 0, 'imagemm': 0, 'imageup': 0, 'lines': 0, 'missingj': 0,
         'missings': 0, 'noprop': 0, 'samples': 0, 'samplemm': 0, 'sampleup': 0}
# Database
CONN = dict()
CURSOR = dict()
DBM = PROPMAP = ''
READ = {"LINEPROP": "SELECT type,value FROM line_property_vw WHERE name=%s ORDER BY 1",
        "IMAGES": "SELECT DISTINCT data_set,slide_code FROM image_data_mv WHERE line=%s "
                  + "AND data_set IS NOT NULL AND slide_code IS NOT NULL ORDER BY 1,2",
       }

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
    global CONFIG, PROPMAP, DBM  # pylint: disable=W0603
    data = call_responder('config', 'config/rest_services')
    CONFIG = data['config']
    data = call_responder('config', 'config/sage_jacs_property_mapping')
    PROPMAP = data['config']
    data = call_responder('config', 'config/db_config')
    (CONN['sage'], CURSOR['sage']) = db_connect(data['config']['sage']['prod'])
    # Connect to Mongo
    rwp = 'write' if ARG.WRITE else 'read'
    try:
        client = MongoClient(data['config']['jacs-mongo'][ARG.MANIFOLD][rwp]['host'])
        DBM = client.jacs
        if ARG.MANIFOLD != 'dev':
            DBM.authenticate(data['config']['jacs-mongo'][ARG.MANIFOLD][rwp]['user'],
                             data['config']['jacs-mongo'][ARG.MANIFOLD][rwp]['password'])
    except Exception as err:
        LOGGER.error('Could not connect to Mongo: %s', err)
        sys.exit(-1)


def update_sample(sid, sage_prop):
    """ Update the sample in JACS
        Keyword arguments:
          sid: sample ID
          sage_prop: lineprop from SAGE
        Returns:
          None
    """
    COUNT['samplemm'] += 1
    if ARG.WRITE:
        data = DBM.sample.update_one({"_id": sid},
                                     {"$set": {PROPMAP['line'][ARG.PROPERTY]: sage_prop}})
        COUNT['sampleup'] += data.modified_count
    else:
        COUNT['sampleup'] += 1


def update_image(iid, sage_prop):
    """ Update the image in JACS
        Keyword arguments:
          iid: image ID
          sage_prop: lineprop from SAGE
        Returns:
          None
    """
    COUNT['imagemm'] += 1
    if ARG.WRITE:
        data = DBM.image.update_one({"_id": iid},
                                    {"$set": {PROPMAP['line'][ARG.PROPERTY]: sage_prop}})
        COUNT['imageup'] += data.modified_count
    else:
        COUNT['imageup'] += 1


def update_jacs(slide_code, data_set, sage_prop):
    """ Check JACS status for an image
        Keyword arguments:
          slide_code: image slide code
          data_set: image data set
          sage_prop: lineprop from SAGE
        Returns:
          None
    """
    # Sample
    try:
        cursor = DBM.sample.find({'dataSet': data_set, 'slideCode': slide_code, 'sageSynced': True})
    except Exception as err:
        LOGGER.error('Could not get sample from FlyPortal: %s', err)
        sys.exit(-1)
    if not cursor:
        LOGGER.error("%s (%s) was not found in JACS", data_set, slide_code)
        COUNT['missingj'] += 1
        return
    prop = sid = ''
    checked = False
    for dset in cursor:
        checked = True
        if sid:
            LOGGER.error("%s (%s) has multiple samples in JACS", data_set, slide_code)
            return
        if PROPMAP['line'][ARG.PROPERTY] not in dset:
            LOGGER.debug("Sample %s does not have a %s", dset['_id'],
                         PROPMAP['line'][ARG.PROPERTY])
            COUNT['noprop'] += 1
        else:
            prop = dset[PROPMAP['line'][ARG.PROPERTY]]
        sid = dset['_id']
    if not checked:
        LOGGER.error("%s (%s) was not found in JACS", data_set, slide_code)
        COUNT['missingj'] += 1
        return
    COUNT['samples'] += 1
    if prop != sage_prop:
        LOGGER.debug("SAGE (%s) does not match image (%s) for JACS:sample %s", sage_prop, prop, sid)
        update_sample(sid, sage_prop)
    # Image
    try:
        cursor = DBM.image.find({'dataSet': data_set, 'slideCode': slide_code, 'sageSynced': True})
    except Exception as err:
        LOGGER.error('Could not get sample from FlyPortal: %s', err)
        sys.exit(-1)
    for image in cursor:
        COUNT['images'] += 1
        prop = image[PROPMAP['line'][ARG.PROPERTY]] \
               if PROPMAP['line'][ARG.PROPERTY] in image else ''
        if prop != sage_prop:
            LOGGER.debug("SAGE (%s) does not match image (%s) for JACS:image %s",
                         sage_prop, prop, image['_id'])
            update_image(image['_id'], sage_prop)


def process_single_line(line):
    """ Update properties for one line on JACS
        Keyword arguments:
          line: line
        Returns:
          None
    """
    try:
        CURSOR['sage'].execute(READ['LINEPROP'], (line, ))
        rows = CURSOR['sage'].fetchall()
    except Exception as err:
        sql_error(err)
    if not rows:
        LOGGER.error("%s was not found in SAGE", line)
        COUNT['missings'] += 1
        return
    COUNT['lines'] += 1
    lineprop = dict()
    for row in rows:
        lineprop[row['type']] = row['value']
    LOGGER.info("%s (%s)", line, lineprop[ARG.PROPERTY])
    try:
        CURSOR['sage'].execute(READ['IMAGES'], (line, ))
        images = CURSOR['sage'].fetchall()
    except Exception as err:
        sql_error(err)
    for image in images:
        LOGGER.info("%s (%s)", image['slide_code'], image['data_set'])
        update_jacs(image['slide_code'], image['data_set'], lineprop[ARG.PROPERTY])


def process_lines():
    """ Update properties for lines on JACS
        Keyword arguments:
          None
        Returns:
          None
    """
    if ARG.LINE:
        process_single_line(ARG.LINE)
    else:
        with open(ARG.FILE) as lfile:
            lines = lfile.read().splitlines()
        lfile.close()
        for line in tqdm(lines):
            process_single_line(line)
    print("Lines found in SAGE:       %d" % (COUNT['lines']))
    print("Lines missing from SAGE:   %d" % (COUNT['missings']))
    print("Samples found:             %d" % (COUNT['samples']))
    print("Images found:              %d" % (COUNT['images']))
    print("Samples missing from JACS: %d" % (COUNT['missingj']))
    print("Samples missing data:      %d" % (COUNT['noprop']))
    print("Samples needing update:    %d" % (COUNT['samplemm']))
    print("Samples updated:           %d" % (COUNT['sampleup']))
    print("Images needing update:     %d" % (COUNT['imagemm']))
    print("Images updated:            %d" % (COUNT['imageup']))


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Update line property on JACS")
    PARSER.add_argument('--property', dest='PROPERTY', action='store',
                        default='flycore_lab', help='Line property')
    PARSER.add_argument('--line', dest='LINE', action='store',
                        default='', help='Line')
    PARSER.add_argument('--file', dest='FILE', action='store',
                        default='', help='File containing lines')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='dev', help='manifold')
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
    if ARG.PROPERTY not in PROPMAP['line']:
        LOGGER.error("Line property %s is not mapped to a JACS property name", ARG.PROPERTY)
        sys.exit(-1)
    process_lines()
    sys.exit(0)
