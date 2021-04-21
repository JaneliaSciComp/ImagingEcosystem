''' This program will update line properties on JACS
'''

import argparse
from os import path, remove
import sys
from time import strftime
import bson
import colorlog
import requests
import MySQLdb
from pymongo import MongoClient
from tqdm import tqdm


# Configuration
CONFIG = {'config': {'url': 'http://config.int.janelia.org/'}}
COUNT = {'images': 0, 'imagemm': 0, 'imageup': 0, 'lines': 0, 'missingj': 0,
         'missingl': 0, 'missings': 0, 'noprop': 0, 'samples': 0, 'samplemm': 0,
         'sampleup': 0, 'slide_codes': 0}
CROSSPROPS = ['cross_description', 'effector_description', 'lab_member', 'lab_project']
# Database
CONN = dict()
CURSOR = dict()
DBM = PROPMAP = PROPSET = ''
READ = {"LINEPROP": "SELECT type,value FROM line_property_vw WHERE name=%s ORDER BY 1",
        "IMAGEPROP": "SELECT data_set,cross_barcode,!REPLACE! FROM image_data_mv WHERE "
                     + "slide_code=%s GROUP BY 1,2,3",
        "IMAGES": "SELECT DISTINCT data_set,slide_code FROM image_data_mv WHERE line=%s "
                  + "AND data_set IS NOT NULL AND slide_code IS NOT NULL ORDER BY 1,2",
       }

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
        setdict = {PROPMAP[PROPSET][ARG.PROPERTY]: sage_prop}
        if ARG.PROPERTY == 'flycore_project':
            setdict[PROPMAP['image']['driver']] = sage_prop
        data = DBM.sample.update_one({"_id": sid},
                                     {"$set": setdict})
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
        setdict = {PROPMAP[PROPSET][ARG.PROPERTY]: sage_prop}
        if ARG.PROPERTY == 'flycore_project':
            setdict[PROPMAP['image']['driver']] = sage_prop
        data = DBM.image.update_one({"_id": iid},
                                    {"$set": setdict})
        COUNT['imageup'] += data.modified_count
    else:
        COUNT['imageup'] += 1


def update_jacs_images(payload, sage_prop):
    """ Update images in JACS
        Keyword arguments:
        payload: Mongo search payload
          sage_prop: property from SAGE
        Returns:
          None
    """
    try:
        cursor = DBM.image.find(payload)
    except Exception as err:
        LOGGER.error('Could not get sample from FlyPortal: %s', err)
        sys.exit(-1)
    for image in cursor:
        COUNT['images'] += 1
        prop = image[PROPMAP[PROPSET][ARG.PROPERTY]] \
               if PROPMAP[PROPSET][ARG.PROPERTY] in image else ''
        if prop != sage_prop:
            LOGGER.debug("SAGE (%s) does not match image (%s) for JACS:image %s",
                         sage_prop, prop, image['_id'])
            CHANGES.write("image\t%s\t%s\t%s\n" % (image['_id'], prop, sage_prop))
            update_image(image['_id'], sage_prop)
        elif ARG.DEBUG:
            LOGGER.debug("Image %s (%s) matches in SAGE and JACS", image['_id'], prop)


def update_jacs(slide_code, data_set, barcode, sage_prop):
    """ Update sample and images in JACS
        Keyword arguments:
          slide_code: image slide code
          data_set: image data set
          barcode: cross barcode
          sage_prop: property from SAGE
        Returns:
          None
    """
    # Sample
    payload = {'dataSet': data_set, 'slideCode': slide_code, 'sageSynced': True}
    if barcode:
        payload['crossBarcode'] = bson.Int64(barcode)
    try:
        cursor = DBM.sample.find(payload)
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
        if PROPMAP[PROPSET][ARG.PROPERTY] not in dset:
            LOGGER.debug("Sample %s does not have a %s", dset['_id'],
                         PROPMAP[PROPSET][ARG.PROPERTY])
            COUNT['noprop'] += 1
        else:
            prop = dset[PROPMAP[PROPSET][ARG.PROPERTY]]
        sid = dset['_id']
    if not checked:
        LOGGER.error("%s (%s) was not found in JACS", data_set, slide_code)
        COUNT['missingj'] += 1
        return
    COUNT['samples'] += 1
    if prop != sage_prop:
        LOGGER.debug("SAGE (%s) does not match sample (%s) for JACS:sample %s",
                     sage_prop, prop, sid)
        CHANGES.write("sample\t%s\t%s\t%s\n" % (sid, prop, sage_prop))
        update_sample(sid, sage_prop)
    elif ARG.DEBUG:
        LOGGER.debug("Sample %s (%s) matches in SAGE and JACS", sid, prop)
        return
    # Images
    update_jacs_images(payload, sage_prop)


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
        COUNT['missingl'] += 1
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
        update_jacs(image['slide_code'], image['data_set'], None, lineprop[ARG.PROPERTY])


def process_single_slide_code(slide):
    """ Update properties for one slide code on JACS
        Keyword arguments:
          slide: slide code
        Returns:
          None
    """
    READ['IMAGEPROP'] = READ['IMAGEPROP'].replace('!REPLACE!', ARG.PROPERTY)
    try:

        CURSOR['sage'].execute(READ['IMAGEPROP'], (slide, ))
        rows = CURSOR['sage'].fetchall()
    except Exception as err:
        sql_error(err)
    if not rows:
        LOGGER.error("%s was not found in SAGE", slide)
        COUNT['missings'] += 1
        return
    COUNT['slide_codes'] += 1
    for row in rows:
        if not row[ARG.PROPERTY]:
            LOGGER.warning("%s is null for image %s/%s", ARG.PROPERTY, slide, row['data_set'])
            continue
        barcode = row['cross_barcode'] if ARG.PROPERTY in CROSSPROPS else None
        if barcode:
            LOGGER.info("%s %s (%s)", slide, barcode, row[ARG.PROPERTY])
        else:
            LOGGER.info("%s (%s)", slide, row[ARG.PROPERTY])
        update_jacs(slide, row['data_set'], barcode, row[ARG.PROPERTY])


def process_lines():
    """ Update properties for lines on JACS
        Keyword arguments:
          None
        Returns:
          None
    """
    if ARG.FILE:
        with open(ARG.FILE) as ifile:
            items = ifile.read().splitlines()
        ifile.close()
        for item in tqdm(items):
            if PROPSET == 'image':
                process_single_slide_code(item)
            else:
                process_single_line(item)
    elif ARG.LINE:
        process_single_line(ARG.LINE)
    elif ARG.SLIDE:
        process_single_slide_code(ARG.SLIDE)
    print("Lines found in SAGE:       %d" % (COUNT['lines']))
    print("Lines missing from SAGE:   %d" % (COUNT['missingl']))
    print("Slides found in SAGE:      %d" % (COUNT['slide_codes']))
    print("Slides missing from SAGE:  %d" % (COUNT['missings']))
    print("Samples found:             %d" % (COUNT['samples']))
    print("Images found:              %d" % (COUNT['images']))
    print("Samples missing from JACS: %d" % (COUNT['missingj']))
    print("Samples missing data:      %d" % (COUNT['noprop']))
    print("Samples needing update:    %d" % (COUNT['samplemm']))
    print("Samples updated:           %d" % (COUNT['sampleup']))
    print("Images needing update:     %d" % (COUNT['imagemm']))
    print("Images updated:            %d" % (COUNT['imageup']))
    if CHANGES:
        CHANGES.close()
        if not path.getsize(CHANGE_FILE):
            remove(CHANGE_FILE)


# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Update line property on JACS")
    PARSER.add_argument('--property', dest='PROPERTY', action='store',
                        default='flycore_lab', help='Line property')
    INPUT_GROUP = PARSER.add_mutually_exclusive_group(required=True)
    INPUT_GROUP.add_argument('--line', dest='LINE', action='store',
                             default='', help='Line')
    INPUT_GROUP.add_argument('--slide', dest='SLIDE', action='store',
                             default='', help='Slide code')
    INPUT_GROUP.add_argument('--file', dest='FILE', action='store',
                             default='', help='File containing lines')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        choices=['dev', 'prod'], default='dev', help='Manifold')
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
    if ARG.PROPERTY in PROPMAP['line']:
        PROPSET = 'line'
    elif ARG.PROPERTY in PROPMAP['image']:
        PROPSET = 'image'
    else:
        LOGGER.error("Property %s is not mapped to a JACS property name", ARG.PROPERTY)
        sys.exit(-1)
    LOGGER.info("%s is a %s property", ARG.PROPERTY, PROPSET)
    CHANGES = None
    TIMESTAMP = strftime("%Y%m%dT%H%M%S")
    CHANGE_FILE = '%s_property_changes_%s.txt' % (PROPSET, TIMESTAMP)
    CHANGES = open(CHANGE_FILE, 'w')
    process_lines()
    sys.exit(0)
