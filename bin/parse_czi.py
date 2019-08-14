''' parse_czi.py
    Parse a CZI file and optionally update SAGE with image properties
    Input must be provided by one of the following methods:
    - --id specifyting a single image ID
    - --czi_file specifying a single CZI file
    - --file containing a list of image IDs or file paths
'''

import argparse
import inspect
import os
import pathlib
import pprint
import re
import select
import sys
import bioformats
import colorlog
import javabridge
import MySQLdb
import requests
import xmltodict

# CZI -> imageprop translation
#'Experimenter': {'@UserName': 'created_by'},
TRANSLATE = {'Instrument': {'@Gain': 'lsm_detection_channel_'},
             'Image': {'AcquisitionDate': 'capture_date', '@Name': 'microscope_filename'},
             'Pixels': {'@PhysicalSizeX' : 'voxel_size_x', '@PhysicalSizeY' : 'voxel_size_y',
                        '@PhysicalSizeZ' : 'voxel_size_z',
                        '@SizeC': 'channels', '@SizeX' : 'dimension_x',
                        '@SizeY' : 'dimension_y', '@SizeZ' : 'dimension_z'},
             'Annotation': {'Experiment|AcquisitionBlock|AcquisitionModeSetup|BitsPerSample': \
                                'bits_per_sample',
                            'Experiment|AcquisitionBlock|AcquisitionModeSetup|Objective': \
                                'objective',
                            'Information|Instrument|Microscope|System': 'microscope_model',
                           },
             'Array': {'XXXInformation|Image|Channel|DigitalGain': 'lsm_detection_channel_',
                       'Experiment|AcquisitionBlock|MultiTrackSetup|TrackSetup|Name': \
                           'lsm_illumination_channel_',
                      },
            }
SUFFIX = {'Information|Image|Channel|DigitalGain': '_detector_gain',
          '@Gain': '_detector_gain',
          'Experiment|AcquisitionBlock|MultiTrackSetup|TrackSetup|Name': '_name',
         }
# Database
READ = {
    'IMAGEID': "SELECT id,path FROM image WHERE name='%s'",
    'IMAGEPATH': "SELECT path FROM image WHERE id=%s",
    'PROPERTY': "SELECT id FROM image_property WHERE image_id=%s AND "
                + "type_id=getCvTermId('light_imagery','%s',NULL)",
}
WRITE = {
    'INSERTPROP': "INSERT INTO image_property (image_id,type_id,value) VALUES "
                  + "('%s',getCvTermId('light_imagery','%s',NULL),'%s')",
    'UPDATEPROP': "UPDATE image_property SET value='%s' WHERE id=%s"
}
# Configuration
CONFIG = {'config': {'url': 'http://config.int.janelia.org/'}}
# Error handling
TEMPLATE = "{2}: An exception of type {0} occurred. Arguments:\n{1!r}"

# -----------------------------------------------------------------------------


# pylint: disable=W0703


def call_responder(server, endpoint, post=''):
    ''' Call a responder
        Keyword arguments:
          server: server
          endpoint: REST endpoint
          post: JSON payload
    '''
    url = CONFIG[server]['url'] + endpoint
    try:
        if post:
            req = requests.post(url, json=post)
        else:
            req = requests.get(url)
    except requests.exceptions.RequestException as err:
        LOGGER.critical(err)
        sys.exit(-1)
    if req.status_code in (200, 201):
        return req.json()
    if req.status_code == 404:
        return ''
    try:
        LOGGER.critical('%s: %s', str(req.status_code), req.json()['rest']['message'])
    except Exception as err:
        LOGGER.critical('%s: %s', str(req.status_code), req.text)
    sys.exit(-1)


def sql_error(err):
    """ Report a SQL error
        Keyword arguments:
          err: error object
    """
    try:
        print('MySQL error [%d]: %s' % (err.args[0], err.args[1]))
    except IndexError:
        print('MySQL error: %s' % err)
    sys.exit(-1)


def db_connect(dba):
    """ Connect to a database
        Keyword arguments:
          dba: database dictionary
    """
    LOGGER.debug("Connecting to %s on %s", dba['name'], dba['host'])
    try:
        conn = MySQLdb.connect(host=dba['host'], user=dba['user'],
                               passwd=dba['password'], db=dba['name'])
    except Exception as err:
        sql_error(err)
    try:
        cur = conn.cursor()
        return conn, cur
    except Exception as err:
        sql_error(err)


def parse_experimenter(j, record):
    ''' Parse the Experimenter block
        Keyword arguments:
          j: JSON OME dictionary
          record: image properties record
    '''
    if 'Experimenter' in j['OME']:
        for key in TRANSLATE['Experimenter']:
            if key in j['OME']['Experimenter']:
                record[TRANSLATE['Experimenter'][key]] = \
                    j['OME']['Experimenter'][key]


def parse_instrument(j, record):
    ''' Parse the Instrument block
        Keyword arguments:
          j: JSON OME dictionary
          record: image properties record
    '''
    if 'Instrument' in j['OME'] and 'Detector' in j['OME']['Instrument']:
        chan = 1
        for detector in j['OME']['Instrument']['Detector']:
            key = '@Gain'
            reckey = TRANSLATE['Instrument'][key] + str(chan)
            if key in SUFFIX:
                reckey += SUFFIX[key]
            record[reckey] = detector[key]
            chan += 1


def parse_image(j, record):
    ''' Parse the Image block
        Keyword arguments:
          j: JSON OME dictionary
          record: image properties record
    '''
    if 'Image' in j['OME']:
        for key in TRANSLATE['Image']:
            if key in j['OME']['Image']:
                record[TRANSLATE['Image'][key]] = j['OME']['Image'][key]
    if 'capture_date' in record:
        record['capture_date'] = record['capture_date'].replace('T', ' ')
        record['capture_date'] = re.sub(r'\.\d+', '', record['capture_date'])
    if 'Pixels' in j['OME']['Image']:
        for key in TRANSLATE['Pixels']:
            record[TRANSLATE['Pixels'][key]] = j['OME']['Image']['Pixels'][key]


def parse_structuredannotations(j, record):
    ''' Parse the StructuredAnnotations block
        Keyword arguments:
          j: JSON OME dictionary
          record: image properties record
    '''
    for ann in j['OME']['StructuredAnnotations']['XMLAnnotation']:
        this = ann['Value']['OriginalMetadata']
        key = this['Key']
        value = this['Value']
        value = re.sub(r'[\[\]]', '', this['Value'])
        if key in TRANSLATE['Annotation']:
            record[TRANSLATE['Annotation'][key]] = value
        elif key in TRANSLATE['Array']:
            chan = 1
            for chanval in value.split(', '):
                reckey = TRANSLATE['Array'][key] + str(chan)
                if key in SUFFIX:
                    reckey += SUFFIX[key]
                record[reckey] = chanval
                chan += 1
        elif DEBUG:
            print("%s\t%s" % (key, value))


def update_database(image_id, record):
    ''' Update the image in SAGE
        Keyword arguments:
          record: image properties record
    '''
    for term in record:
        CURSOR.execute(READ['PROPERTY'] % (image_id, term))
        prop_id = CURSOR.fetchone()
        if prop_id:
            prop_id = prop_id[0]
            cursor = 'UPDATEPROP'
            bind = (record[term], prop_id)
        else:
            cursor = 'INSERTPROP'
            bind = (image_id, term, record[term])
        if DEBUG:
            LOGGER.debug(WRITE[cursor], *bind)
        try:
            CURSOR.execute(WRITE[cursor] % bind)
        except Exception as err:
            sql_error(err)
    CONNECTION.commit()


def parse_czi(id_or_file):
    ''' Parse a CZI file
    '''
    LOGGER.info("Processing %s", id_or_file)
    filepath = image_id = id_or_file
    if id_or_file.isdigit():
        # Get filepath
        try:
            CURSOR.execute(READ['IMAGEPATH'] % (id_or_file))
            filepath = CURSOR.fetchone()
        except Exception as err:
            sql_error(err)
        if filepath:
            filepath = filepath[0]
        else:
            print("No file found for image ID %s" % (image_id))
            return
    else:
        if not ARGS.czi_file:
            try:
                CURSOR.execute(READ['IMAGEID'] % (id_or_file))
                row = CURSOR.fetchone()
            except Exception as err:
                sql_error(err)
            if row:
                (image_id, filepath) = row
            else:
                print("No file found for image ID %s" % (image_id))
                return
    path = pathlib.Path(filepath)
    if not (path.exists() and path.is_file()):
        print("File %s was not found" % (filepath))
        return
    xml = bioformats.get_omexml_metadata(filepath)
    j = xmltodict.parse(xml)
    record = dict()
    ppr = pprint.PrettyPrinter(indent=2)
    if 'Experimenter' in TRANSLATE:
        parse_experimenter(j, record)
    parse_instrument(j, record)
    parse_image(j, record)
    parse_structuredannotations(j, record)
    ppr.pprint(record)
    if image_id and ARGS.write:
        update_database(image_id, record)


def parse_input():
    ''' Accept input and parse one or more IDs/files
    '''
    if ARGS.id or ARGS.czi_file:
        parse_czi(ARGS.id if ARGS.id else ARGS.czi_file)
    elif ARGS.file:
        try:
            with open(ARGS.file, "r") as infile:
                for line in infile:
                    line = line.rstrip()
                    parse_czi(line)
        except Exception as err:
            mess = TEMPLATE.format(type(err).__name__, err.args, inspect.stack()[0][3])
            LOGGER.critical(mess)
    elif select.select([sys.stdin], [], [], 1)[0]:
        for line in sys.stdin:
            line = line.rstrip()
            parse_czi(line)
    else:
        LOGGER.critical("You must specify input with --id, --czi_file, --file, or on STDIN")


# -----------------------------------------------------------------------------


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(description='Parse CZI files')
    PARSER.add_argument('--id', dest='id', action='store',
                        help='Image ID to identify file to parse (optional)')
    PARSER.add_argument('--czi_file', dest='czi_file', action='store',
                        help='File path of CZI to parse (optional)')
    PARSER.add_argument('--file', dest='file', action='store',
                        help='File with list of IDs or files to process (optional)')
    PARSER.add_argument('--verbose', action='store_true', dest='verbose',
                        default=False, help='Turn on verbose output')
    PARSER.add_argument('--debug', action='store_true', dest='debug',
                        default=False, help='Turn on debug output')
    PARSER.add_argument('--write', action='store_true', dest='write',
                        default=False, help='Write parsed data to database')
    ARGS = PARSER.parse_args()
    if ARGS.czi_file and ARGS.write:
        print("--czi_file cannot be used with --write")
        sys.exit(0)
    VERBOSE = ARGS.verbose
    DEBUG = ARGS.debug
    if DEBUG:
        VERBOSE = True
    LOGGER = colorlog.getLogger()
    if DEBUG:
        LOGGER.setLevel(colorlog.colorlog.logging.DEBUG)
    elif VERBOSE:
        LOGGER.setLevel(colorlog.colorlog.logging.INFO)
    else:
        LOGGER.setLevel(colorlog.colorlog.logging.WARNING)
    HANDLER = colorlog.StreamHandler()
    HANDLER.setFormatter(colorlog.ColoredFormatter())
    LOGGER.addHandler(HANDLER)
    DATA = call_responder('config', 'config/db_config')
    DATABASE = DATA['config']
    (CONNECTION, CURSOR) = db_connect(DATABASE['sage']['prod'])
    LOG_CONFIG = os.path.join(os.path.split(__file__)[0], "log4j.properties")
    javabridge.start_vm(args=["-Dlog4j.configuration=file:{}".format(LOG_CONFIG),],
                        class_path=bioformats.JARS, run_headless=True)
    parse_input()
    javabridge.kill_vm()
