''' parse_czi.py
    Parse a CZI file and optionally update SAGE with image properties
'''

import argparse
import pprint
import re
import sys
import bioformats
import colorlog
import javabridge
import MySQLdb
import requests
import xmltodict

TRANSLATE = {'Experimenter': {'@UserName': 'created_by'},
             'Instrument': {'@Gain': 'lsm_detection_channel_'},
             'Image': {'AcquisitionDate': 'capture_date', '@Name': 'microscope_filename'},
             'Pixels': {'@PhysicalSizeX' : 'voxel_size_x', '@PhysicalSizeY' : 'voxel_size_y',
                        '@PhysicalSizeZ' : 'voxel_size_z',
                        '@SizeC': 'num_channels', '@SizeX' : 'dimension_x',
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
# Configuration
CONFIG = {'config': {'url': 'http://config.int.janelia.org/'}}

# -----------------------------------------------------------------------------


def call_responder(server, endpoint, post=''):
    url = CONFIG[server]['url'] + endpoint
    try:
        if post:
            req = requests.post(url, json=post)
        else:
            req = requests.get(url)
    except requests.exceptions.RequestException as err:
        logger.critical(err)
        sys.exit(-1)
    if req.status_code in (200, 201):
        return req.json()
    elif req.status_code == 404:
        return ''
    else:
        try:
            logger.critical('%s: %s', str(req.status_code), req.json()['rest']['message'])
        except:
            logger.critical('%s: %s', str(req.status_code), req.text)
        sys.exit(-1)


def sqlError(err):
    try:
        print('MySQL error [%d]: %s' % (err.args[0], err.args[1]))
    except IndexError:
        print('MySQL error: %s' % err)
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


def connect_databases():
    (conn, cursor) = db_connect(DATABASE['sage']['prod'])
    return(cursor)


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
        else:
            print("%s\t%s" % (key, value))


def parse_czi(id_or_file):
    ''' Parse a CZI file
    '''
    filepath = id_or_file
    xml = bioformats.get_omexml_metadata(filepath)
    j = xmltodict.parse(xml)
    record = dict()
    ppr = pprint.PrettyPrinter(indent=4)
    ppr.pprint(j['OME'])
    print('------------------------------')
    parse_experimenter(j, record)
    parse_instrument(j, record)
    parse_image(j, record)
    parse_structuredannotations(j, record)
    ppr.pprint(record)

# -----------------------------------------------------------------------------


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Parse CZI files')
    parser.add_argument('--id', dest='id', action='store',
                        help='Image ID to identify file to parse (optional)')
    parser.add_argument('--czi_file', dest='czi_file', action='store',
                        help='File path of CZI to parse (optional)')
    parser.add_argument('--verbose', action='store_true', dest='verbose',
                        default=False, help='Turn on verbose output')
    parser.add_argument('--debug', action='store_true', dest='debug',
                        default=False, help='Turn on debug output')
    parser.add_argument('--write', action='store_true', dest='write',
                        default=False, help='Write parsed data to database')
    args = parser.parse_args()
    VERBOSE = args.verbose
    DEBUG = args.debug
    WRITE = args.write
    if DEBUG:
        VERBOSE = True
    logger = colorlog.getLogger()
    if DEBUG:
        logger.setLevel(colorlog.colorlog.logging.DEBUG)
    elif VERBOSE:
        logger.setLevel(colorlog.colorlog.logging.INFO)
    else:
        logger.setLevel(colorlog.colorlog.logging.WARNING)
    HANDLER = colorlog.StreamHandler()
    HANDLER.setFormatter(colorlog.ColoredFormatter())
    logger.addHandler(HANDLER)

    data = call_responder('config', 'config/db_config')
    DATABASE = data['config']
    (cursor) = connect_databases()
    javabridge.start_vm(class_path=bioformats.JARS, run_headless=True)

    if args.id or args.czi_file:
        parse_czi(args.id if args.id else args.czi_file)
    else:
        for line in sys.stdin:
            line = line.rstrip()
            parse_czi(line)

    javabridge.kill_vm()
