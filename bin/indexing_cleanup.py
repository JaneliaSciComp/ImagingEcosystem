#!/opt/python/bin/python2.7

import argparse
import subprocess
import sys
import colorlog
import requests
import MySQLdb
from pymongo import MongoClient


# SQL statements
SQL = {
    'OVERDUE': "SELECT i.family,ipd.value AS data_set,ips.value AS slide_code,"
               + "i.name FROM image_vw i JOIN image_property_vw ipd ON "
               + "(i.id=ipd.image_id AND ipd.type='data_set') JOIN image_property_vw ips "
               + "ON (i.id=ips.image_id AND ips.type='slide_code') WHERE i.family NOT "
               + "LIKE 'simpson%' AND i.id NOT IN (SELECT image_id FROM "
               + "image_property_vw WHERE type='bits_per_sample') AND "
               + "TIMESTAMPDIFF(HOUR,i.create_date,NOW()) > 8",
    'ALL': "SELECT i.family,ipd.value AS data_set,ips.value AS slide_code,"
           + "i.name FROM image_vw i JOIN image_property_vw ipd ON "
           + "(i.id=ipd.image_id AND ipd.type='data_set') JOIN image_property_vw ips "
           + "ON  (i.id=ips.image_id AND ips.type='slide_code') WHERE i.family NOT "
           + "LIKE 'simpson%' AND i.id NOT IN (SELECT image_id FROM "
           + "image_property_vw WHERE type='bits_per_sample')",
    'SINGLE': "SELECT family,data_set,slide_code,name FROM image_data_mv WHERE "
              + "id=%s",
}
# Counters
COUNT = {'failure': 0, 'found': 0, 'skipped': 0, 'success': 0}
DSDICT = {}
INDEXED = {}
# Configuration
CONFIG = {'config': {'url': 'http://config.int.janelia.org/'}}
CONN = dict()
CURSOR = dict()

# -----------------------------------------------------------------------------


def call_responder(server, endpoint, post=''):
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
    except:
        LOGGER.critical('%s: %s', str(req.status_code), req.text)
    sys.exit(-1)


def sql_error(err):
    try:
        print('MySQL error [%d]: %s' % (err.args[0], err.args[1]))
    except IndexError:
        print('MySQL error: %s' % err)
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


def connect_databases():
    try:
        client = MongoClient('mongodb3.int.janelia.org:27017')
        dbm = client.jacs
        dbm.authenticate('flyportalRead', 'flyportalRead')
        cursor = dbm.dataSet.find({'sageGrammarPath':{'$exists':True}},
                                  {'_id':0, 'identifier':1, 'sageConfigPath':1,
                                   'sageGrammarPath':1})
    except Exception as err:
        print('Could not connect to Mongo: %s' % (err))
        sys.exit(-1)
    for dset in cursor:
        DSDICT[dset['identifier']] = {'config': dset['sageConfigPath'],
                                      'grammar': dset['sageGrammarPath']}
    (CONN['sage'], CURSOR['sage']) = db_connect(DATABASE['sage']['prod'])


def process_images():
    mode = 'ALL' if ARG.ALL else 'OVERDUE'
    stmt = SQL[mode]
    if ARG.SLIDE:
        addition = '%' + ARG.SLIDE + '%'
        stmt = stmt.replace("sample')",
                            "sample') AND ips.value LIKE '" + addition + "'")
    if ARG.DATASET:
        addition = '%' + ARG.DATASET + '%'
        stmt = stmt.replace("sample')",
                            "sample') AND ipd.value LIKE '" + addition + "'")
    rows = list()
    if ARG.IDS:
        idfile = open(ARG.IDS, "r")
        for line in idfile:
            line = line.rstrip()
            try:
                CURSOR['sage'].execute(SQL['SINGLE'], (line, ))
                row = CURSOR['sage'].fetchone()
                rows.append(row)
            except MySQLdb.Error as err:
                sql_error(err)
        idfile.close()
    else:
        try:
            LOGGER.debug(stmt)
            CURSOR['sage'].execute(stmt)
            rows = CURSOR['sage'].fetchall()
            print(rows)
        except MySQLdb.Error as err:
            sql_error(err)

    lsm = dict()
    for row in rows:
        config = ''
        grammar = ''
        COUNT['found'] += 1
        LOGGER.info("%s\t%s\t%s\t%s", row['family'], row['data_set'],
                    row['slide_code'], row['name'])
        if row['family'] == 'rubin_chacrm':
            config = '/groups/scicompsoft/informatics/data/rubin_light_imagery-config.xml'
            grammar = '/misc/sc/pipeline/grammar/chacrm_sage.gra'
        elif row['data_set'] in DSDICT:
            config = DSDICT[row['data_set']]['config']
            grammar = DSDICT[row['data_set']]['grammar']
            grammar = grammar.replace("/misc/local/pipeline", "/misc/sc/pipeline")
        else:
            LOGGER.error('Could not determine configuration and grammar '
                         + 'for data set %s', row['data_set'])
        lsm.setdefault(row['data_set'], []).append(row['name'])
        if config and grammar and ARG.INDEX_ONLY:
            index_image(config, grammar, row['name'], row['data_set'])
    operation = 'indexed'
    if not ARG.INDEX_ONLY:
        operation = 'indexed/discovered'
        for dataset, lsmlist in lsm.items():
            LOGGER.info("Running indexing/discovery on data set " + dataset
                        + " with " + str(len(lsmlist)) + " LSM(s)")
            if sys.version_info[0] == 2:
                carr = xrange(0, len(lsmlist), 50)
            else:
                carr = range(0, len(lsmlist), 50)
            chunks = [lsmlist[i:i + 50] for i in carr]
            for sublist in chunks:
                post = {"lsmNames": sublist}
                LOGGER.debug("  Posting " + str(len(sublist)) + " LSM(s)")
                if ARG.TEST:
                    COUNT['success'] += len(sublist)
                    INDEXED[dataset] = INDEXED.setdefault(dataset, 0) + len(sublist)
                else:
                    call_responder('jacs', 'process/owner/system/dataSet/'
                                   + dataset + '/lsmPipelines', post)
                    COUNT['success'] += len(sublist)
                    INDEXED[dataset] = INDEXED.setdefault(dataset, 0) + len(sublist)
    print('Unindexed images: %d' % COUNT['found'])
    if COUNT['skipped']:
        print('Skipped images: %d' % COUNT['skipped'])
    print('Images successfully %s: %d' % (operation, COUNT['success']))
    if INDEXED:
        for dset in sorted(INDEXED):
            print('  %s: %d' % (dset, INDEXED[dset]))
    if COUNT['failure']:
        print('Images failing indexing: %d' % COUNT['failure'])


def index_image(config, grammar, name, data_set):
    command = ['perl', '/groups/scicompsoft/informatics/bin/sage_loader.pl', '-config',
               config, '-grammar', grammar, '-item', name, '-lab',
               'flylight', '-verbose', '-description',
               'Image load from indexing_cleanup']
    LOGGER.info('Processing %s %s' % (data_set, name))
    LOGGER.debug('  ' + ' '.join(command))
    try:
        if ARG.TEST:
            tmp = 'OK'
            LOGGER.warning("Test mode: will not send transactions")
        else:
            tmp = subprocess.check_output(command, stderr=subprocess.STDOUT)
        if (tmp.find('Cannot read file') != -1) or \
           (tmp.find('Permission denied') != -1) or \
           (tmp.find('Unable to uncompress the stack') != -1):
            LOGGER.error('  Could not read LSM file')
            COUNT['failure'] += 1
        elif tmp.find('Incomplete image processing') != -1:
            LOGGER.error('  Image processing failed')
            COUNT['failure'] += 1
        else:
            COUNT['success'] += 1
            INDEXED[data_set] = INDEXED.setdefault(data_set, 0) + 1
    except subprocess.CalledProcessError as err:
        print(err.output)
        COUNT['failure'] += 1
    except Exception as err:
        print(err)
        COUNT['failure'] += 1


# -----------------------------------------------------------------------------


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(description='Find and index/discover newly tmogged imagery')
    PARSER.add_argument('--ids', dest='IDS', action='store',
                        help='File of image IDs (optional)')
    PARSER.add_argument('--data_set', dest='DATASET', action='store',
                        help='Data set (optional)')
    PARSER.add_argument('--slide_code', dest='SLIDE', action='store',
                        help='Slide code (optional)')
    PARSER.add_argument('--index', action='store_true', dest='INDEX_ONLY',
                        default=False, help='Do not run discovery')
    PARSER.add_argument('--verbose', action='store_true', dest='VERBOSE',
                        default=False, help='Turn on verbose output')
    PARSER.add_argument('--debug', action='store_true', dest='DEBUG',
                        default=False, help='Turn on debug output')
    PARSER.add_argument('--test', action='store_true', dest='TEST',
                        default=False,
                        help='Test mode - does not actually run the indexer or discovery service')
    PARSER.add_argument('--all', action='store_true', dest='ALL',
                        default=False,
                        help='Selects all images, not just overdue ones')
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

    data = call_responder('config', 'config/db_config')
    DATABASE = data['config']
    connect_databases()
    data = call_responder('config', 'config/rest_services')
    CONFIG = data['config']
    process_images()
