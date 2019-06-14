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
    'OVERDUE': "SELECT i.family,ipd.value,ips.value,i.name FROM image_vw i "
        + "JOIN image_property_vw ipd ON (i.id=ipd.image_id AND "
        + "ipd.type='data_set') JOIN image_property_vw ips ON "
        + "(i.id=ips.image_id AND ips.type='slide_code') WHERE i.family NOT "
        + "LIKE 'simpson%' AND i.id NOT IN (SELECT image_id FROM "
        + "image_property_vw WHERE type='bits_per_sample') AND "
        + "TIMESTAMPDIFF(HOUR,i.create_date,NOW()) > 8",
    'ALL': "SELECT i.family,ipd.value,ips.value,i.name FROM image_vw i JOIN "
        + "image_property_vw ipd ON (i.id=ipd.image_id AND "
        + "ipd.type='data_set') JOIN image_property_vw ips ON "
        + "(i.id=ips.image_id AND ips.type='slide_code') WHERE i.family NOT "
        + "LIKE 'simpson%' AND i.id NOT IN (SELECT image_id FROM "
        + "image_property_vw WHERE type='bits_per_sample')",
}
# Counters
count = {'failure': 0, 'found': 0, 'skipped': 0, 'success': 0}
dsdict = {}
indexed = {}
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
    try:
        client = MongoClient('mongodb3.int.janelia.org:27017')
        db = client.jacs
        db.authenticate('flyportalRead', 'flyportalRead')
        cursor = db.dataSet.find({'sageGrammarPath':{'$exists':True}}, {'_id':0,'identifier':1, 'sageConfigPath':1, 'sageGrammarPath':1})
    except Exception as e:
        print('Could not connect to Mongo: %s' % (e))
        sys.exit(-1)
    for ds in cursor:
        dsdict[ds['identifier']] = {'config': ds['sageConfigPath'],
                                    'grammar': ds['sageGrammarPath']}
    (conn, cursor) = db_connect(DATABASE['sage']['prod'])
    return(cursor)


def processImages(cursor):
    mode = 'ALL' if ALL else 'OVERDUE'
    stmt = SQL[mode]
    if SLIDE:
        addition = '%' + SLIDE + '%'
        stmt = stmt.replace("sample')",
                            "sample') AND ips.value LIKE '" + addition + "'")
    if DATASET:
        addition = '%' + DATASET + '%'
        stmt = stmt.replace("sample')",
                            "sample') AND ipd.value LIKE '" + addition + "'")
    try:
        logger.debug(stmt)
        cursor.execute(stmt)
    except MySQLdb.Error as e:
        sqlError(e)

    lsm = dict()
    for (family, data_set, slide_code, name) in cursor:
        config = ''
        grammar = ''
        count['found'] += 1
        if DEBUG:
            print("%s\t%s\t%s\t%s" % (family, data_set, slide_code, name))
        if family == 'rubin_chacrm':
            config = '/groups/scicomp/informatics/data/rubin_light_imagery-config.xml'
            grammar = '/misc/local/pipeline/grammar/chacrm_sage.gra'
        elif data_set in dsdict:
            config = dsdict[data_set]['config']
            grammar = dsdict[data_set]['grammar']
        else:
            print('Could not determine configuration and grammar for data set %s' % (data_set))
        lsm.setdefault(data_set, []).append(name)
        if config and grammar and INDEX_ONLY:
            indexImage(config, grammar, name, data_set)
    operation = 'indexed'
    if not INDEX_ONLY:
        operation = 'indexed/discovered'
        for dataset, lsmlist in lsm.items():
            logger.info("Running indexing/discovery on data set " + dataset + " with " + str(len(lsmlist)) + " LSM(s)")
            if sys.version_info[0] == 2:
                carr = xrange(0, len(lsmlist), 50)
            else:
                carr = range(0, len(lsmlist), 50)
            chunks = [lsmlist[i:i + 50] for i in carr]
            for sublist in chunks:
                post = {"lsmNames": sublist}
                logger.debug("  Posting " + str(len(sublist)) + " LSM(s)")
                if TEST:
                    count['success'] += len(sublist)
                    indexed[dataset] = indexed.setdefault(dataset, 0) + len(sublist)
                else:
                    response = call_responder('jacs', 'process/owner/system/dataSet/' + dataset + '/lsmPipelines', post)
                    count['success'] += len(sublist)
                    indexed[dataset] = indexed.setdefault(dataset, 0) + len(sublist)
    print('Unindexed images: %d' % count['found'])
    if count['skipped']:
        print('Skipped images: %d' % count['skipped'])
    print('Images successfully %s: %d' % (operation, count['success']))
    if len(indexed):
        for (ds) in sorted(indexed):
            print('  %s: %d' % (ds, indexed[ds]))
    if count['failure']:
        print('Images failing indexing: %d' % count['failure'])


def indexImage(config, grammar, name, data_set):
    command = ['perl', '/opt/informatics/bin/sage_loader.pl', '-config',
               config, '-grammar', grammar, '-item', name, '-lab',
               'flylight', '-verbose', '-description',
               'Image load from indexing_cleanup']
    logger.info('Processing %s %s' % (data_set, name))
    logger.debug('  ' + ' '.join(command))
    try:
        if TEST:
            tmp = 'OK'
            logger.warning("Test mode: will not send transactions")
        else:
            tmp = subprocess.check_output(command, stderr=subprocess.STDOUT)
        if (tmp.find('Cannot read file') != -1) or (tmp.find('Permission denied') != -1) or (tmp.find('Unable to uncompress the stack') != -1):
            logger.error('  Could not read LSM file')
            count['failure'] += 1
        elif tmp.find('Incomplete image processing') != -1:
            logger.error('  Image processing failed')
            count['failure'] += 1
        else:
            count['success'] += 1
            indexed[data_set] = indexed.setdefault(data_set, 0) + 1
    except subprocess.CalledProcessError as err:
        print(err.output)
        count['failure'] += 1
    except Exception as err:
        print(err)
        count['failure'] += 1
    if DEBUG:
        print('-' * 79)

# -----------------------------------------------------------------------------


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Find and index/discover newly tmogged imagery')
    parser.add_argument('--data_set', dest='dataset', action='store',
                        help='Data set (optional)')
    parser.add_argument('--slide_code', dest='slide', action='store',
                        help='Slide code (optional)')
    parser.add_argument('--index', action='store_true', dest='index',
                        default=False, help='Do not run discovery')
    parser.add_argument('--verbose', action='store_true', dest='verbose',
                        default=False, help='Turn on verbose output')
    parser.add_argument('--debug', action='store_true', dest='debug',
                        default=False, help='Turn on debug output')
    parser.add_argument('--test', action='store_true', dest='test',
                        default=False, help='Test mode - does not actually run the indexer or discovery service')
    parser.add_argument('--all', action='store_true', dest='all',
                        default=False, help='Selects all images, not just overdue ones')
    args = parser.parse_args()
    DATASET = args.dataset
    SLIDE = args.slide
    INDEX_ONLY = args.index
    VERBOSE = args.verbose
    DEBUG = args.debug
    TEST = args.test
    ALL = args.all
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
    data = call_responder('config', 'config/rest_services')
    CONFIG = data['config']
    processImages(cursor)
