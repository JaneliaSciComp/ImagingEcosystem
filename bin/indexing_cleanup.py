#!/opt/python/bin/python2.7

import argparse
import os
import subprocess
import sys
import MySQLdb
from pymongo import MongoClient

# Command line parms
VERBOSE = False
DEBUG = False
TEST = False
# SQL statements
SQL = {
  'ALL': "SELECT i.family,ipd.value,i.name FROM image_vw i JOIN image_property_vw ipd ON (i.id=ipd.image_id AND ipd.type='data_set') WHERE i.family NOT LIKE 'simpson%' AND i.id NOT IN (SELECT image_id FROM image_property_vw WHERE type='bits_per_sample')",
}
# Counters
count = {'failure': 0, 'found': 0, 'success': 0}
dsdict = {}
indexed = {}

# -----------------------------------------------------------------------------
def sqlError (e):
    try:
      print 'MySQL error [%d]: %s' % (e.args[0],e.args[1])
    except IndexError:
      print 'MySQL error: %s' % e
    sys.exit(-1)

def dbConnect():
    try:
        client = MongoClient('mongodb3:27017')
        db = client.jacs
        db.authenticate('flyportalRead','flyportalRead')
        cursor = db.dataSet.find({'sageGrammarPath':{'$exists':True}},{'_id':0,'identifier':1,'sageConfigPath':1,'sageGrammarPath':1})
    except Exception as e:
        print 'Could not connect to Mongo: %s' % (e)
        sys.exit(-1)
    for ds in cursor:
        dsdict[ds['identifier']] = {'config': ds['sageConfigPath'],
                                    'grammar': ds['sageGrammarPath']}

    try:
        conn = MySQLdb.connect(host='mysql3',user='sageRead',passwd='sageRead',db='sage')
    except MySQLdb.Error as e:
        sqlError(e)
    try:
        cursor = conn.cursor()
        return(cursor)
    except MySQLdb.Error as e:
        sqlError(e)

def processImages(cursor):
    try:
        cursor.execute(SQL['ALL'])
    except MySQLdb.Error as e:
        sqlError(e)

    for (family,data_set,name) in cursor:
        config = ''
        grammar = ''
        count['found']+=1
        if family == 'rubin_chacrm':
            config = '/opt/informatics/data/rubin_light_imagery-config.xml'
            grammar = '/usr/local/pipeline/grammar/chacrm_sage.gra'
        elif dsdict.has_key(data_set):
          config = dsdict[data_set]['config']
          grammar = dsdict[data_set]['grammar']
        else:
          print 'Could not determine configuration and grammar for data set %s' % (data_set)
        if config and grammar:
            command = ['perl','/opt/informatics/bin/sage_loader.pl','-config',config,'-grammar',grammar,'-item',name,'-lab','flylight','-verbose']
            if VERBOSE:
                print 'Processing %s %s' % (data_set,name)
                if DEBUG:
                    print '  '+' '.join(command)
            try:
                if TEST:
                    tmp = 'OK'
                else:
                    tmp = subprocess.check_output(command,stderr=subprocess.STDOUT)
                if (tmp.find('Cannot read file') != -1) or (tmp.find('Permission denied') != -1) or (tmp.find('Unable to uncompress the stack') != -1):
                    if VERBOSE:
                        print '  Could not read LSM file'
                    count['failure']+=1
                elif tmp.find('Incomplete image processing') != -1:
                    if VERBOSE:
                        print '  Image processing failed'
                    count['failure']+=1
                else:
                    count['success']+=1
                    indexed[data_set] = indexed.setdefault(data_set,0) + 1
            except subprocess.CalledProcessError as e:
                print e.output
                count['failure']+=1
            except Exception as e:
                print e.output
                count['failure']+=1
        if DEBUG:
            print '-'*79
    print 'Unindexed images: %d' % count['found']
    print 'Images successfully indexed: %d' % count['success']
    if len(indexed):
        for (ds) in sorted(indexed):
            print '  %s: %d' % (ds,indexed[ds])
    print 'Images failing indexing: %d' % count['failure']

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Find and index newly tmogged imagery')
    parser.add_argument('-verbose',action='store_true',dest='verbose',default=False,help='Turn on verbose output')
    parser.add_argument('-debug',action='store_true',dest='debug',default=False,help='Turn on debug output')
    parser.add_argument('-test',action='store_true',dest='test',default=False,help='Test mode - does not actually run the indexer')
    args = parser.parse_args()
    VERBOSE = args.verbose
    DEBUG = args.debug
    TEST = args.test
    if DEBUG:
        VERBOSE = True
    (cursor) = dbConnect()
    processImages(cursor)