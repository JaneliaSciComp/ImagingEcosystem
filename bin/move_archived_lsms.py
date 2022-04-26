import argparse
import datetime
import json
import os
import re
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
READ = {"PRIMARY": "SELECT i.*,slide_code FROM image_vw i JOIN image_data_mv im "
                   + "ON (i.id=im.id) WHERE data_set=%s AND i.name LIKE '%%\.lsm%%' "
                   + "AND i.jfs_path like '/groups/scicomp/lsms/JACS/%' ORDER BY i.name",
        "FROMLSM": "SELECT i.*,slide_code FROM image_vw i JOIN image_data_mv im "
                    + "ON (i.id=im.id) WHERE data_set=%s AND im.jfs_path=%s",
        "FROMLSM2": "SELECT i.*,slide_code FROM image_vw i JOIN image_data_mv im "
                     + "ON (i.id=im.id) WHERE im.jfs_path=%s"
       }
WRITE = {"IMAGE": "UPDATE image SET jfs_path=%s,url=%s WHERE id=%s",
         "MV": "UPDATE image_data_mv SET jfs_path=%s,image_url=%s WHERE id=%s"
        }
# Configuration
CONFIG = {'config': {'url': 'http://config.int.janelia.org/'}}
# Stats
COUNT = {"In SAGE": 0, "Missing": 0, "Not archived": 0, "Already moved": 0, 
         "Incorrect path": 0, "Archived": 0, "In JACS": 0, "Not flylight": 0,
         "No path": 0, "Not in JACS": 0}
# Constants
ARCHIVE_PATH = "/groups/scicomp/lsms/JACS/"
NEW_PATH = "/nearline/flylight/lsms/JACS/"

# pylint: disable=W0703

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
    LOGGER.info("Connecting to Mongo on %s", ARG.MANIFOLD)
    rwp = 'write' if ARG.WRITE else 'read'
    try:
        if ARG.MANIFOLD == 'prod':
            client = MongoClient(data['config']['jacs-mongo'][ARG.MANIFOLD][rwp]['host'],
                                 replicaSet='replWorkstation')
        elif ARG.MANIFOLD == 'local':
            client = MongoClient()
        else:
            client = MongoClient(data['config']['jacs-mongo'][ARG.MANIFOLD][rwp]['host'])
        DBM = client.jacs
        if ARG.MANIFOLD == 'prod':
            DBM.authenticate(data['config']['jacs-mongo'][ARG.MANIFOLD][rwp]['user'],
                             data['config']['jacs-mongo'][ARG.MANIFOLD][rwp]['password'])
    except Exception as err:
        terminate_program('Could not connect to Mongo: %s' % (err))


def dump(obj):
  for attr in dir(obj):
    print("obj.%s = %r" % (attr, getattr(obj, attr)))


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime.date, datetime.datetime)):
        return obj.isoformat()
    raise TypeError ("Type %s not serializable" % type(obj))


def fetch_mongo(name, slide_code=None):
    short = re.sub("^.*\/", "", name)
    try:
        response = DBM.image.find({"slideCode": slide_code})
    except Exception as err:
        print('Could not get sample from FlyPortal: %s' % (err))
        sys.exit(-1)
    rec = []
    cnt = 0
    for entry in response:
        if short in entry["name"]:
            rec.append(entry)
            cnt += 1
    if rec:
        if cnt > 1:
            pass
            #print(json.dumps(rec, indent=4, sort_keys=False, default=json_serial))
            #LOGGER.warning("%d %s %s is in JACS more than once", cnt, short, slide_code)
            #return None
    else:
        LOGGER.error("%s %s is not in JACS", short, slide_code)
        COUNT["Not in JACS"] += 1
        #dump(response)
        return None
    return rec


def produce_order():
    try:
        if "%" in ARG.DATASET:
            READ["PRIMARY"] = READ["PRIMARY"].replace("data_set=%s",
                                                      "data_set LIKE '" + ARG.DATASET + "'")
            print(READ["PRIMARY"])
            CURSOR["sage"].execute(READ["PRIMARY"])
        else:
            CURSOR["sage"].execute(READ["PRIMARY"], (ARG.DATASET,))
        rows = CURSOR["sage"].fetchall()
    except Exception as err:
        sql_error(err)
    LOGGER.info("LSMs in SAGE: %d", len(rows))
    COUNT["In SAGE"] = len(rows)
    order = dict()
    for row in tqdm(rows):
        path = row["jfs_path"] if row["jfs_path"] else row["path"]
        if not path:
            COUNT["No path"] += 1
            continue
        if not "/flylight/" in path:
            COUNT["Not flylight"] += 1
            continue
        if not os.path.exists(path):
            LOGGER.error("%s is not on filesystem", path)
            COUNT["Missing"] += 1
            continue
        try:
            _ = path.index(ARCHIVE_PATH)
        except:
            try:
                _ = path.index(NEW_PATH)
                #LOGGER.warning("%s was already moved", path)
                COUNT["Already moved"] += 1
            except:
                if "/groups/flylight" in path:
                    #LOGGER.warning("Not archived %s", path)
                    COUNT["Not archived"] += 1
                else:
                    LOGGER.error("Non-standard location for %s", path)
                    COUNT["Incorrect path"] += 1
            continue
        COUNT["Archived"] += 1
        LOGGER.debug(path)
        jacs = fetch_mongo(row["name"], row["slide_code"])
        if not jacs:
            continue
        jacs_err = False
        for smp in jacs:
            if smp["filepath"] != smp["files"]["LosslessStack"]:
                LOGGER.error("JACS data mismatch on %s", row["slide_code"])
                jacs_err = True
            if smp["filepath"] != path:
                #LOGGER.error("JACS path %s does not match %s", smp["filepath"], path)
                #jacs_err = True
                pass
        if jacs_err:
            continue
        COUNT["In JACS"] += 1
        newpath = path.replace(ARCHIVE_PATH, NEW_PATH)
        order[path] = []
        for smp in jacs:
            order[path].append({"path": newpath, "id": smp["_id"], "name": smp["name"],
                                "slide_code": row["slide_code"]})
    if order:
        dataset = ARG.DATASET.replace("%", "=")
        output = open(dataset + "_copy.cmd", "w")
        output2 = open(dataset + "_remove.cmd", "w")
        output2.write("exit\n")
        output3 = open(dataset + "_images.txt", "w")
        for path in sorted(order):
            output2.write("rm %s\n" % (path))
            output.write("install -D %s\t%s\n" % (path, order[path][0]["path"]))
            for itm in order[path]:
                output3.write("%s\t%s\t%s\t%s\n" % (path, itm["id"], itm["name"], itm["slide_code"]))
        output.close()
        output2.close()


def process_line(line):
    source,target = line.strip().split("\t")
    source = source.replace("install -D ", "")
    if ARG.REVERT:
        source, target = target, source
    if not os.path.exists(target):
        LOGGER.error("%s is not on filesystem", target)
        COUNT["Missing"] += 1
        sys.exit(-1)
    # SAGE
    try:
        #CURSOR["sage"].execute(READ["FROMLSM"], (ARG.DATASET, source))
        CURSOR["sage"].execute(READ["FROMLSM2"], (source,))
        rows = CURSOR["sage"].fetchall()
    except Exception as err:
        sql_error(err)
    if not rows:
        LOGGER.error("Missing from SAGE: %s", source)
        sys.exit(-1)
    if len(rows) > 1:
        LOGGER.error("Duplicates in SAGE: %s", source)
        sys.exit(-1)
    COUNT["In SAGE"] += 1
    row = rows[0]
    if ARG.REVERT:
        newurl = row["url"].replace(NEW_PATH, ARCHIVE_PATH)
    else:
        newurl = row["url"].replace(ARCHIVE_PATH, NEW_PATH)
    try:
        CURSOR["sage"].execute(WRITE["IMAGE"], (target, newurl, row['id']))
    except MySQLdb.Error as err:
        sql_error(err)
    COUNT["SAGE rows"] += 1
    try:
        CURSOR["sage"].execute(WRITE["MV"], (target, newurl, row['id']))
    except MySQLdb.Error as err:
        sql_error(err)
    COUNT["SAGE rows"] += 1
    # JACS
    jacs = fetch_mongo(row["name"], row["slide_code"])
    if not jacs:
        LOGGER.error("Missing from JACS: %s, %s", row["slide_code"], row["name"])
        sys.exit(-1)
    COUNT["In JACS"] += 1
    for smp in jacs:
        LOGGER.debug("Updating JACS %s %s to %s", smp["_id"], smp["slideCode"], target)
        smp["filepath"] = smp["files"]["LosslessStack"] = target
        if ARG.WRITE:
            data = DBM.image.update_one({"_id": smp["_id"]},
                                        {"$set": smp})
            COUNT["JACS rows"] += data.modified_count
        else:
            COUNT["JACS rows"] += 1
    COUNT["Updated"] += 1


def update_databases():
    with open(ARG.UPDATE) as input:
        lines = [ line.strip() for line in input ]
    LOGGER.info("LSMs in input file: %d", len(lines))
    COUNT["In file"] = len(lines)
    COUNT["JACS rows"] = COUNT["SAGE rows"] = COUNT["Updated"] = 0
    for line in tqdm(lines):
        process_line(line)
    if ARG.WRITE:
        CONN['sage'].commit()


def move_files():
    LOGGER.info("Processing data set %s", ARG.DATASET)
    if ARG.UPDATE:
        update_databases()
    else:
        produce_order()
    if ARG.UPDATE:
        print("Rows in update file:     %d" % (COUNT["In file"]))
        print("LSMs updated:            %d" % (COUNT["Updated"]))
        print("SAGE rows updated:       %d" % (COUNT["SAGE rows"]))
        print("JACS rows updated:       %d" % (COUNT["JACS rows"]))
    print("Rows in SAGE:            %d" % (COUNT["In SAGE"]))
    print("LSMs ready to move:      %d" % (COUNT["Archived"]))
    print("LSMs not in JACS:        %d" % (COUNT["Not in JACS"]))
    print("LSMs in JACS:            %d" % (COUNT["In JACS"]))
    print("LSMs already moved:      %d" % (COUNT["Already moved"]))
    print("LSMs not archived:       %d" % (COUNT["Not archived"]))
    print("No path in SAGE:         %d" % (COUNT["No path"]))
    print("Not FlyLight image:      %d" % (COUNT["Not flylight"]))
    print("Missing from filesystem: %d" % (COUNT["Missing"]))
    print("Non-standard paths:      %d" % (COUNT["Incorrect path"]))


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(description="Update ALPS release")
    PARSER.add_argument('--dataset', dest='DATASET', action='store',
                        help='Data set')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        choices=['dev', 'prod'], default='prod', help='Manifold')
    PARSER.add_argument('--update', dest='UPDATE', action='store',
                        default='', help='File to update SAGE and JACS')
    PARSER.add_argument('--revert', dest='REVERT', action='store_true',
                        default=False, help='Revert to original values')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Write')
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
    move_files()
    sys.exit(0)

