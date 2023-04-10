''' This program will update the grammar for data sets
'''

import argparse
import json
import os
import sys
from types import SimpleNamespace
import colorlog
from pymongo import MongoClient
import requests
from tqdm import tqdm

# Database
DATABASE= {}
# Counters
COUNT = {"found": 0, "updated": 0}
#pylint: disable=W0718


def terminate_program(msg=None):
    """ Log an optional error to output, close files, and exit
        Keyword arguments:
          err: error message
        Returns:
           None
    """
    if msg:
        LOGGER.critical(msg)
    sys.exit(-1 if msg else 0)


def call_responder(server, endpoint):
    ''' Call a responder
        Keyword arguments:
          server: server
          endpoint: REST endpoint
        Returns:
          JSON response
    '''
    url = ((getattr(getattr(REST, server), "url") if server else "") if "REST" in globals() \
           else (os.environ.get('CONFIG_SERVER_URL') if server else "")) + endpoint
    try:
        req = requests.get(url, timeout=10)
    except requests.exceptions.RequestException as err:
        terminate_program(err)
    if req.status_code == 200:
        return req.json()
    terminate_program(f"Could not get response from {url}: {req.text}")
    return False


def create_config_object(config):
    """ Convert the JSON received from a configuration to an object
        Keyword arguments:
          config: configuration name
        Returns:
          Configuration object
    """
    data = (call_responder("config", f"config/{config}"))["config"]
    return json.loads(json.dumps(data), object_hook=lambda dat: SimpleNamespace(**dat))


def initialize_program():
    """ Initialize the program
        Keyword arguments:
          None
        Returns:
          None
    """
    dbconfig = create_config_object("db_config")
    # MongoDB
    LOGGER.info("Connecting to JACS MongoDB on %s", ARG.MANIFOLD)
    rwp = 'write' if ARG.WRITE else 'read'
    try:
        dbc = getattr(getattr(getattr(dbconfig, "jacs-mongo"), ARG.MANIFOLD), rwp)
        if ARG.MANIFOLD == 'prod':
            client = MongoClient(dbc.host, replicaSet=dbc.replicaset)
            DATABASE["JACS"] = client.jacs
        else:
            client = MongoClient(dbc.host)
            DATABASE["JACS"] = client['jacs-02142023']
            return
    except Exception as err:
        terminate_program(f"Could not connect to Mongo: {err}")
    try:
        if ARG.MANIFOLD != 'dev':
            DATABASE["JACS"].authenticate(dbc.user, dbc.password)
    except Exception as err:
        terminate_program(f"Could not authenticate to Mongo as {dbc.user}: {err}")


def update_grammar(dataset):
    """ Upodate the grammar for a single data set
        Keyword arguments:
          dataset: data set
        Returns:
          None
    """
    try:
        entry = DATABASE['JACS'].dataSet.find_one({"name": dataset})
    except Exception as err:
        terminate_program(f"Could not find data set {dataset} in JACS: {err}")
    if not entry:
        terminate_program(f"Data set {dataset} is not in JACS")
    COUNT["found"] += 1
    LOGGER.info("%s is %s", entry['name'], entry['sageGrammarPath'])
    entry['sageGrammarPath'] = "/".join([ARG.SOURCE, ARG.GRAMMAR])
    LOGGER.info("Update %s to %s", entry['name'], entry['sageGrammarPath'])
    if ARG.WRITE:
        data = DATABASE['JACS'].dataSet.update_one({"_id": entry["_id"]},
                                                   {"$set": entry})
        COUNT["updated"] += data.modified_count


def update_datasets():
    """ Upodate the grammar for one or more data sets
        Keyword arguments:
          None
        Returns:
          None
    """
    if ARG.FILE:
        dataset = []
        with open(ARG.FILE, "r", encoding="ascii") as instream:
            for line in instream:
                dataset.append(line.strip())
        for dset in tqdm(dataset):
            update_grammar(dset)
    elif ARG.DATASET:
        update_grammar(ARG.DATASET)
    else:
        terminate_program("You must specify a dataset or a file")
    print(f"Data sets found:   {COUNT['found']}")
    print(f"Data sets updated: {COUNT['updated']}")

# ****************************************************************************

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Update the grammar for a data set")
    PARSER.add_argument('--dataset', dest='DATASET', action='store',
                        help='Data set to update')
    PARSER.add_argument('--file', dest='FILE', action='store',
                        help='File containing data sets up update')
    PARSER.add_argument('--grammar', dest='GRAMMAR', action='store',
                        required=True, help='Grammar')
    PARSER.add_argument('--source', dest='SOURCE', action='store',
                        default='/misc/sc/pipeline/grammar',
                        help='Source directory for grammar')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'],
                        help='Manifold')
    PARSER.add_argument('--write', action='store_true', dest='WRITE',
                        default=False, help='Write to DynamoDB')
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
    REST = create_config_object("rest_services")
    initialize_program()
    update_datasets()
    terminate_program()
