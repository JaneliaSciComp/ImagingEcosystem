#!/opt/python/bin/python2.7

import argparse
from datetime import datetime
import sys
import colorlog
import requests
import pytz
import time

# Configuration
CONFIG = {'config': {'url': 'http://config.int.janelia.org/'}}
COUNT = {'imageprop': 0, 'imagedatamv': 0, 'multiple': 0, 'notfound': 0, 'read': 0}


def call_responder(server, endpoint):
    """ Call a responder
        Keyword arguments:
        server: server
        endpoint: REST endpoint
    """
    url = CONFIG[server]['url'] + endpoint
    try:
        req = requests.get(url)
    except requests.exceptions.RequestException as err:
        logger.critical(err)
        sys.exit(-1)
    if req.status_code == 200:
        return req.json()
    else:
        logger.error('Status: %s', str(req.status_code))
        sys.exit(-1)


def initialize_program():
    global CONFIG
    dbc = call_responder('config', 'config/rest_services')
    CONFIG = dbc['config']


def check_samples():
    """ Find samples in process
    """
    response = call_responder('jacs', 'info/sample?totals=false&status=Processing')
    print("%d sample%s in process" % (len(response), '' if len(response) == 1 else 's'))
    newlist = sorted(response, key=lambda k: k['updatedDate'], reverse=True)
    pattern = '%Y-%m-%dT%H:%M:%S.%f%z'
    for sample in newlist:
        timestamp = sample['updatedDate']
        pdt = datetime.strptime(timestamp, pattern)
        now = datetime.utcnow().replace(tzinfo=pytz.UTC)
        elapsed = (now - pdt).total_seconds()
        hours, rem = divmod(elapsed, 3600)
        minutes, seconds = divmod(rem, 60)
        etime = "{:0>2}:{:0>2}:{:05.2f}".format(int(hours),int(minutes),seconds)
        owner = sample['ownerKey'].split(':')[1]
        print("%s\t%s\t%s\t%s" % (sample['name'], owner, timestamp, etime))
        sys.exit(0)

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(description="Delete image properties from a set of images")
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()

    logger = colorlog.getLogger()
    if ARG.DEBUG:
        logger.setLevel(colorlog.colorlog.logging.DEBUG)
    elif ARG.VERBOSE:
        logger.setLevel(colorlog.colorlog.logging.INFO)
    else:
        logger.setLevel(colorlog.colorlog.logging.WARNING)
    HANDLER = colorlog.StreamHandler()
    HANDLER.setFormatter(colorlog.ColoredFormatter())
    logger.addHandler(HANDLER)

    initialize_program()
    check_samples()
