#!/opt/python/bin/python2.7

import argparse
from datetime import datetime, timezone
import sys
import colorlog
import requests
import pytz
import time

# Configuration
CONFIG = {'config': {'url': 'http://config.int.janelia.org/'}}
LOCAL_TIMEZONE = datetime.now(timezone.utc).astimezone().tzinfo
TIME_PATTERN = '%Y-%m-%dT%H:%M:%S.%f%z'


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


def process_list(response):

    newlist = sorted(response, key=lambda k: k['updatedDate'], reverse=True)
    for sample in newlist:
        locpdt = datetime.strptime(sample['updatedDate'], TIME_PATTERN).replace(tzinfo=timezone.utc).astimezone(tz=LOCAL_TIMEZONE)
        timestamp = locpdt.strftime(TIME_PATTERN).split('.')[0].replace('T', ' ')
        elapsed = (datetime.now().replace(tzinfo=LOCAL_TIMEZONE) - locpdt).total_seconds()
        days, hoursrem = divmod(elapsed, 3600 * 24)
        hours, rem = divmod(hoursrem, 3600)
        minutes, seconds = divmod(rem, 60)
        if days:
            etime = "{:} day(s), {:0>2}:{:0>2}:{:0>2}".format(int(days), int(hours),int(minutes),int(seconds))
        else:
            etime = "{:0>2}:{:0>2}:{:0>2}".format(int(hours),int(minutes),int(seconds))
        owner = sample['ownerKey'].split(':')[1]
        # Link format: http://informatics-prod.int.janelia.org/cgi-bin/sample_search.cgi?sample_id=JRC_SS65810-20190426_49_A1
        print("%s\t%s\t%s\t%s" % (sample['name'], owner, timestamp, etime))


def check_samples():
    """ Find ssamples thast haven's completed processing
    """
    #response = call_responder('jacs', 'info/sample?totals=false&status=Queued')
    #print("%d sample%s queued" % (len(response), '' if len(response) == 1 else 's'))
    #process_list(response)
    response = call_responder('jacs', 'info/sample?totals=false&status=Processing')
    print("%d sample%s in process" % (len(response), '' if len(response) == 1 else 's'))
    process_list(response)


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
