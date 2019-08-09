''' parse_czi.py
    Produce a tab-delimited list of samples that are in process
'''

import argparse
from datetime import datetime, timezone
import sys
import colorlog
import requests

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
        LOGGER.critical(err)
        sys.exit(-1)
    if req.status_code == 200:
        return req.json()
    LOGGER.error('Status: %s', str(req.status_code))
    sys.exit(-1)


def process_list(response):
    """ Call a responder
        Keyword arguments:
          response: response from JACS REST call
    """
    newlist = sorted(response, key=lambda k: k['updatedDate'], reverse=True)
    for sample in newlist:
        locpdt = datetime.strptime(sample['updatedDate'],
                                   TIME_PATTERN).replace\
                                   (tzinfo=timezone.utc).astimezone(tz=LOCAL_TIMEZONE)
        timestamp = locpdt.strftime(TIME_PATTERN).split('.')[0].replace('T', ' ')
        elapsed = (datetime.now().replace(tzinfo=LOCAL_TIMEZONE) - locpdt).total_seconds()
        days, hoursrem = divmod(elapsed, 3600 * 24)
        hours, rem = divmod(hoursrem, 3600)
        minutes, seconds = divmod(rem, 60)
        etime = "{:0>2}:{:0>2}:{:0>2}".format(int(hours), int(minutes), int(seconds))
        if days:
            etime = "%d day%s, %s" % (days, '' if days == 1 else 's', etime)
        owner = sample['ownerKey'].split(':')[1]
        response = call_responder('jacs', 'info/sample/search?name=' + sample['name'])
        print("%s\t%s\t%s\t%s\t%s\t%s" % (response[0]['line'],
                                          response[0]['slideCode'],
                                          response[0]['dataSet'],
                                          owner, timestamp, etime))


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
    DATA = call_responder('config', 'config/rest_services')
    CONFIG = DATA['config']
    check_samples()
