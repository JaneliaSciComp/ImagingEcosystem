''' run_discovery.py
    Run JACS discovery for slide codes in a file
'''

import argparse
import os
import sys
import colorlog


# JACS call details
PREFIX = 'action=invokeOpByName&name=ComputeServer%3Aservice%3DSampleDataManager' \
         + '&methodName=runSampleDiscovery&arg0='
SUFFIX = '&argType=java.lang.String" http://jacs-data7.int.janelia.org:8180/jmx-console/HtmlAdaptor'


def process_slide_codes():
    codefile = open(ARG.CODES, "r")
    sent = 0
    for code in codefile:
        command = 'wget -v --post-data="%s%s%s' % (PREFIX, code, SUFFIX)
        os.system(command)
        sent += 1
    codefile.close()
    print("Slide codes processed: %d" % (sent))


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(description='Rundiscovery for a list of slide codes')
    PARSER.add_argument('--codes', dest='CODES', action='store',
                        help='File of slide codes')
    PARSER.add_argument('--verbose', action='store_true', dest='VERBOSE',
                        default=False, help='Turn on verbose output')
    PARSER.add_argument('--debug', action='store_true', dest='DEBUG',
                        default=False, help='Turn on debug output')
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
    if not ARG.CODES:
        LOGGER.error("Must specidy file of slide codes")
        sys.exit(-1)
    process_slide_codes()
    sys.exit(0)
