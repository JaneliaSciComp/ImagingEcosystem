''' run_discovery.py
    Run JACS discovery for slide codes in a file
'''

import argparse
import subprocess
import sys
import colorlog
from tqdm import tqdm


# JACS call details
# action=invokeOp&name=ComputeServer%3Aservice%3DSampleDataManager&methodIndex=17&arg0=20201211_41_A6
PREFIX = 'action=invokeOp&name=ComputeServer%3Aservice%3DSampleDataManager' \
         + '&methodIndex=17&arg0='
SUFFIX = '&argType=java.lang.String" http://jacs-data8.int.janelia.org:8180/jmx-console/HtmlAdaptor'


def process_slide_codes():
    codefile = open(ARG.CODES, "r")
    sent = 0
    slide_code = []
    for code in codefile:
        slide_code.append(code)
    codefile.close()
    for code in tqdm(slide_code):
        command = 'wget -v --post-data="%s%s%s' % (PREFIX, code.strip(), SUFFIX)
        subprocess.run(command, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        sent += 1
    print("Slide codes processed: %d" % (sent))


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(description='Run discovery for a list of slide codes')
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
