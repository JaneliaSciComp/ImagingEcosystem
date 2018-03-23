#!/usr/bin/env python
import argparse
from datetime import datetime
import json
import os
import re
import sys
import urllib2
import colorlog
import requests


# Configuration
SUFFIX_SCORE = {}
CONFIG = {'config': {'url': 'http://config.int.janelia.org/'}}
WARNED = {}
fcdict = dict()


def call_responder(server, endpoint):
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
    """ Get REST and score configuration
    """
    global CONFIG, SUFFIX_SCORE
    data = call_responder('config', 'config/rest_services')
    CONFIG = data['config']
    prog = os.path.basename(__file__).replace('.py', '')
    data = call_responder('config', 'config/' + prog)
    SUFFIX_SCORE = data['config']['score']


def generateScore(line):
    suffix = line.rsplit('_', 2)
    last = "_".join(suffix[-2:])
    if (last in SUFFIX_SCORE):
        return(SUFFIX_SCORE[last])
    else:
        if last not in WARNED:
            logger.error("No score found for suffix %s", last)
            WARNED[last] = 1
        return(0)


def generateCross(fragdict, frag1, frag2):
    logger.debug("%s-x-%s", frag1, frag2)
    max_score = {'score': 0, 'ad': '', 'dbd': ''}
    for f1 in fragdict[frag1]:
        # frag1 = AD, frag2 = DBD
        if f1['type'] == 'DBD':
            continue
        score = generateScore(f1['line'])
        for f2 in fragdict[frag2]:
            if f2['type'] == 'AD':
                continue
            final_score = score + generateScore(f2['line'])
            logger.debug("Score %s-x-%s = %f", f1['line'], f2['line'],
                         final_score)
            if final_score > max_score['score']:
                max_score['score'] = final_score
                max_score['ad'] = f1['line']
                max_score['dbd'] = f2['line']
    for f1 in fragdict[frag1]:
        # frag1 = DBD, frag2 = AD
        if f1['type'] == 'AD':
            continue
        score = generateScore(f1['line'])
        for f2 in fragdict[frag2]:
            if f2['type'] == 'DBD':
                continue
            final_score = score + generateScore(f2['line'])
            logger.debug("Score %s-x-%s = %f", f1['line'], f2['line'],
                         final_score)
            if (final_score > max_score['score']):
                max_score['score'] = final_score
                max_score['dbd'] = f1['line']
                max_score['ad'] = f2['line']
    return(max_score['ad'], max_score['dbd'])


def flycoreData(line):
    response = call_responder('flycore', "?request=linedata&line=" + line)
    fcdict[line] = response['linedata']


def good_cross(ad, dbd):
    logger.info("Found cross %s-x-%s", ad, dbd)
    CROSSES.write("%s-x-%s\n" % (ad, dbd))
    if (ad not in fcdict):
        flycoreData(ad)
    if (dbd not in fcdict):
        flycoreData(dbd)
    alias = ad + '-x-' + dbd
    pfrag = fcdict[ad]['fragment'] + '-x-' + fcdict[dbd]['fragment']
    FLYCORE.write("%s\t%s\t%s\t%s\t%s" % ('Tanya Wolff', '', alias, pfrag, ''))
    for half in (ad, dbd):
        FLYCORE.write("\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s"
                      % (fcdict[half]['A_Concat_Loc'],
                         fcdict[half]['__kp_UniqueID'], fcdict[half]['RobotID'],
                         fcdict[half]['Genotype_GSI_Name_PlateWell'],
                         fcdict[half]['Chromosome'], half, fcdict[half]['fragment'],
                         fcdict[half]['Production_Info'],
                         fcdict[half]['Quality_Control']))
    FLYCORE.write("\n")


def convertVT(vt):
    response = call_responder('sage', "translatevt/" + vt)
    if ('line_data' in response and len(response['line_data'])):
        return(response['line_data'][0]['line'])
    else:
        return('')


def read_lines(fragdict):
    inputlist = []
    linelist = []
    fragsFound = dict()
    frags_read = 0
    if ARG.ALINE:
        inputlist.append(ARG.ALINE)
    F = open(ARG.INPUT, 'r')
    for input_line in F:
        inputlist.append(input_line)
    F.close()
    for input_line in inputlist:
        frags_read = frags_read + 1
        search_term = input_line.rstrip()
        new_term = ''
        if search_term.isdigit():
            vt = 'VT' + search_term.zfill(6)
            st = convertVT(vt)
            if (not st):
                logger.warning("Could not convert %s to line", vt)
                NO_CROSSES.write("Could not convert %s to line" % (vt))
                continue
            logger.debug("Converted %s to %s)", search_term, st)
            search_term = st.split('_')[1]
        search_term = search_term.upper()
        new_term = '*\_' + search_term + '*'
        if (search_term in fragsFound):
            frags_read = frags_read - 1
            continue
        else:
            fragsFound[search_term] = 1
        logger.debug(search_term + ' (' + new_term + ')')
        response = call_responder('sage', "lines?name=" + new_term + '&_columns=name')
        ld = response['line_data']
        if ld:
            for l in ld:
                if (('_' + search_term) not in l['name']):
                    continue
                fragment = re.sub('_[A-Z][A-Z]_[0-9][0-9]', '', l['name'])
                if (ARG.DEBUG):
                    logger.info(l['name'] + ' -> ' + fragment)
                if (fragment not in fragdict):
                    logger.warning("No AD or DBD found for fragment %s", fragment)
                    continue
                linelist.append(fragment)
                break
    linelist.sort()
    print "Fragments read: %d" % (frags_read)
    n = len(linelist)
    print "Eligible line fragments: %d" % n
    combos = (n * (n - 1)) / 2
    print "Theoretical crosses: %d" % (combos)
    return(linelist)


def process_input():
    logger.info("Fetching split halves")
    start_time = datetime.now()
    response = call_responder('sage', 'split_halves')
    fragdict = response['split_halves']
    logger.info("Found %d fragments with AD/DBDs", len(fragdict))
    logger.info("Processing line fragment list")
    fraglist = read_lines(fragdict)
    logger.info("Generating crosses")
    crosses = 0
    for idx, frag1 in enumerate(fraglist):
        for frag2 in fraglist[idx:]:
            if (frag1 == frag2):
                continue
            if ARG.ALINE:
                if ARG.ALINE not in frag1 and ARG.ALINE not in frag2:
                    logger.warning("AD or DBD is not A line %s", ARG.ALINE)
                    continue
            (ad, dbd) = generateCross(fragdict, frag1, frag2)
            if (ad and dbd):
                crosses += 1
                good_cross(ad, dbd)
            elif ((not ad) or (not dbd)):
                what = "AD and DBD"
                if ad:
                    what = "AD"
                elif dbd:
                    what = "DBD"
                logger.warning("Missing %s for %s-x-%s", what, frag1, frag2)
                NO_CROSSES.write("Missing %s for %s-x-%s\n" % (what, frag1,
                                                                   frag2))
    stop_time = datetime.now()
    print "Crosses found: %d" % crosses
    logger.info("Elapsed time: %s", (stop_time - start_time))


# -----------------------------------------------------------------------------


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description='Generate Gen1 initial splits')
    PARSER.add_argument('--file', dest='INPUT', required=True, default='', help='Input file')
    PARSER.add_argument('--aline', dest='ALINE', default='', help='A line')
    PARSER.add_argument('--verbose', action='store_true', dest='VERBOSE',
                        default=False, help='Turn on verbose output')
    PARSER.add_argument('--debug', action='store_true', dest='DEBUG',
                        default=False, help='Turn on debug output')
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
    if (ARG.ALINE):
    	CROSSES = open(ARG.ALINE + '-' + ARG.INPUT + '.crosses.txt', 'w')
    	FLYCORE = open(ARG.ALINE + '-' + ARG.INPUT + '.flycore.xls', 'w')
    	NO_CROSSES = open(ARG.ALINE + '-' + ARG.INPUT + '.no_crosses.txt', 'w')
    else:
    	CROSSES = open(ARG.INPUT + '.crosses.txt', 'w')
    	FLYCORE = open(ARG.INPUT + '.flycore.xls', 'w')
    	NO_CROSSES = open(ARG.INPUT + '.no_crosses.txt', 'w')

    for h in ('Who', '#', 'Alias', 'Pfrag'):
        FLYCORE.write("%s\t" % (h))
    FLYCORE.write('IS')
    for i in (1, 2):
        for h in ('__flipper_flystocks_stock::RACK_LOCATION',
                  'StockFinder::__kp_UniqueID', 'StockFinder::RobotID',
                  'StockFinder::Genotype_GSI_Name_PlateWell',
                  'StockFinder::Chromosome', 'StockFinder::Stock_Name',
                  'StockFinder::fragment', 'StockFinder::Production_Info',
                  'StockFinder::Quality_Control'):
            FLYCORE.write("\t%s" % (h))
    FLYCORE.write("\n")
    process_input()
    CROSSES.close()
    FLYCORE.close()
    NO_CROSSES.close()
