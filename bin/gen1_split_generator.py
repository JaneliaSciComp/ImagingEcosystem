#!/usr/bin/env python
import argparse
from datetime import datetime
import os
import re
import sys
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


def is_gen1(line):
    m = re.search('^((BJD|GMR)_)*[0-9]+[A-H][0-9]{2}$', line)
    return(1 if m else 0)


def generate_score(line):
    if not is_gen1(line):
        return(1)
    suffix = line.rsplit('_', 2)
    last = "_".join(suffix[-2:])
    if (last in SUFFIX_SCORE):
        return(SUFFIX_SCORE[last])
    else:
        if last not in WARNED:
            logger.warning("Using default score for suffix %s", last)
            SUFFIX_SCORE[last] = 1
            WARNED[last] = 1
        return(1)


def generateCross(fragdict, frag1, frag2):
    logger.debug("Attempting cross %s-x-%s", frag1, frag2)
    max_score = {'score': 0, 'ad': '', 'dbd': ''}
    for f1 in fragdict[frag1]:
        # frag1 = AD, frag2 = DBD
        if f1['type'] == 'DBD':
            continue
        score = generate_score(f1['line'])
        for f2 in fragdict[frag2]:
            if f2['type'] == 'AD':
                continue
            final_score = score + generate_score(f2['line'])
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
        score = generate_score(f1['line'])
        for f2 in fragdict[frag2]:
            if f2['type'] == 'DBD':
                continue
            final_score = score + generate_score(f2['line'])
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


def read_lines(fragdict, converted_aline):
    inputlist = []
    linelist = []
    fragsFound = dict()
    frags_read = 0
    if ARG.ALINE:
        inputlist.append(converted_aline)
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
        search_option =  '&_columns=name'
        if not is_gen1(search_term):
            new_term = search_term
            search_option = ''
        if (search_term in fragsFound):
            frags_read = frags_read - 1
            continue
        else:
            fragsFound[search_term] = 1
        logger.debug(search_term + ' (' + new_term + ')')
        response = call_responder('sage', "lines?name=" + new_term + search_option)
        ld = response['line_data']
        if ld:
            for l in ld:
                if search_term == l['name']:
                    if search_term in fragdict:
                        linelist.append(search_term)
                        logger.info(l['name'])
                        break
                    elif is_gen1(search_term):
                        logger.warning("Line %s is not a valid split half", search_term)
                    else:
                        dtype = l['flycore_project'].split('-')[-1]
                        if dtype not in ['AD', 'DBD']:
                            logger.error("Non-Gen1 line %s is not an AD or DBD (%s)", search_term, l['flycore_project'])
                            break
                        fragdict[new_term] = []
                        fragdict[new_term].append({'line': new_term, 'type': dtype})
                        linelist.append(search_term)
                        logger.info("Non-Gen1 %s (%s)", search_term, dtype)
                        break
                if (('_' + search_term) not in l['name']):
                    continue
                fragment = re.sub('_[A-Z][A-Z]_[0-9][0-9]', '', l['name'])
                logger.debug(l['name'] + ' -> ' + fragment)
                if (fragment not in fragdict):
                    logger.warning("Fragment %s does not have an AD or DBD", fragment)
                    break
                linelist.append(fragment)
                logger.info(fragment)
                break
        else:
            logger.error("%s was not found in SAGE", search_term)
            if ARG.ALINE and (converted_aline == search_term):
                sys.exit(-1)
    linelist.sort()
    print("Fragments read: %d" % (frags_read))
    n = len(linelist)
    print("Eligible line fragments: %d" % n)
    combos = (n * (n - 1)) / 2
    if ARG.ALINE:
        combos = n - 1
    print("Theoretical crosses: %d" % (combos))
    return(linelist)


def process_input():
    logger.info("Fetching split halves")
    start_time = datetime.now()
    response = call_responder('sage', 'split_halves')
    fragdict = response['split_halves']
    logger.info("Found %d fragments with AD/DBDs", len(fragdict))
    # Convert A line
    converted_aline = ARG.ALINE
    if ARG.ALINE:
        original = ARG.ALINE.rstrip()
        if original.isdigit():
            vt = 'VT' + original.zfill(6)
            st = convertVT(vt)
            if (not st):
                logger.critical("Could not convert aline %s from VT to line", vt)
                sys.exit(-1)
            else:
                converted_aline = st.split('_')[1]
    # Find fragments
    logger.info("Processing line fragment list")
    fraglist = read_lines(fragdict, converted_aline)
    logger.info("Generating crosses")
    crosses = 0
    for idx, frag1 in enumerate(fraglist):
        for frag2 in fraglist[idx:]:
            if (frag1 == frag2):
                continue
            if converted_aline:
                if converted_aline not in frag1 and converted_aline not in frag2:
                    logger.debug("Cross does not contain A line %s", converted_aline)
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
    print("Crosses found: %d" % crosses)
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
        nocross_file = ARG.ALINE + '-' + ARG.INPUT + '.no_crosses.txt'
    	NO_CROSSES = open(nocross_file, 'w')
    else:
    	CROSSES = open(ARG.INPUT + '.crosses.txt', 'w')
    	FLYCORE = open(ARG.INPUT + '.flycore.xls', 'w')
        nocross_file = ARG.INPUT + '.no_crosses.txt'
    	NO_CROSSES = open(nocross_file, 'w')

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
    if os.path.getsize(nocross_file) < 1:
        os.remove(nocross_file)
    FLYCORE.close()
    NO_CROSSES.close()
