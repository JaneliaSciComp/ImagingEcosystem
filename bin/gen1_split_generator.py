#!/usr/bin/env python
import argparse
from datetime import datetime
import json
import os
from os.path import expanduser
import pwd
import re
import select
import sys
import colorlog
import requests


# Configuration
SUFFIX_SCORE = {}
CONFIG = {'config': {'url': 'http://config.int.janelia.org/'}}
VTCACHE = {}
WARNED = {}
fcdict = dict()

def call_responder(server, endpoint, post=''):
    url = CONFIG[server]['url'] + endpoint
    try:
        if post:
            req = requests.post(url, post)
        else:
            req = requests.get(url)
    except requests.exceptions.RequestException as err:
        logger.critical(err)
        sys.exit(-1)
    if req.status_code == 200:
        return req.json()
    elif req.status_code == 404:
    	return ''
    else:
        try:
            logger.critical('%s: %s', str(req.status_code), req.json()['rest']['message'])
        except:
            logger.critical('%s: %s', str(req.status_code), req.text)
        sys.exit(-1)


def find_username(userid):
    if not userid:
        userid = pwd.getpwuid(os.getuid())[0]
    userdata = call_responder('config', 'config/workday/' + userid)
    if userdata:
        return userdata['config']['first'] + ' ' + userdata['config']['last']
    else:
        return userid


def initialize_program(name):
    """ Get REST and score configuration
    """
    global CONFIG, SUFFIX_SCORE, ORDERNAME
    data = call_responder('config', 'config/rest_services')
    CONFIG = data['config']
    prog = os.path.basename(__file__).replace('.py', '')
    data = call_responder('config', 'config/' + prog)
    SUFFIX_SCORE = data['config']['score']
    ORDERNAME = find_username(name)
    logger.info("Will use name " + ORDERNAME + " on output spreadsheet")


def is_vt(line):
    if line.upper().startswith('VT'):
        return(1)
    return(1 if line.isdigit() else 0)


def is_gen1(line):
    m = re.search('^((BJD|GMR)_)*[0-9]+[A-H][0-9]{2}_[A-Z]{2}_[0-9]{2}$', line, re.IGNORECASE)
    return(1 if m else 0)


def is_gen1_fragment(line):
    m = re.search('^((BJD|GMR)_)*[0-9]+[A-H][0-9]{2}$', line, re.IGNORECASE)
    return(1 if m else 0)


def convert_gen1(gen1):
	gen1 = gen1.upper()
	if re.search('^((BJD|GMR)_)', gen1):
		gen1 = gen1.split('_' )[1]
	gen1 = re.sub('_.*', '', gen1)
	return(gen1)


def translate_vt(vt):
    response = call_responder('sage', "translatevt/" + vt)
    if ('line_data' in response and len(response['line_data'])):
        # Add to config
        platewell = response['line_data'][0]['line'].split('_')[1]
        vtdict = {"config": json.dumps(platewell)}
        call_responder('config', 'importjson/vt_conversion/' + vt, vtdict)
        # Returns qualified line ("BJD_112C03_BB_21")
        return(response['line_data'][0]['line'])
    else:
        return('')


def convert_vt(search_term):
    search_term = search_term.upper()
    search_term = search_term.replace('VT', '')
    vt = 'VT' + search_term.zfill(6)
    st = ''
    if vt in VTCACHE:
        st = VTCACHE[vt]
    else:
        st = translate_vt(vt)
        if (not st):
            logger.warning("Could not convert %s to line", vt)
            NO_CROSSES.write("Could not convert %s to line" % (vt))
            return()
        st = st.split('_')[1]
        VTCACHE[vt] = st
    logger.debug("Converted %s to %s", vt, st)
    return(st)

    
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


def set_max_score(ad, dbd, final_score, max_score):
    logger.debug("Score %s-x-%s = %f", ad, dbd, final_score)
    if final_score > max_score['score']:
        max_score['score'] = final_score
        max_score['ad'] = ad
        max_score['dbd'] = dbd


def generate_cross(fragdict, frag1, frag2):
    logger.debug("Attempting cross %s-x-%s", frag1, frag2)
    max_score = {'score': -1, 'ad': '', 'dbd': ''}
    for f1 in fragdict[frag1]:
        # frag1 = AD, frag2 = DBD
        if f1['type'] == 'DBD' or f1['driver'] != 'GAL4':
            continue
        score = generate_score(f1['line'])
        ls1 = f1['line'].rpartition('_')[-1]
        for f2 in fragdict[frag2]:
            if f2['type'] == 'AD' or f2['driver'] != 'GAL4':
                continue
            ls2 = f2['line'].rpartition('_')[-1]
            if (ls1 == ls2):
                logger.error("Same landing site for %s and %s" % (f1['line'], f2['line']))
                continue
            final_score = score + generate_score(f2['line'])
            set_max_score(f1['line'], f2['line'], final_score, max_score)
            if ARG.ALL:
                good_cross(f1['line'], f2['line'])
    for f1 in fragdict[frag1]:
        # frag1 = DBD, frag2 = AD
        if f1['type'] == 'AD' or f1['driver'] != 'GAL4':
            continue
        score = generate_score(f1['line'])
        ls1 = f1['line'].rpartition('_')[-1]
        for f2 in fragdict[frag2]:
            if f2['type'] == 'DBD' or f2['driver'] != 'GAL4':
                continue
            ls2 = f2['line'].rpartition('_')[-1]
            if (ls1 == ls2):
                logger.error("Same landing site for %s and %s" % (f1['line'], f2['line']))
                continue
            final_score = score + generate_score(f2['line'])
            set_max_score(f2['line'], f1['line'], final_score, max_score)
            if ARG.ALL:
                good_cross(f2['line'], f1['line'])
    return(max_score['ad'], max_score['dbd'])


def flycoreData(line):
    response = call_responder('flycore', "?request=linedata&line=" + line)
    if 'linedata' in response:
        fcdict[line] = response['linedata']
    else:
        logger.critical("No connectivity to FlyCore responder")
        sys.exit(-1)


def good_cross(ad, dbd):
    logger.info("Found cross %s-x-%s", ad, dbd)
    CROSSES.write("%s-x-%s\n" % (ad, dbd))
    if (ad not in fcdict):
        flycoreData(ad)
    if (dbd not in fcdict):
        flycoreData(dbd)
    alias = ad + '-x-' + dbd
    pfrag = fcdict[ad]['fragment'] + '-x-' + fcdict[dbd]['fragment']
    FLYCORE.write("%s\t%s\t%s\t%s\t%s" % (ORDERNAME, '', alias, pfrag, ''))
    for half in (ad, dbd):
        FLYCORE.write("\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s"
                      % (fcdict[half]['A_Concat_Loc'],
                         fcdict[half]['__kp_UniqueID'],
                         fcdict[half]['RobotID'],
                         fcdict[half]['Genotype_GSI_Name_PlateWell'],
                         fcdict[half]['Chromosome'], half,
                         fcdict[half]['fragment'],
                         fcdict[half]['Production_Info'],
                         fcdict[half]['Quality_Control']))
    FLYCORE.write("\n")


def search_for_ad_dbd(aline, search_term, new_term, search_option,
                      linelist, fragdict, fragsFound):
    found = 0
    if is_gen1_fragment(search_term):
        m = re.search('([0-9]+)', search_term)
        number = int(m.groups()[0])
        extended = 'GMR_' if number < 100 else 'BJD_'
        extended += search_term
        if extended in fragdict:
            linelist.append(extended)
            logger.info("Found %s in split half list", extended)
            found = 1
    if not found:
        response = call_responder('sage', "lines?name=" + new_term +
                                  search_option)
        if 'line_data' not in response:
            logger.error("%s was not found in SAGE", search_term)
            sys.exit(-1)
        ld = response['line_data']
        if ld:
            for l in ld:
                if search_term == l['name']:
                    if is_gen1_fragment(search_term):
                        logger.warning("Line %s is not a valid split half",
                                       search_term)
                    else:
                        # Non Gen-1
                        dtype = l['flycore_project'].split('-')[-1]
                        if dtype not in ['AD', 'DBD']:
                            logger.error("Non-Gen1 line %s is not an AD or DBD (%s)", search_term, l['flycore_project'])
                            break
                        fragdict[search_term] = []
                        fragdict[search_term].append({'line': new_term, 'type': dtype})
                        linelist.append(search_term)
                        fragsFound[search_term] = search_term
                        logger.info("Non-Gen1 %s (%s)", search_term, dtype)
                        break
                if (('_' + search_term) not in l['name']):
                    continue
                fragment = re.sub('_[A-Z][A-Z]_[0-9][0-9]', '', l['name'])
                logger.debug(l['name'] + ' -> ' + fragment)
                if (fragment not in fragdict):
                    logger.warning("Fragment %s does not have an AD or DBD", fragment)
                    break
                fragsFound[search_term] = fragment
                linelist.append(fragment)
                logger.info(fragment)
                break
        else:
            logger.error("%s was not found in SAGE", search_term)
            if ARG.ALINE and (aline == search_term):
                sys.exit(-1)


def read_lines(fragdict, aline):
    global VTCACHE
    inputlist = []
    linelist = []
    fragsFound = dict()
    frags_read = 0
    if ARG.ALINE:
        inputlist.append(aline)
    filename = ARG.FILE if ARG.FILE else ''
    if (not filename) and (not select.select([sys.stdin,],[],[],0.0)[0]):
        logger.critical('You must either specify a file or pass data in through STDIN')
        sys.exit(-1)        
    try:
        filehandle = open(filename, "r") if filename else sys.stdin
    except Exception as e:
        logger.critical('Failed to open input: '+ str(e))
        sys.exit(-1)
    for input_line in filehandle:
        inputlist.append(input_line)
    if filehandle is not sys.stdin:
        filehandle.close()
    # Get cached VT conversions
    response = call_responder('config', 'config/vt_conversion')
    VTCACHE = response['config']
    logger.info("Found %s entries in VT cache", len(VTCACHE))
    # Process input file
    for input_line in inputlist:
        search_term = input_line.rstrip()
        if not len(search_term):
            continue
        frags_read += 1
        new_term = ''
        if is_vt(search_term):
            search_term = convert_vt(search_term)
            if not search_term:
                continue
        if is_gen1_fragment(search_term) or is_gen1(search_term):
            search_term = convert_gen1(search_term)
            new_term = '*\_' + search_term + '*'
            search_option = '&_columns=name'
        else:
            new_term = search_term
            search_option = ''
        if (search_term in fragsFound):
            logger.warning("Ignoring duplicate %s", search_term)
            frags_read = frags_read - 1
            continue
        else:
            fragsFound[search_term] = 1
        logger.debug(search_term + ' (' + new_term + ')')
        search_for_ad_dbd(aline, search_term, new_term, search_option,
                          linelist, fragdict, fragsFound)
    linelist.sort()
    print("Fragments read: %d" % (frags_read))
    n = len(linelist)
    print("Eligible line fragments: %d" % n)
    combos = (n * (n - 1)) / 2
    if ARG.ALINE:
        combos = n - 1
    print("Theoretical crosses: %d" % (combos))
    return(linelist, combos)


def process_input():
    logger.info("Fetching split halves")
    start_time = datetime.now()
    response = call_responder('sage', 'split_halves')
    fragdict = response['split_halves']
    logger.info("Found %d fragments with AD/DBDs", len(fragdict))
    # Convert A line
    aline = ARG.ALINE.upper()
    if ARG.ALINE:
        original = ARG.ALINE.rstrip()
        if is_vt(original):
            aline = convert_vt(original)
            if not aline:
                sys.exit(-1)
    # Find fragments
    logger.info("Processing line fragment list")
    (fraglist, combos) = read_lines(fragdict, aline)
    if not combos:
    	logger.critical("No theoretical crosses found")
    	sys.exit(-1)
    logger.info("Generating crosses")
    crosses = 0
    for idx, frag1 in enumerate(fraglist):
        for frag2 in fraglist[idx:]:
            if (frag1 == frag2):
                continue
            if aline:
                if aline not in frag1 and aline not in frag2:
                    logger.debug("Cross does not contain A line %s", aline)
                    continue
            (ad, dbd) = generate_cross(fragdict, frag1, frag2)
            if (ad and dbd):
                crosses += 1
                if not ARG.ALL:
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
    print("Crosses found: %d/%d (%.2f%%)" % (crosses, combos, float(crosses) / float(combos) * 100.0))
    logger.info("Elapsed time: %s", (stop_time - start_time))


# -----------------------------------------------------------------------------


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description='Generate Gen1 initial splits')
    PARSER.add_argument('--file', dest='FILE', default='', help='Input file')
    PARSER.add_argument('--aline', dest='ALINE', default='', help='A line')
    PARSER.add_argument('--all', action='store_true', dest='ALL',
                        default=False, help='Output all cross combinations')
    PARSER.add_argument('--name', dest='NAME', default='', help='Name to use for the order')
    PARSER.add_argument('--task', dest='TASK', default='', help='Task name')
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

    initialize_program(ARG.NAME)
    fname = ARG.FILE if ARG.FILE else ARG.TASK if ARG.TASK else 'STDIN'
    name_insert = prefix = ''
    if ARG.ALINE:
        prefix = ARG.ALINE + '-'
    if ARG.ALL:
        name_insert += '-ALL'
    CROSSES = open(prefix + fname + name_insert + '.crosses.txt', 'w')
    FLYCORE = open(prefix + fname + name_insert + '.flycore.xls', 'w')
    nocross_file = prefix + fname + name_insert + '.no_crosses.txt'
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
    NO_CROSSES.close()
    if os.path.getsize(nocross_file) < 1:
        os.remove(nocross_file)
    FLYCORE.close()
