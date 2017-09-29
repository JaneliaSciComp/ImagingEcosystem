import argparse
from datetime import datetime
import json
import pprint
import re
import urllib2

SAGE_RESPONDER = 'http://informatics-flask.int.janelia.org:83/' + \
    'sage_responder/'
FLYCORE_RESPONDER = 'http://informatics-prod.int.janelia.org/' + \
    'cgi-bin/flycore_responder.cgi'
SUFFIX_SCORE = {'AV_01': 1,
                'AV_57': 1,
                'BB_04': 1,
                'BB_21': 1,
                'XA_21': 1,
                'XD_01': 1}
fcdict = dict()


def processInput():
    if (VERBOSE):
        print "Fetching split halves"
    start_time = datetime.now()
    url = SAGE_RESPONDER + "split_halves"
    req = urllib2.Request(url)
    req.add_header('Content-Type', 'application/json')
    try:
        response = urllib2.urlopen(req,)
    except urllib2.HTTPError, e:
        print 'Call to %s failed: %s.' % (url, e.code)
        pp.pprint(e.read())
    else:
        fd = json.load(response)
        fragdict = fd['split_halves']
    if (VERBOSE):
        print "Found %d fragments with AD/DBDs" % (len(fragdict))
        print "Processing line fragment list"
    fraglist = readLines(fragdict)
    if (VERBOSE):
        print "Generating crosses"
    crosses = 0
    for idx, frag1 in enumerate(fraglist):
        for frag2 in fraglist[idx:]:
            if (frag1 == frag2):
                continue
            (ad, dbd) = generateCross(fragdict, frag1, frag2)
            if (ad and dbd):
                crosses += 1
                goodCross(ad, dbd)
            elif ((not ad) or (not dbd)):
                NO_CROSSES.write("Missing AD/DBD for %s-x-%s\n" % (frag1,
                                                                   frag2))
    stop_time = datetime.now()
    if (VERBOSE):
        print "  Crosses found: %d" % (crosses)
        print "Elapsed time: ", stop_time - start_time


def generateCross(fragdict, frag1, frag2):
    if (DEBUG):
        print "%s-x-%s" % (frag1, frag2)
    max_score = {'score': 0, 'ad': '', 'dbd': ''}
    for f1 in fragdict[frag1]:
        # frag1 = AD, frag2 = DBD
        if (f1['type'] == 'DBD'):
            continue
        score = generateScore(f1['line'])
        for f2 in fragdict[frag2]:
            if (f2['type'] == 'AD'):
                continue
            final_score = score + generateScore(f2['line'])
            if (DEBUG):
                print "  Score %s-x-%s = %f" % (f1['line'], f2['line'],
                                                final_score)
            if (final_score > max_score['score']):
                max_score['score'] = final_score
                max_score['ad'] = f1['line']
                max_score['dbd'] = f2['line']
    for f1 in fragdict[frag1]:
        # frag1 = DBD, frag2 = AD
        if (f1['type'] == 'AD'):
            continue
        score = generateScore(f1['line'])
        for f2 in fragdict[frag2]:
            if (f2['type'] == 'DBD'):
                continue
            final_score = score + generateScore(f2['line'])
            if (DEBUG):
                print "  Score %s-x-%s = %f" % (f1['line'], f2['line'],
                                                final_score)
            if (final_score > max_score['score']):
                max_score['score'] = final_score
                max_score['dbd'] = f1['line']
                max_score['ad'] = f2['line']
    return(max_score['ad'], max_score['dbd'])


def generateScore(line):
    suffix = line.rsplit('_', 2)
    last = "_".join(suffix[-2:])
    if (last in SUFFIX_SCORE):
        return(SUFFIX_SCORE[last])
    else:
        return(0)


def goodCross(ad, dbd):
    if (DEBUG):
        print "  Found cross %s-x-%s" % (ad, dbd)
    CROSSES.write("%s-x-%s\n" % (ad, dbd))
    if (ad not in fcdict):
        flycoreData(ad)
    if (dbd not in fcdict):
        flycoreData(dbd)
    alias = ad + '-x-' + dbd
    pfrag = fcdict[ad]['fragment'] + '-x-' + fcdict[dbd]['fragment']
    FLYCORE.write("%s\t%s\t%s\t%s\t%s" % ('Tanya Wolff', '', alias, pfrag, ''))
    for w in (ad, dbd):
        FLYCORE.write("\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s"
                      % (fcdict[w]['location']['RACK_LOCATION'],
                         fcdict[w]['__kp_UniqueID'], fcdict[w]['RobotID'],
                         fcdict[w]['Genotype_GSI_Name_PlateWell'],
                         fcdict[w]['Chromosome'], w, fcdict[w]['fragment'],
                         fcdict[w]['Production_Info'],
                         fcdict[w]['Quality_Control']))
    FLYCORE.write("\n")


def flycoreData(line):
    url = FLYCORE_RESPONDER + "?request=linedata&line=" + line
    req = urllib2.Request(url)
    req.add_header('Content-Type', 'application/json')
    try:
        response = urllib2.urlopen(req,)
    except urllib2.HTTPError, e:
        print 'Call to %s failed: %s.' % (url, e.code)
        pp.pprint(e.read())
        return
    else:
        fd = json.load(response)
        fcdict[line] = fd['linedata']
    url = FLYCORE_RESPONDER + "?request=location&robot_id=" + \
        fcdict[line]['RobotID']
    req = urllib2.Request(url)
    req.add_header('Content-Type', 'application/json')
    try:
        response = urllib2.urlopen(req,)
    except urllib2.HTTPError, e:
        print 'Call to %s failed: %s.' % (url, e.code)
        pp.pprint(e.read())
    else:
        fd = json.load(response)
        fcdict[line]['location'] = fd['location']


def readLines(fragdict):
    linelist = []
    fragsFound = dict()
    frags_read = 0
    F = open(INPUT_FILE, 'r')
    for input_line in F:
        frags_read = frags_read + 1
        search_term = input_line.rstrip()
        new_term = ''
        if search_term.isdigit():
            vt = 'VT' + search_term.zfill(6)
            st = convertVT(vt)
            if (not st):
                print "Could not convert %s to line" % (vt)
                NO_CROSSES.write("Could not convert %s to line" % (vt))
                continue
            if (DEBUG):
                print "(Converted %s to %s)" % (search_term, st)
            search_term = st.split('_')[1]
        search_term = search_term.upper()
        new_term = '*\_' + search_term + '*'
        if (search_term in fragsFound):
            frags_read = frags_read - 1
            continue
        else:
            fragsFound[search_term] = 1
        if (DEBUG):
            print search_term + ' (' + new_term + ')'
        url = SAGE_RESPONDER + "lines?name=" + new_term + '&_columns=name'
        req = urllib2.Request(url)
        req.add_header('Content-Type', 'application/json')
        try:
            response = urllib2.urlopen(req,)
        except urllib2.HTTPError, e:
            print 'Call to %s failed: %s.' % (url, e.code)
            pp.pprint(e.read())
        else:
            ld = json.load(response)
            ld = ld['line_data']
            for l in ld:
                if (('_' + search_term) not in l['name']):
                    continue
                fragment = re.sub('_[A-Z][A-Z]_[0-9][0-9]', '', l['name'])
                if (DEBUG):
                    print '  ' + l['name'] + ' -> ' + fragment
                if (fragment not in fragdict):
                    print "  No AD or DBD found for fragment %s" % (fragment)
                    continue
                linelist.append(fragment)
                break
    F.close()
    linelist.sort()
    if (VERBOSE):
        print "  Fragments read: %d" % (frags_read)
        n = len(linelist)
        print "  Eligible line fragments: %d" % n
        combos = (n * (n - 1)) / 2
        print "  Theoretical crosses: %d" % (combos)
    return(linelist)


def convertVT(vt):
    url = SAGE_RESPONDER + "translatevt/" + vt
    req = urllib2.Request(url)
    req.add_header('Content-Type', 'application/json')
    try:
        response = urllib2.urlopen(req,)
    except urllib2.HTTPError, e:
        print 'Call to %s failed: %s.' % (url, e.code)
        pp.pprint(e.read())
    else:
        ld = json.load(response)
        if ('line_data' in ld and len(ld['line_data'])):
            return(ld['line_data'][0]['line'])
        else:
            return('')


# -----------------------------------------------------------------------------


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Generate Gen1 initial splits')
    parser.add_argument('-file', dest='file', default='', help='Input file')
    parser.add_argument('-verbose', action='store_true', dest='verbose',
                        default=False, help='Turn on verbose output')
    parser.add_argument('-debug', action='store_true', dest='debug',
                        default=False, help='Turn on debug output')
    args = parser.parse_args()
    INPUT_FILE = args.file
    VERBOSE = args.verbose
    DEBUG = args.debug
    if DEBUG:
        VERBOSE = True
    pp = pprint.PrettyPrinter(indent=4)
    CROSSES = open(INPUT_FILE + '.crosses.txt', 'w')
    FLYCORE = open(INPUT_FILE + '.flycore.xls', 'w')
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
    NO_CROSSES = open(INPUT_FILE + '.no_crosses.txt', 'w')
    processInput()
    CROSSES.close()
    FLYCORE.close()
    NO_CROSSES.close()
