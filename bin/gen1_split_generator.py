import argparse
import json
import pprint
import re
import urllib2

RESPONDER = 'http://informatics-flask-dev.int.janelia.org:83/sage_responder/'
SUFFIX_SCORE = {'AV_01': 1,
                'AV_57': 1,
                'BB_04': 1,
                'BB_21': 1,
                'XA_21': 1,
                'XD_01': 1}


def processInput():
    if (VERBOSE):
        print "Fetching split halves"
    url = RESPONDER + "split_halves"
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
                if (DEBUG):
                    print "  Found cross %s x %s" % (ad, dbd)
                CROSSES.write("%s x %s\n" % (ad, dbd))
            elif ((not ad) or (not dbd)):
                NO_CROSSES.write("Missing AD/DBD for %s x %s\n" % (frag1, frag2))
    if (VERBOSE):
        print "Crosses found: %d" % (crosses)


def generateCross(fragdict, frag1, frag2):
    if (DEBUG):
        print "%s x %s" % (frag1, frag2)
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
                print "  Score %s x %s = %f" % (f1['line'], f2['line'],
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
                print "  Score %s x %s = %f" % (f1['line'], f2['line'],
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


def readLines(fragdict):
    linelist = []
    fragsFound = dict()
    frags_read = 0
    F = open(INPUT_FILE, 'r')
    for input_line in F:
        search_term = input_line.rstrip()
        new_term = ''
        if search_term.isdigit():
            st = convertVT('VT' + search_term.zfill(6))
            if (DEBUG):
                print "(Converted %s to %s)" % (search_term, st)
            search_term = st.split('_')[1]
        search_term = search_term.upper()
        new_term = '*' + search_term + '*'
        if (search_term in fragsFound):
            continue
        else:
            fragsFound[search_term] = 1
        frags_read = frags_read + 1
        if (DEBUG):
            print search_term + ' (' + new_term + ')'
        url = RESPONDER + "lines?name=" + new_term + '&_columns=name'
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
        print "Fragments read: %d" % (frags_read)
        n = len(linelist)
        print "Eligible line fragments: %d" % n
        combos = (n * (n - 1)) / 2
        print "Theoretical crosses: %d" % (combos)
    return(linelist)


def convertVT(vt):
    url = RESPONDER + "translatevt/" + vt
    req = urllib2.Request(url)
    req.add_header('Content-Type', 'application/json')
    try:
        response = urllib2.urlopen(req,)
    except urllib2.HTTPError, e:
        print 'Call to %s failed: %s.' % (url, e.code)
        pp.pprint(e.read())
    else:
        ld = json.load(response)
        if ('line_data' in ld):
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
    NO_CROSSES = open(INPUT_FILE + '.no_crosses.txt', 'w')
    processInput()
    CROSSES.close()
    NO_CROSSES.close()
