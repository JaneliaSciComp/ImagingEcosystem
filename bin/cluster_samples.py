''' cluster_samples.py
    Cluster a list of Workstation sample IDs by line parent fragments
'''

import argparse
import sys
import colorlog
import requests

# Configuration
CONFIG = {'config': {'url': 'http://config.int.janelia.org/'}}
SAMPLES = dict()


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


def generate_sample_dict(keylist):
    ''' Given a list of sample names, create a sample dictionary
        with line and parent fragments
        Keyword arguments:
          keylist: list of sample names
        Returns:
          Sample dictionary
    '''
    sdict = dict()
    for key in keylist:
        sid = key.replace('Sample#', '')
        response = call_responder('jacs', 'data/sample?sampleId=' + sid)
        line = response[0]['line']
        genotype = response[0]['flycoreAlias'].split('-x-')
        half = []
        for gen in genotype:
            #half.append(re.sub(r'_.._..$', '', gen))
            half.append('_'.join(gen.split('_')[0:2]))
        sdict[key] = {"line": line,
                      "parents": sorted(half)}
    return sdict


def add_parents(parents, key):
    ''' Add a samples parent fragments to a parent list
        Keyword arguments:
          parents: parents list
          key: sample name
    '''
    print(key)
    for par in SAMPLES[key]['parents']:
        parents[par] = 1


def process_list(sublist, parents, group_name):
    ''' Cluster a list of sample names by parents
        This is a recursize function
        Keyword arguments:
          sublist: list of sample names
          parents: list of parent fragments
          group_name: name of group
        Returns:
          A list of sample names in one group
    '''
    old_plen = len(parents)
    LOGGER.info("Creating %s", group_name)
    LOGGER.debug(sublist)
    LOGGER.debug(parents)
    LOGGER.debug("In process_list, list length=%d", len(sublist))
    clust = dict()
    if not parents:
        shash = dict((s, 1) for s in sublist)
        key = next(iter(shash))
        clust[key] = 1
        add_parents(parents, key)
        del sublist[key]
    removal = []
    for key in sublist:
        for par in SAMPLES[key]['parents']:
            if par in parents:
                add_parents(parents, key)
                clust[key] = 1
                removal.append(key)
                break
    for rem in removal:
        del sublist[rem]
    clustkeys = list(clust.keys())
    plen = len(parents)
    if sublist and (plen != old_plen):
        nextclust = process_list(sublist, parents, group_name)
        for nxt in nextclust:
            LOGGER.debug("Adding %s to clustkeys", nxt)
            clustkeys.append(nxt)
    LOGGER.debug("Parents: %s", ','.join(parents.keys()))
    LOGGER.debug("Return %s", ','.join(clustkeys))
    return list(clustkeys)


def process_file():
    ''' Process a file of sample names
    '''
    global SAMPLES # pylint: disable=W0603
    handle = open(ARG.FILE, 'r')
    skeys = []
    for line in handle:
        skeys.append(line.rstrip())
    handle.close()
    SAMPLES = generate_sample_dict(skeys)
    xxx = dict((s, 1) for s in list(SAMPLES.keys()))
    done = False
    group = dict()
    group_num = 1
    while not done:
        group_name = 'Group_%d' % (group_num)
        group_num += 1
        clust = process_list(xxx, dict(), group_name)
        group[group_name] = clust
        if not xxx:
            done = True
    for grp in group:
        handle = open(grp + '.txt', 'w')
        print(grp)
        for key in sorted(group[grp]):
            print("%s: %s (%s)" % (key, SAMPLES[key]['line'], ','.join(SAMPLES[key]['parents'])))
            handle.write(key + "\n")
        handle.close()


# *****************************************************************************

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(description="Cluster a list of Workstation samples by parents")
    PARSER.add_argument('--file', dest='FILE', default='', help='File containins sample IDs')
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

    process_file()
    sys.exit(0)
