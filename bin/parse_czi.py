''' parse_czi.py
    Parse a CZI file and optionally update SAGE with image properties
'''

import pprint
import re
import bioformats
import javabridge
import xmltodict

TRANSLATE = {'Experimenter': {'@UserName': 'created_by'},
             'Instrument': {'@Gain': 'lsm_detection_channel_'},
             'Image': {'AcquisitionDate': 'capture_date', '@Name': 'microscope_filename'},
             'Pixels': {'@PhysicalSizeX' : 'voxel_size_x', '@PhysicalSizeY' : 'voxel_size_y',
                        '@PhysicalSizeZ' : 'voxel_size_z',
                        '@SizeC': 'num_channels', '@SizeX' : 'dimension_x',
                        '@SizeY' : 'dimension_y', '@SizeZ' : 'dimension_z'},
             'Annotation': {'Experiment|AcquisitionBlock|AcquisitionModeSetup|BitsPerSample': \
                                'bits_per_sample',
                            'Experiment|AcquisitionBlock|AcquisitionModeSetup|Objective': \
                                'objective',
                            'Information|Instrument|Microscope|System': 'microscope_model',
                           },
             'Array': {'XXXInformation|Image|Channel|DigitalGain': 'lsm_detection_channel_',
                       'Experiment|AcquisitionBlock|MultiTrackSetup|TrackSetup|Name': \
                           'lsm_illumination_channel_',
                      },
            }
SUFFIX = {'Information|Image|Channel|DigitalGain': '_detector_gain',
          '@Gain': '_detector_gain',
          'Experiment|AcquisitionBlock|MultiTrackSetup|TrackSetup|Name': '_name',
         }


def initialize():
    ''' Call a responder
    '''
    javabridge.start_vm(class_path=bioformats.JARS, run_headless=True)


def parse_experimenter(j, record):
    ''' Parse the Experimenter block
        Keyword arguments:
          j: JSON OME dictionary
          record: image properties record
    '''
    if 'Experimenter' in j['OME']:
        for key in TRANSLATE['Experimenter']:
            if key in j['OME']['Experimenter']:
                record[TRANSLATE['Experimenter'][key]] = j['OME']['Experimenter'][key]


def parse_instrument(j, record):
    ''' Parse the Instrument block
        Keyword arguments:
          j: JSON OME dictionary
          record: image properties record
    '''
    if 'Instrument' in j['OME'] and 'Detector' in j['OME']['Instrument']:
        chan = 1
        for detector in j['OME']['Instrument']['Detector']:
            key = '@Gain'
            reckey = TRANSLATE['Instrument'][key] + str(chan)
            if key in SUFFIX:
                reckey += SUFFIX[key]
            record[reckey] = detector[key]
            chan += 1


def parse_image(j, record):
    ''' Parse the Image block
        Keyword arguments:
          j: JSON OME dictionary
          record: image properties record
    '''
    if 'Image' in j['OME']:
        for key in TRANSLATE['Image']:
            if key in j['OME']['Image']:
                record[TRANSLATE['Image'][key]] = j['OME']['Image'][key]
    if 'capture_date' in record:
        record['capture_date'] = record['capture_date'].replace('T', ' ')
        record['capture_date'] = re.sub(r'\.\d+', '', record['capture_date'])
    if 'Pixels' in  j['OME']['Image']:
        for key in TRANSLATE['Pixels']:
            record[TRANSLATE['Pixels'][key]] = j['OME']['Image']['Pixels'][key]


def parse_structuredannotations(j, record):
    ''' Parse the StructuredAnnotations block
        Keyword arguments:
          j: JSON OME dictionary
          record: image properties record
    '''
    for ann in j['OME']['StructuredAnnotations']['XMLAnnotation']:
        this = ann['Value']['OriginalMetadata']
        key = this['Key']
        value = this['Value']
        value = re.sub(r'[\[\]]', '', this['Value'])
        if key in TRANSLATE['Annotation']:
            record[TRANSLATE['Annotation'][key]] = value
        elif key in TRANSLATE['Array']:
            chan = 1
            for chanval in value.split(', '):
                reckey = TRANSLATE['Array'][key] + str(chan)
                if key in SUFFIX:
                    reckey += SUFFIX[key]
                record[reckey] = chanval
                chan += 1
        else:
            print("%s\t%s" % (key, value))


def parse_czi():
    ''' Parse a CZI file
    '''
    xml = bioformats.get_omexml_metadata('LabRD95_01_Block1_20180409.czi')
    j = xmltodict.parse(xml)
    record = dict()
    ppr = pprint.PrettyPrinter(indent=4)
    ppr.pprint(j['OME'])
    print('------------------------------')
    parse_experimenter(j, record)
    parse_instrument(j, record)
    parse_image(j, record)
    parse_structuredannotations(j, record)
    ppr.pprint(record)


initialize()
parse_czi()
javabridge.kill_vm()
