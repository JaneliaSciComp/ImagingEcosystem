import argparse
import sys
from kafka import KafkaConsumer
from kafka import KafkaProducer
from kafka.errors import KafkaError
from datetime import datetime
from time import gmtime, strftime, sleep
import json
from os import path
from pprint import pprint

def read_messages():
    producer = KafkaProducer(bootstrap_servers=['kafka.int.janelia.org','kafka2.int.janelia.org','kafka3.int.janelia.org'])
    if ARGS.server:
        server_list = [ARGS.server + ':9092']
    else:
        server_list = ['kafka.int.janelia.org:9092', 'kafka2.int.janelia.org:9092', 'kafka3.int.janelia.org:9092']
    offset = 'earliest'
    consumer = KafkaConsumer(ARGS.topic,
                             bootstrap_servers=server_list,
                             group_id=None,
                             auto_offset_reset='earliest',
                             consumer_timeout_ms=int(5000))

    for message in consumer:
        msg = json.loads(message.value)
        timestamp = datetime.fromtimestamp(message.timestamp/1000).strftime('%Y-%m-%dT%H:%M:%S')
        if '1969' in timestamp:
            timestamp = message.key
            timestamp = timestamp.replace(' ', 'T')
        head, sep, tail = timestamp.partition('.')
        if msg['operation'] == 'search':
            kafka = {"client": "screen_review", "user": str(msg['user']), "category": "search",
                     "time": message.timestamp/1000, "host": "vm506.int.janelia.org", "count": 1,
                     "duration": msg['elapsed_time'], "status": 200}
            if 'line' in msg:
                kafka['line'] = str(msg['line'])
            elif 'slide_code' in msg:
                kafka['slide_code'] = str(msg['slide_code'])
            else:
                kafka['date_range'] = str(msg['date_range'])
        else:
            for split in msg['order']['splits']:
                line = ''
                for key, val in split.items():
                    if key == 'line':
                        line = val
                        break
                for key, val in split.items():
                    if key in ['mcfo', 'polarity', 'stabilization']:
                        kafka = {"client": "screen_review", "user": str(msg['user']), "category": "order",
                                 "time": message.timestamp/1000, "host": "vm506.int.janelia.org", "count": 1,
                                 "status": 200, "order": str(msg['order']), str(key): 1, "line": line}

        print(json.dumps(kafka))
        future = producer.send('screen_review', json.dumps(kafka))
        try:
            record_metadata = future.get(timeout=10)
        except KafkaError:
            # Decide what to do if produce request failed...
            print "Failed!"
            pass

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description='Kafka split_screen log generator')
    PARSER.add_argument('--server', dest='server', default='', help='Server')
    PARSER.add_argument('--topic', dest='topic', default='split_screen', help='Topic')
    PARSER.add_argument('--restart', dest='restart', action='store_true',
                        default=False, help='Pull from very beginning (no group)')
    ARGS = PARSER.parse_args()
    read_messages()
    sys.exit(0)
