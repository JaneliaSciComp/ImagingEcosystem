import argparse
import sys
from kafka import KafkaConsumer
from datetime import datetime
import json
from os import path
from pprint import pprint

def read_messages():
  if ARGS.server:
      server_list = [ARGS.server + ':9092']
  else:
      server_list = ['kafka.int.janelia.org:9092', 'kafka2.int.janelia.org:9092', 'kafka3.int.janelia.org:9092']
  group = None
  client = None
  if ARGS.restart:
      group = None
      client = None
  else:
      group = ARGS.topic + '_log'
      client = group
  consumer = KafkaConsumer(ARGS.topic,
                           bootstrap_servers = server_list,
                           group_id = group,
                           auto_offset_reset = 'earliest',
                           consumer_timeout_ms = int(5000))

  for message in consumer:
      msg = json.loads(message.value)
      timestamp = datetime.fromtimestamp(message.timestamp/1000).strftime('%Y-%m-%dT%H:%M:%S')
      if '1969' in timestamp:
          timestamp = message.key
          timestamp = timestamp.replace(' ', 'T')
      head, sep, tail = timestamp.partition('.')
      if msg['operation'] == 'search':
          print("%s\t%s\t%s\t%s\t%s\t%d" % (head, str(msg['user']), str(msg['operation']), 'search', 'search', 1))
      else:
          for split in msg['order']['splits']:
              line = ''
              for key, val in split.items():
                  if key == 'line':
                      line = val
                      break
              for key, val in split.items():
                  if key in ['mcfo', 'polarity', 'stabilization']:
                      print("%s\t%s\t%s\t%s\t%s\t%d" % (head, str(msg['user']), str(msg['operation']), line, key, val))


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
