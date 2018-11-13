#!/opt/python/bin/python2.7

import argparse
import json
import sys
import colorlog
import requests
from kafka import KafkaConsumer
from datetime import datetime
from smtplib import SMTPException, SMTP
from email.mime.text import MIMEText


# Configuration
CONFIG = {'config': {'url': 'http://config.int.janelia.org/'}}
SENDER = 'svirskasr@janelia.hhmi.org'


# -----------------------------------------------------------------------------
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
    """ Initialize database """
    global CONFIG
    data = call_responder('config', 'config/rest_services')
    CONFIG = data['config']


def generate_email_message(sender, recievers, subject, message):
    msg = MIMEText(message)
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = ", ".join(recievers)
    return msg.as_string()


def send_mail(sender, recievers, subject, message):
    email = generate_email_message(sender, recievers, subject, message)
    try:
        smtpObj = SMTP()
        smtpObj.connect()
        smtpObj.sendmail(sender, recievers, email)
        logger.info("Successfully sent email")
    except SMTPException:
        logger.error("Error: unable to send email")


def read_messages():
    if ARG.server:
        server_list = [ARG.server + ':9092']
    else:
        rest = call_responder('config','config/servers/Kafka')
        server_list = rest['config']['broker_list']
    consumer = KafkaConsumer('screen_review',
                             bootstrap_servers=server_list,
                             auto_offset_reset='earliest',
                             consumer_timeout_ms=int(500))
    orderlist = dict()
    rest = call_responder('config', 'config/workday')
    usermap = rest['config']
    for message in consumer:
        msg = json.loads(message.value)
        if msg['category'] != 'order' or message.key == None:
            continue
        if ARG.USER and msg['user'] != ARG.USER:
            continue
        order_date = message.key.split(' ')
        if order_date[0] < ARG.START or order_date[0] > ARG.END:
            continue
        if ARG.DEBUG:
            print(msg)
        if msg['user'] not in orderlist:
            orderlist[msg['user']] = list()
        splittype = list()
        for typ in ('stabilization', 'polarity', 'mcfo'):
            if typ in msg and msg[typ]:
                splittype.append(typ)
        orderlist[msg['user']].append("%s\t%s\t%s" % (message.key, msg['line'], ', '.join(splittype)))
    for user in orderlist:
        if ARG.START == ARG.END:
            body = 'On ' + ARG.START
        else:
            body = "Between the dates of %s and %s" % (ARG.START, ARG.END)
        body += ", you ordered the following stable splits:\n\n"
        for order in orderlist[user]:
            body += order + "\n"
        if user in usermap:
            str = ((usermap[user][key]) for key in ('first', 'last'))
            username = ' '.join(str)
        else:
            username = user
        subject = 'Split screen orders for %s' % (username)
        primary = '%s@hhmi.org' % (user)
        if ARG.VERBOSE:
            print("%s\n%s" % (subject, body))
        send_mail(SENDER, [primary, SENDER], subject, body)


# -----------------------------------------------------------------------------

if __name__ == '__main__':
    today = datetime.today().strftime('%Y-%m-%d')
    PARSER = argparse.ArgumentParser(
        description='Find and index/discover newly tmogged imagery')
    PARSER.add_argument('--server', dest='server', default='', help='Kafka server')
    PARSER.add_argument('--verbose', action='store_true',
                        dest='VERBOSE', default=False,
                        help='Turn on verbose output')
    PARSER.add_argument('--debug', action='store_true',
                        dest='DEBUG', default=False,
                        help='Turn on debug output')
    PARSER.add_argument('--nolog', action='store_true',
                        dest='NOLOG', default=False,
                        help='Set logging for errors only')
    PARSER.add_argument('--user', dest='USER', default='',
                        help='User ID')
    PARSER.add_argument('--start', dest='START', default=today,
                        help='Start date (YYYY-MM-DD)')
    PARSER.add_argument('--end', dest='END', default=today,
                        help='End date (YYYY-MM-DD)')
    ARG = PARSER.parse_args()

    logger = colorlog.getLogger()
    if ARG.DEBUG:
        logger.setLevel(colorlog.colorlog.logging.DEBUG)
    elif ARG.VERBOSE:
        logger.setLevel(colorlog.colorlog.logging.INFO)
    elif ARG.NOLOG:
        logger.setLevel(colorlog.colorlog.logging.ERROR)
    else:
        logger.setLevel(colorlog.colorlog.logging.WARNING)
    HANDLER = colorlog.StreamHandler()
    HANDLER.setFormatter(colorlog.ColoredFormatter())
    logger.addHandler(HANDLER)

    initialize_program()
    read_messages()
    sys.exit(0)
