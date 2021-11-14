#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = "Sekiwere Samuel"

import requests
import json
import base64
# from settings import config
import getopt
import sys
import datetime
import time
import psycopg2
import psycopg2.extras

dbconfig = {
    'dbname': 'dhis2_integrator',
    'host': 'localhost',
    'port': '5432',
    'user': '',
    'password': '',
}

config = {
    # dispatcher2 confs
    #'dispatcher2_queue_url':'http://localhost:9191/queue',
    'dispatcher2_queue_url':'http://iol.gcinnovate.com/queue',
    'dispatcher2_username': 'admin',
    'dispatcher2_password': 'admin',
    'dispatcher2_source': 'epivac',
    'dispatcher2_destination': 'eidsr',

    # DHIS 2
    'dhis2_username': '',
    'dhis2_password': '',
    'dhis2_url': '',
}

cmd = sys.argv[1:]
opts, args = getopt.getopt(
    cmd, 'dy:m:',
    [])

# use current month as default
now = datetime.datetime.now()
year = now.year
month = now.month
DIRECT_SENDING = False

for option, parameter in opts:
    if option == '-d':
        DIRECT_SENDING = True
    if option == '-y':
        year = parameter
        try:
            year = int(year)
        except:
            pass
    if option == '-m':
        month = parameter
        try:
            month = int(month)
        except:
            pass


def get_start_and_end_date(year, month):
    start_month = datetime.datetime(year, month, 1)
    date_in_next_month = start_month + datetime.timedelta(35)
    start_next_month = datetime.datetime(date_in_next_month.year, date_in_next_month.month, 1)
    return start_month.strftime('%Y-%m-%d'), start_next_month.strftime('%Y-%m-%d')


def post_data_to_dhis2(url, data, params={}, method="POST"):
    user_pass = '{0}:{1}'.format(config['dhis2_username'], config['dhis2_password'])
    coded = base64.b64encode(user_pass.encode())
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Basic ' + coded.decode()
    }

    response = requests.post(
	url, data=data, headers=headers,
	verify=False, params=params
    )
    return response


def queue_in_dispatcher2(data, url=config['dispatcher2_queue_url'], ctype="json", params={}):
    user_pass = '{0}:{1}'.format(config['dispatcher2_username'], config['dispatcher2_password'])
    coded = base64.b64encode(user_pass.encode())
    if 'xml' in ctype:
        ct = 'text/xml'
    elif 'json' in ctype:
        ct = 'application/json'
    else:
        ct = 'text/plain'
    response = requests.post(
        url, data=data, headers={
            'Content-Type': ct,
            'Authorization': 'Basic ' + coded.decode()},
        verify=False, params=params  # , cert=config['dispatcher2_certkey_file']
    )
    return response



conn = psycopg2.connect(
    "dbname=" + dbconfig["db_name"] + " host= " + dbconfig["db_host"] + " port=" + dbconfig["db_port"] +
    " user=" + dbconfig["db_user"] + " password=" + dbconfig["db_passwd"])
cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

conn.close()
