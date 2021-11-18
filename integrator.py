#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = "Sekiwere Samuel"

import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
import json
import base64
# from settings import config
import getopt
import sys
import datetime
import time
import psycopg2
import psycopg2.extras

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

dbconfig = {
    'db_name': 'dhis2-integrator',
    'db_host': 'localhost',
    'db_port': '5432',
    'db_user': 'postgres',
    'db_passwd': 'postgres',
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


def read_from_dhis2(url, username, password):
    user_pass = '{0}:{1}'.format(username, password)
    coded = base64.b64encode(user_pass.encode())
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Basic ' + coded.decode()
    }

    response = requests.get(url, headers=headers, verify=False)
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

cur.execute(
        "SELECT id, source, destination, source_url, source_username, source_password "
        "FROM dhis2_instance_pair WHERE is_active = TRUE")
instance_pairs = cur.fetchall()
for pair in instance_pairs:
    print(
        "Dealing with instance [ID: {0}, Souece: {1}, Destination: {2}]".format(
        pair['id'], pair['source'], pair['destination']))
    # Now work the datasets to sync for this instance pair
    cur.execute(
        "SELECT dataset_id, dataset_name, reporting_frequency, include_deleted "
        "FROM sync_datasets WHERE instance_pair_id = %s", [pair['id']])
    sync_datasets = cur.fetchall()
    for dataset in sync_datasets:
        print("Gonna Sync dataSet: [{0}: {1}]".format(dataset['dataset_id'], dataset['dataset_name']))
        reporting_frequency = dataset['reporting_frequency']

        cur.execute(
            "SELECT dhis2_name, dhis2_id FROM orgunits WHERE instance_pair_id = %s LIMIT 1", [pair['id']])
        orgunits = cur.fetchall()
        for orgunit in orgunits:
            print("Reading data values for [Orgunit: {0} - {1}]".format(
                orgunit['dhis2_id'], orgunit['dhis2_name']))

            # If reporting frequency is daily
            if reporting_frequency == 'daily':
                start_date = datetime.date(year, 1, 1)
                if year != now.year and year < now.year:
                    end_date = datetime.date(year, 12, 31)
                else:
                    end_date = datetime.date(year, now.month, now.day)
                delta = datetime.timedelta(days=1)
                print("\tStart-Date: {0}, End-Date: {1}".format(start_date, end_date))
                while start_date <= end_date:
                    # print(start_date)
                    period = start_date.strftime('%Y%m%d') # use this as period
                    url = pair['source_url'] + "dataSet={0}&orgUnit={1}&period={2}".format(
                            dataset['dataset_id'], orgunit['dhis2_id'], period)
                    # print(url)
                    response = read_from_dhis2(url, pair['source_username'], pair['source_password'])
                    print(response.json())
                    response_obj = response.json()
                    if 'dataValues' in response_obj:
                        # create payload to submit
                        payload = {
                            'orgUnit': response_obj['orgUnit'],
                            'period': response_obj['period'],
                            'attributeOptionCombo': response_obj['dataValues'][0]['attributeOptionCombo'],
                            'dataValues':[
                                {
                                    'dataElement': i['dataElement'],
                                    'value': i['value'],
                                    'categoryOptionCombo': i['categoryOptionCombo']
                                } for i in response_obj['dataValues']]
                        }
                        print(payload)
                    start_date += delta

conn.close()
