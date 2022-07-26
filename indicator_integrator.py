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
from itertools import groupby
from operator import itemgetter

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
    'dispatcher2_queue_url':'http://localhost:9191/queue',
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
    cmd, 'cdy:m:p:i:n:l:h',
    ['current_date', 'direct_sync', 'year', 'month', 'period', 'indicator',
        'days_back', 'district_list', 'help'])

# https://emisuganda.org/emis/api/analytics?dimension=dx:lDLUOXEbjE2,pe:20220211,ou:LEVEL-3
# use current month as default
now = datetime.datetime.now()
date_now = datetime.date(now.year, now.month, now.day)
year = now.year
month = now.month
USE_CURRENT_DATE = False
USE_DAYS_BACK = False
MONTH_DEFINED = False
DEFAULT_ATTRIBUTE_OPTION_COMBO = "HllvX50cXC0"
DEFAULT_CATEGORY_COMBO = "HllvX50cXC0"
specific_period = ""
days_back = 0
district_list = ""
indicator = ""

for option, parameter in opts:
    if option in ['-c', '--current_date']:
        USE_CURRENT_DATE = True

    if option in ['-y', '--year']:
        year = parameter
        try:
            year = int(year)
        except:
            pass
    if option in ['-m', '--month']:
        month = parameter
        MONTH_DEFINED = True
        try:
            month = int(month)
        except:
            pass
    if option in ['-p', '--period']:
        specific_period = parameter
    if option in ['-n', '--days_back']:
        days_back = parameter
        try:
            days_back = int(days_back)
            if days_back:
                USE_DAYS_BACK = True
        except:
            pass
    if option in ['-l', '--district_list']:
        district_list = parameter

    if option in ['-h', '--help']:
        print("A script to generate indicator values for submitting to a dataset in ")
        print("another DHIS2 instance via dispatcher2 or another data exhange middleware.")
        print("")
        print("Usage: python inidicator_integrator.py [-d ] [-c ] [-y <year>] [-m <month>] [-p <period>] [-n <days>] [-l <district_list>]")
        print("-c --current_date Whether to generate values only for the date when script is run.")
        print("-y --year The year for which to generate vales.")
        print("-m --month The month for which to generate/pull values before submission.")
        print("-n --days_back Generate values pre dating n days back.")
        print("-p --period The DHIS 2 period used for pulling data from source instance.")
        print("-l --district_list A string of comma-separated district names")
        print("-h --help This message.")
        sys.exit(2)

MONTH_DAYS = {1: 31, 3: 31, 4: 30, 5: 31, 6: 30, 7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31}

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
    cur.execute(
        "SELECT name, source_indicator_id, dataset, dataelement, category_option_combo "
        "FROM indicator_mapping WHERE instance_pair_id = %s", [pair['id']])
    our_mapping = cur.fetchall()
    indicators = ';'.join([i[1] for i in our_mapping])
    print("Indicators: ", indicators)
    mappings = {}
    for m in our_mapping:
        mappings[m['source_indicator_id']] = {
            'dataset': m['dataset'],
            'dataelement': m['dataelement'],
            'category_option_combo': m['category_option_combo']
        }
    print("MAPPING: ", mappings)
    # sys.exit(1)
    # Now work the datasets to sync for this instance pair
    cur.execute(
        "SELECT dataset_id, dataset_name, reporting_frequency, include_deleted "
        "FROM sync_datasets WHERE instance_pair_id = %s", [pair['id']])
    sync_datasets = cur.fetchall()
    for dataset in sync_datasets:
        print("Gonna Sync dataSet: [{0}: {1}]".format(dataset['dataset_id'], dataset['dataset_name']))
        reporting_frequency = dataset['reporting_frequency']

        if reporting_frequency == 'daily':
            if USE_CURRENT_DATE:
                print("Using Curren Date")
                start_date = datetime.date(now.year, now.month, now.day)
            else:
                if MONTH_DEFINED:
                    start_date = datetime.date(year, month, 1)
                else:
                    start_date = datetime.date(year, 1, 1)

            if year != now.year and year < now.year:
                end_date = datetime.date(year, 12, 31)
            else:
                if MONTH_DEFINED:
                    if month == 2:
                        try:
                            end_date = datetime.date(year, month, 28)
                        except:
                            end_date = datetime.date(year, month, 29)
                    else:
                        end_date = datetime.date(year, month, MONTH_DAYS[month])
                else:
                    end_date = datetime.date(year, now.month, now.day)

            if USE_DAYS_BACK:
                start_date = datetime.date(now.year, now.month, now.day)
                end_date = datetime.date(now.year, now.month, now.day)
                start_date -= datetime.timedelta(days=days_back)

            if specific_period:
                try:
                    start_date = datetime.datetime.strptime(specific_period, '%Y%m%d').date()
                    end_date = start_date
                except:
                    start_date = datetime.date(now.year, now.month, now.day)
                    end_date = start_date

            delta = datetime.timedelta(days=1)
            print("\tStart-Date: {0}, End-Date: {1}".format(start_date, end_date))

            if start_date > date_now:
                print("Start-Date: {0} is ahead of today {1}".format(start_date, date_now))

            while start_date <= end_date:
                # print(start_date)
                period = start_date.strftime('%Y%m%d') # use this as period
                print("GENERATING FOR PERIOD: {0}".format(period))
                url = pair['source_url'] + "/analytics?dimension=" + "dx:{0}&dimension=pe:{1}&dimension=ou:LEVEL-{2}&paging=false".format(
                        indicators, period, 3)  #  LEVEL defaults to 3 for districts
                print("URL: [{0}] ".format(url))
                # sys.exit(1)
                try:
                    response = read_from_dhis2(url, pair['source_username'], pair['source_password'])
                    response_obj = response.json()
                except:
                    pass
                    print("We have an Exception")
                    # sys.exit(1)
                rows = response_obj["rows"] # <><><> Remove this
                # sort the results based ob ou which is in position 2 for each row
                rows = sorted(rows, key=lambda x: x[2])

                # group records for each ou together
                grouped_rows = [list(group) for key, group in groupby(rows, itemgetter(2))]
                print("We got our data", grouped_rows)
                metadata = response_obj["metaData"]

                for group in grouped_rows:
                    orgUnit = ""
                    orgUnitName = ""
                    dataValues = []
                    for row in group:
                        if not orgUnit:
                            orgUnit = row[2]
                        if not orgUnitName:
                            orgUnitName = metadata["items"][orgUnit]["name"]

                        value = int(float(row[3]))
                        dataValues.append({
                            'dataElement': mappings[row[0]]['dataelement'],
                            'value': value,
                            'categoryOptionCombo': mappings[row[0]]['category_option_combo']
                        })


                    payload = {
                        'orgUnit': orgUnit,
                        'period': period,
                        'attributeOptionCombo': DEFAULT_ATTRIBUTE_OPTION_COMBO,
                        'dataValues': dataValues
                    }
                    extra_params = {
                        'year': year,
                        'month': month,
                        'source': pair['source'],
                        'destination': pair['destination'],
                        'facility': orgUnitName,
                        'is_qparams': "f",
                        'report_type': '{0}_{1}'.format(pair['source'], pair['destination'])
                    }
                    print(">>>Period: {0}, === OrgUnit: {1} ===, Payload: {2}".format(period, orgUnitName, payload))

                    try:
                        queue_in_dispatcher2(json.dumps(payload), ctype="json", params=extra_params)
                    except:
                        pass
                        time.sleep(4)
                        print("Failed to queue for: ", period)

                start_date += delta

conn.close()
