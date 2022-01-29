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
    cmd, 'cdy:m:p:n:l:h',
    ['current_date', 'direct_sync', 'year', 'month', 'period', 'days_back', 'district_list', 'help'])

# use current month as default
now = datetime.datetime.now()
date_now = datetime.date(now.year, now.month, now.day)
year = now.year
month = now.month
DIRECT_SENDING = False
USE_CURRENT_DATE = False
USE_DAYS_BACK = False
MONTH_DEFINED = False
specific_period = ""
days_back = 0
district_list = ""

for option, parameter in opts:
    if option in ['-d', '--direct_sync']:
        DIRECT_SENDING = True
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
        print("A script to generate aggregate datavalues of a dataset to district level for submitting to ")
        print("another DHIS2 instance via dispatcher2 or another data exhange middleware.")
        print("")
        print("Usage: python aggregate_integrator.py [-d ] [-c ] [-y <year>] [-m <month>] [-p <period>] [-n <days>] [-l <district_list>]")
        print("-d Direct synchronisation without use of data exchange middleware.")
        print("-c --current_date Whether to generate values only for the date when script is run.")
        print("-y --year The year for which to generate vales.")
        print("-m --month The month for which to generate/pull values before submission.")
        print("-n --days_back Generate values pre dating n days back.")
        print("-p --period The DHIS 2 period used for pulling data from source instance.")
        print("-l --district_list A string of comma-separated district names")
        print("-h --help This message.")
        sys.exit(2)

MONTH_DAYS = {1: 31, 3: 31, 4: 30, 5: 31, 6: 30, 7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31}

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

        districtSQL = (
            "SELECT dhis2_name, dhis2_id FROM orgunits WHERE dhis2_level='3' "
            "AND is_active = TRUE AND instance_pair_id = %s "
        )
        if district_list:
            passed_districts = [d.strip() for d in district_list.split(',')]
            district_array = str(passed_districts).replace(
                '[', '{').replace(']', '}').replace("\'", '\"').replace('u', '')
            districtSQL += " AND dhis2_name = ANY('{0}'::TEXT[]) ".format(district_array)

        districtSQL += " ORDER BY priority, dhis2_name"
        # print(">>>>>>>", districtSQL)
        #sys.exit(1)
        #
        cur.execute(districtSQL, [pair['id']])
        # cur.execute(
        #     "SELECT dhis2_name, dhis2_id FROM orgunits WHERE dhis2_level='3' "
        #     "AND instance_pair_id = %s ORDER BY priority, dhis2_name", [pair['id']])
        districts = cur.fetchall()

        if reporting_frequency == 'daily':
            if USE_CURRENT_DATE:
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

            if specific_period:
                try:
                    start_date = datetime.datetime.strptime(specific_period, '%Y%m%d').date()
                    end_date = start_date
                except:
                    start_date = datetime.date(now.year, now.month, now.day)
                    end_date = start_date
            if USE_DAYS_BACK:
                start_date = datetime.date(now.year, now.month, now.day)
                end_date = datetime.date(now.year, now.month, now.day)
                start_date -= datetime.timedelta(days=days_back)

            delta = datetime.timedelta(days=1)
            print("\tStart-Date: {0}, End-Date: {1}".format(start_date, end_date))
            if start_date > date_now:
                print("Start-Date: {0} is ahead of today {1}".format(start_date, date_now))
                sys.exit(1)

            # sys.exit(1)
            while start_date <= end_date:
                # print(start_date)
                period = start_date.strftime('%Y%m%d') # use this as period
                print("GENERATING FOR PERIOD: {0}".format(period))

                for district in districts:
                    print("Gonna handle records for: {0}".format(district))
                    cur.execute(
                        "SELECT dhis2_name, dhis2_id FROM orgunits WHERE instance_pair_id = %s "
                        " AND split_part(dhis2_path, '/', 4) = %s AND dhis2_level::int > '3' ", [pair['id'], district['dhis2_id']]) # XX remove limit
                    orgunits = cur.fetchall()
                    # print(orgunits)
                    # sys.exit(1)
                    dataValuesTotals = {}
                    attributeOptionCombo = ""
                    for orgunit in orgunits:

                        url = pair['source_url'] + "/dataValueSets?" + "dataSet={0}&orgUnit={1}&period={2}".format(
                                dataset['dataset_id'], orgunit['dhis2_id'], period)
                        response = read_from_dhis2(url, pair['source_username'], pair['source_password'])
                        try:
                            response_obj = response.json()
                            # print(response.json())
                        except:
                            continue
                        if 'dataValues' in response_obj:
                            print("Got data values for [Orgunit: {0} - {1}] - Period: {2}".format(
                                orgunit['dhis2_id'], orgunit['dhis2_name'], period))
                            for dval in response_obj['dataValues']:
                                dataElement = dval['dataElement']
                                value = dval['value']
                                categoryOptionCombo = dval['categoryOptionCombo']
                                if not attributeOptionCombo:
                                    attributeOptionCombo = dval['attributeOptionCombo']
                                data_key = "{0}_{1}".format(dataElement, categoryOptionCombo)
                                if value:
                                    if data_key in dataValuesTotals:
                                        dataValuesTotals[data_key]['value'] = "{0}".format(
                                                int(dataValuesTotals[data_key]['value']) + int(value))
                                    else:
                                        dataValuesTotals[data_key] = {
                                            'dataElement': dataElement,
                                            'value': value,
                                            'categoryOptionCombo': dval['categoryOptionCombo']
                                        }
                    if dataValuesTotals:
                        # print("Period: {0} =====> {1}".format(period, dataValuesTotals))
                        payload = {
                            'orgUnit': district['dhis2_id'],
                            'period': period,
                            'attributeOptionCombo': attributeOptionCombo,
                            'dataValues':[
                                {
                                    'dataElement': i['dataElement'],
                                    'value': i['value'],
                                    'categoryOptionCombo': i['categoryOptionCombo']
                                } for i in dataValuesTotals.values()]
                        }
                        extra_params = {
                            'year': year,
                            'month': month,
                            'source': pair['source'],
                            'destination': pair['destination'],
                            'facility': district['dhis2_name'],
                            'is_qparams': "f",
                            'report_type': '{0}_{1}'.format(pair['source'], pair['destination'])
                        }
                        print(">>>>>> Period: {0} =====> {1}".format(period, payload))
                        if DIRECT_SENDING:
                            pass
                        else:
                            queue_in_dispatcher2(json.dumps(payload), ctype="json", params=extra_params)
                start_date += delta

conn.close()
