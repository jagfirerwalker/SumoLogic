import os
from datetime import datetime, timedelta
from sumologic import SumoLogic
from typing import Optional, Iterable, Sequence, TypeVar, Any
import time
import yaml
import json
from prometheus_client import start_http_server, Gauge, Summary
from prometheus_client.samples import Sample, Timestamp



def sumo_search(sumo_api, query, start_time, end_time):
    job = sumo_api.search_job(query, start_time, end_time)

    job_status = sumo_api.search_job_status(job)
    while job_status['state'] != 'DONE GATHERING RESULTS':
        if job_status['state'] == 'CANCELLED':
            break
        time.sleep(1)
        print(job_status)
        job_status = sumo_api.search_job_status(job)

    try:
        if job_status['state'] == 'DONE GATHERING RESULTS':
            return sumo_api.search_job_records(job, limit=1000)
        else:
            print(job_status)
            return None
    except KeyError as kerr:
        print(kerr)
        print(job_status)
        return None




def get_unique_titles(sumo_api):
    query = '''
    _sourceCategory=*liveeos* AND EpicHttpAPi AND (!sdkconfig)
    | parse "path\\":\\"http://*." as serviceName 
    | parse "EOS-SDK/* (*/*) */*\\"" as sdkVersion, osType, osVersion, productName, productVersion 
    | formatDate(_messagetime,"yyyy-MM-dd") as date
    | count_distinct(productName) as unique_EOS_titles by date 
    | order by date asc
    '''

    start_time = "2023-02-03T09:01:00"
    end_time = "2023-03-03T09:15:00"

    result = sumo_search(sumo_api, query, start_time, end_time)
    if result is not None:
        for rec in result:
            yield rec   
    return None



access_id = "#####"
access_key = "########"

sumo_api = SumoLogic(access_id, access_key)

RUN_INTERVAL_SECS = 60


start_http_server(8000)

next_run = datetime.now()
while True:
    # we use the value from 3 minutes ago to allow propagation time
    record_time = datetime.now() - timedelta(minutes=3)
    record_time -= timedelta(seconds=record_time.second)+timedelta(microseconds=record_time.microsecond)


    result = get_unique_titles(sumo_api)
    if result is not None:
        for rec in result:
            rec
