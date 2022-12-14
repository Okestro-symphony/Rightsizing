# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from elasticsearch import Elasticsearch
from elasticsearch import ElasticsearchDeprecationWarning
import warnings
warnings.filterwarnings(action='ignore', category=ElasticsearchDeprecationWarning)
warnings.simplefilter(action='ignore', category=FutureWarning)
# from hdfs import InsecureClient

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import datetime
import time
import configparser
import os
import requests

import os
import sys
project_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(project_path)
from utils.result import Reporting
from utils.logs.log import standardLog
standardLog = standardLog()
reporting = Reporting(job='Resource RightSizing')

# make vm list
# loop for filter vm
# get target para(including adaptive threshold)
# decide type


class Right_Sizing:
    def __init__(self):
        project_path = os.path.dirname(os.path.dirname(__file__))
        self.config = configparser.ConfigParser()
        self.config.read(project_path + "/../config.ini")
        # self.get_db_info()
        self.api_url = 'https://{}/api/v1'.format(self.config['META']['DOMAIN'])
        self.api_key = {'X-Api-key' : self.config['META']['API_KEY']}
        self.vm_env = self.retrieve_meta_data()
        self.es = Elasticsearch(hosts=self.config['ES']['HOST'], port=int(self.config['ES']['PORT']), http_auth=(
            self.config['ES']['USER'], self.config['ES']['PASSWORD']
        ),timeout= 1000)
        # 임계값 등 초기설정값
        self.th_oversized_cpu_used = 0.2
        self.th_oversized_cpu_ready = 0.05
        self.th_oversized_mem_usage = 0.2
        self.th_oversized_mem_swapped = 0
        self.th_undersized_cpu_used = 0.7
        self.th_undersized_cpu_ready = 0.05
        self.th_undersized_mem_used = 0.8
        self.th_undersized_mem_swapped = 0
        self.th_resourceover_cpu_ready = 0.1
        self.th_resourceover_mem_usage = 0.9
        self.th_resourceover_cpu_recent = 0.8
        # disk -> bytes
        self.th_zombi_disk_usage = 30
        # net -> bytes
        self.th_zombi_net_usage = 1000
        self.th_zombi_vm_rest_day = 30
        # filesystem
        self.th_oversized_filesystem_used = 0.05
        self.th_undersized_filesystem_used = 0.8
        # setting total
        self.model = 'arima'
        self.duration = 30
        self.CI_level = 'standard'

        self.rightsizing_info = pd.DataFrame(columns = ['vm_id', 'vm_name', 'datetime', 'diagnosis_result',
                                                        'analysis_result', 'metric_type', 'resource_current',
                                                        'resource_recommend', 'is_excluded', 'effect_insight',
                                                        'analysis_period', 'prediction_model', 'timestamp'])