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



    def retrieve_data(self):
        """
        데이터 불러오기.
        임시로 로컬에 저장되어 있는 csv 파일 사용
        return : df_cpu, df_mem, df_disk, df_net (pandas dataframe)
        """
        # temp) read csv
        df_cpu = pd.read_csv('cpu_df.csv')
        df_mem = pd.read_csv('mem_df.csv')
        df_disk = pd.read_csv('disk_df.csv')
        df_net = pd.read_csv('net_df.csv')

        return df_cpu, df_mem, df_disk, df_net

    # def retrieve_data(self):
    #     """
    #     데이터 불러오기
    #     hdfs csv load
    #     return : df_cpu, df_mem, df_disk, df_net(pandas DF)
    #     """
    #     config = self.config
    #     hdfs_host = config.get('HDFS', 'HOST')
    #     hdfs_port = config.get('HDFS', 'PORT')
    #     hdfs_web_port = config.get('HDFS', 'WEB_PORT')
    #     hdfs_id = config.get('HDFS', 'USER')
    #     hdfs_pw = config.get('HDFS', 'PASSWORD')
    #
    #     hadoop_client = InsecureClient(url=f"http://{hdfs_host}:{hdfs_web_port}", user="hadoop")
    #     try:
    #         with hadoop_client.read(f"/preprocessed_data/cpu.csv") as reader:
    #             df_cpu = pd.read_csv(reader)
    #             reader.close()
    #
    #         with hadoop_client.read(f"/preprocessed_data/memory.csv") as reader:
    #             df_mem = pd.read_csv(reader)
    #             reader.close()
    #
    #
    #         with hadoop_client.read(f"/preprocessed_data/network.csv") as reader:
    #             df_net = pd.read_csv(reader)
    #             reader.close()
    #
    #
    #         with hadoop_client.read(f"/preprocessed_data/diskio.csv") as reader:
    #             df_disk = pd.read_csv(reader)
    #             reader.close()
    #
    #
    #
    #         df_cpu.loc[:, 'datetime'] = pd.to_datetime(df_cpu['datetime'], utc = True)
    #         df_mem.loc[:, 'datetime'] = pd.to_datetime(df_mem['datetime'], utc = True)
    #         df_disk.loc[:, 'datetime'] = pd.to_datetime(df_disk['datetime'], utc = True)
    #         df_net.loc[:, 'datetime'] = pd.to_datetime(df_net['datetime'], utc = True)
    #
    #     except Exception as e:
    #         print(f"something wrong during data road", e)
    #
    #     return df_cpu, df_mem, df_disk, df_net

    def retrieve_data(self):
        """
        데이터 불러오기
        hdfs csv load
        return : df_cpu, df_mem, df_disk, df_net(pandas DF)
        """
        config = self.config
        data_dir = config.get('Preprocessing', 'DATA_DIR')

        try:
            df_cpu = pd.read_csv(data_dir+"/cpu.csv")

            df_mem = pd.read_csv(data_dir+"/memory.csv")

            df_net = pd.read_csv(data_dir+"/network.csv")

            df_disk = pd.read_csv(data_dir+"/diskio.csv")

            df_filesystem = pd.read_csv(data_dir+"/filesystem.csv")

            df_cpu.loc[:, 'datetime'] = pd.to_datetime(df_cpu['datetime'], utc = True)
            df_mem.loc[:, 'datetime'] = pd.to_datetime(df_mem['datetime'], utc = True)
            df_disk.loc[:, 'datetime'] = pd.to_datetime(df_disk['datetime'], utc = True)
            df_net.loc[:, 'datetime'] = pd.to_datetime(df_net['datetime'], utc = True)
            df_filesystem.loc[:, 'datetime'] = pd.to_datetime(df_filesystem['datetime'], utc = True)

        except Exception as e:
            print(f"something wrong during data road", e)

        return df_cpu, df_mem, df_disk, df_net, df_filesystem

    # def retrieve_pod_data(self):
    #     """
    #     pod 데이터 불러오기
    #     hdfs csv load
    #     return : df_pod(pandas DF)
    #     """
    #     config = self.config
    #     hdfs_host = config.get('HDFS', 'HOST')
    #     hdfs_port = config.get('HDFS', 'PORT')
    #     hdfs_web_port = config.get('HDFS', 'WEB_PORT')
    #     hdfs_id = config.get('HDFS', 'USER')
    #     hdfs_pw = config.get('HDFS', 'PASSWORD')
    #
    #     hadoop_client = InsecureClient(url=f"http://{hdfs_host}:{hdfs_web_port}", user="hadoop")
    #     try:
    #         with hadoop_client.read(f"/preprocessed_data/pod.csv") as reader:
    #             df_pod = pd.read_csv(reader)
    #             reader.close()
    #
    #
    #         df_pod.loc[:, 'datetime'] = pd.to_datetime(df_pod['datetime'], utc = True)
    #
    #     except Exception as e:
    #         print(f"something wrong during data road", e)
    #
    #     return df_pod

    def retrieve_pod_data(self):
        """
        pod 데이터 불러오기
        hdfs csv load
        return : df_pod(pandas DF)
        """
        config = self.config
        data_dir = config.get('Preprocessing', 'DATA_DIR')

        try:
            df_pod = pd.read_csv(data_dir+"/pod.csv")
            df_pod.loc[:, 'datetime'] = pd.to_datetime(df_pod['datetime'], utc = True)

        except Exception as e:
            standardLog.sending_log('error', e).error('something wrong during pod data load')

        return df_pod

    def retrieve_vm_list(self, df_cpu):
        """
        df_cpu 기준 vm id에 unique 함수 적용
        return : vm_list (list)
        """
        vm_list = list(df_cpu['host_name'].unique())
        vm_list_new = vm_list.copy()
        for vm in vm_list:
            if (len(df_cpu[df_cpu['host_name']==vm]) < self.duration * 24 * 12 * 0.8 * 0.1) and False:
                vm_list_new.remove(vm)

        return vm_list_new

    def retrieve_prediction_data(self, vm_list, provider):
        """
        es 에서 각 vm별 데이터 수집
        return : pred( dict { metric : df})
        """
        es = self.es
        now = round(time.time() * 1000)

        if provider == 'openshift':
            prvd_type = 'pod'
            pred = {'cpu': None,
                    'memory' : None}

        else:
            prvd_type = 'vm'
            pred = {'cpu': None,
                    'memory': None,
                    'diskio-read': None,
                    'diskio-write': None,
                    'network-in': None,
                    'network-out': None,
                    'filesystem': None}
        for metric in pred.keys():
            for vm in vm_list:
                index = 'sym-metric-{prvd_type}-prediction-{metric}'.format(prvd_type = prvd_type, metric = metric)
                body = {
                    'size' : 10000,
                    'track_total_hits' : True,
                    'query' : {
                        'bool' : {
                            'must' : [
                                {"range": {
                                    'timestamp':{
                                        'gte': now

                                    }
                                }},
                                {'terms' : {
                                    'host_id': [vm]
                                }}
                            ]
                        }
                    }
                }
                res = es.search(index = index, body = body)
                if res['hits']['total']['value'] != 0:
                    res = pd.DataFrame([t['_source'] for t in res['hits']['hits']])
                    res['timestamp'] = pd.to_datetime(res['timestamp'], unit = 'ms')
                    res.rename(columns = {'host_id' : 'host_name'}, inplace = True)
                    res = res.drop_duplicates(['timestamp'], keep = 'first')
                    if pred[metric] is None:
                        pred[metric] = res
                    else:
                        pred[metric] = pred[metric].append(res)


        return pred