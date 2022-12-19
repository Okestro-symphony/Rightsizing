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

    def retrieve_meta_data(self):
        """
        api 활용 vm metadata 불러오기
        return : vm_metadata(dataframe)
        """
        meta_providers = requests.get(self.api_url + '/meta/providers', headers = self.api_key,
                                      verify = False).json()['resResult']
        self.provider_list = list(set([t['type'] for t in meta_providers]))
        df = pd.DataFrame()
        for provider in meta_providers:
            if provider['type'] != 'openstack':
                prvd_id = provider['id']
                res = requests.get(self.api_url + '/meta/pods', params={'prvdId': prvd_id}, headers=self.api_key,
                                   verify=False)
                try:
                    df = df.append(pd.DataFrame(res.json()['resResult']))
                except Exception as e:
                    standardLog.sending_log('error', e).error(f'error occurs during loading meta data from {prvd_id}')

            else:
                prvd_id = provider['id']
                res = requests.get(self.api_url + '/meta/vms', params = {'prvdId' : prvd_id}, headers = self.api_key,
                                   verify = False)
                try:
                    df = df.append(pd.DataFrame(res.json()['resResult']))
                except Exception as e:
                    standardLog.sending_log('error', e).error(f'error occurs during loading meta data from {prvd_id}')
                    
        return df.reset_index(drop = True)


    def calculate_type(self, df_cpu, df_mem, df_disk, df_net, df_filesystem, df_pred, target_vms, pred_model, provider):
        """
        dataframe과 target vm list를 활용하여 vm rightsizing 판단 정보 제공
        return : vm_info (pandas dataframe)
        """
        # 임계값 넘었는지 확인
        # vm별 분류

        # cpu_readiness % = cpu_ready / 200 / vCPU_num
        vm_info = pd.DataFrame(columns = self.rightsizing_info.columns)
        th_undersized_cpu_used = self.th_undersized_cpu_used
        th_undersized_mem_used = self.th_undersized_mem_used



        def is_cpu_oversized(current, pred):
            if pred['cpu_usage'] < self.th_oversized_cpu_used or current['cpu_ready'] < self.th_oversized_cpu_ready:
                return True
            else: return False

        def is_mem_oversized(current, pred):
            if pred['mem_usage'] < self.th_oversized_mem_usage or current['mem_swap'] < self.th_oversized_mem_swapped:
                return True
            else: return False

        def is_cpu_undersized(current, pred):
            if pred['cpu_usage'] > th_undersized_cpu_used or current['cpu_ready'] > self.th_undersized_cpu_ready:
                return True
            else: return False

        def is_mem_undersized(current, pred):
            if pred['mem_usage'] > th_undersized_mem_used or current['mem_swap'] > self.th_undersized_mem_swapped:
                return True
            else: return False

        def is_resourceover(current, pred):
            if current['cpu_ready'] > self.th_resourceover_cpu_ready or\
                    pred['mem_usage'] > self.th_resourceover_mem_usage or\
                    current['cpu_recent'] > self.th_resourceover_cpu_recent:
                return True
            else: return False

        def is_zombi(current, pred):
            if pred['net_usage'] < self.th_zombi_net_usage or pred['disk_usage'] < self.th_zombi_disk_usage:
                return True
            else: return False

        def is_filesystem_undersized(current, pred):
            if pred['filesystem_usage'] > self.th_undersized_filesystem_used and\
                    current['filesystem_usage'] < pred['filesystem_usage']:
                return True
            else: return False


        # 환경정보를 불러와서 현재 상태 대비 변경해야할 환경 정보 제공
        def diagnosis_detail(vm_info, vm_name, current, pred, pred_model, provider):
            """
            vm 환경정보(cpu core 수, memory 할당량) 등을 활용하여 vm 상태 진단
            진단별 분석, 기대효과, 추천안 등을 pandas Series로 각각 datafrmae에 추가
            return : None
            """
            # metadata api 환경 정보 연동
            vm_env = self.vm_env
            if provider == 'openstack':
                if vm_env[vm_env['name'] == vm_name].empty:
                    vcpu = 4
                    mem = 8192
                    disk = 50
                    vm_id = 'meta_error'
                    # return vm_info
                else:
                    vcpu = int(vm_env[vm_env['name'] == vm_name].iloc[-1]['vcpus'])
                    mem = int(vm_env[vm_env['name'] == vm_name].iloc[-1]['localMemory'])
                    vm_id = vm_env[vm_env['name'] == vm_name].iloc[-1]['id']
                    # disk의 경우 flavor와 실제 filesystem 값 사이의 괴리 해소 필요
                    disk = vm_env[vm_env['name'] == vm_name].iloc[-1]['localDisk']

            elif provider == 'openshift':
                if vm_env[vm_env['podName'] == vm_name].empty:
                    vcpu = 4
                    mem = 8192
                    vm_id = 'meta_error'
                else:
                    vcpu = int(vm_env[vm_env['podName'] == vm_name].iloc[-1]['cpu'])
                    mem = int(vm_env[vm_env['podName'] == vm_name].iloc[-1]['ram'])
                    vm_id = 'None'



            if is_resourceover(current, pred):
                diag = 'resource 과소비가 예상되므로 해당 vm을 정지하고 이상 유무 점검 필요'

                analysis = '{}일 뒤 cpu_ready {:0.3f}%, memory_usage {:0.3f}%, 최근 15분간 cpu 사용량 {:0.3f}%로 resource 과소비 상태로 판단'.format(
                    self.duration, current['cpu_ready']*100, pred['mem_usage']*100, current['cpu_recent']*100 / vcpu
                )

                effect = 'vm 중지를 통해 host 머신에 미칠 수 있는 과부하 최소화'
                vm_info = vm_info.append(pd.Series(
                    [vm_id, vm_name, datetime_now, 'over_use', analysis, 'null', 0, 0, False, effect,
                     self.duration, pred_model, timestamp_now], index=vm_info.columns), ignore_index=True)
            elif is_zombi(current, pred):
                diag = '사용하지 않는 vm인 경우 제거 권장'
                analysis = '{}일 뒤 net_usage {:0.3f}kB/s, disk_usage {:0.3f}kB/s로 사용량이 거의 없는 inactive 상태로 판단'.format(
                    self.duration, pred['net_usage'], pred['disk_usage']
                )
                effect = 'vm 삭제를 통해 host 내 vm 관리 최적화'
                vm_info = vm_info.append(pd.Series(
                    [vm_id, vm_name, datetime_now, 'inactive', analysis, 'null', 0, 0, False, effect,
                     self.duration, pred_model, timestamp_now], index=vm_info.columns), ignore_index=True)
            else:
                if is_cpu_undersized(current, pred):
                    diag = 'vCPU 수를 {}개에서 {}개로 증가 권장'.format(vcpu, vcpu*2)
                    analysis = '{}일 뒤 cpu_usage {:0.3f}%, cpu_ready {:0.3f}%로 undersized 상태로 판단'.format(
                        self.duration, pred['cpu_usage']*100, current['cpu_ready']*100
                    )

                    effect = '{}일 뒤 cpu_usage {:0.3f}% 수준까지 감소 예상'.format(self.duration, pred['cpu_usage']*100/2)
                    vm_info = vm_info.append(pd.Series(
                        [vm_id, vm_name, datetime_now, 'undersized', analysis, 'cpu', vcpu, vcpu*2, False, effect,
                         self.duration, pred_model, timestamp_now], index=vm_info.columns), ignore_index=True)
                elif is_cpu_oversized(current, pred):
                    if vcpu > 1:
                        diag = 'vCPU 수를 {}개에서 {}개로 김소 권장'.format(vcpu, vcpu//2)
                        analysis = '{}일 뒤 cpu_usage {:0.3f}%, cpu_ready {:0.3f}%로 oversized 상태로 판단'.format(
                            self.duration, pred['cpu_usage']*100, current['cpu_ready']*100)

                        effect = '{}일 뒤 cpu_usage {:0.3f}% 수준까지 증가 예상'.format(self.duration,
                                                                              pred['cpu_usage']*100*(vcpu/(vcpu//2)))
                        vm_info = vm_info.append(pd.Series(
                            [vm_id, vm_name, datetime_now, 'oversized', analysis, 'cpu', vcpu, vcpu // 2, False, effect,
                             self.duration, pred_model, timestamp_now], index=vm_info.columns), ignore_index=True)
                if is_mem_undersized(current, pred):
                    diag = 'Memory 용량을 {}MB에서 {}MB로 증가 권장'.format(mem, mem*2)
                    analysis = '{}일 뒤 mem_usage {:0.3f}%, mem_swapped {}kB로 undersized 상태로 판단'.format(
                        self.duration, pred['mem_usage']*100, current['mem_swap'])
                    effect = '{}일 뒤 mem_usage {:0.3f}% 수준까지 감소 예상'.format(self.duration, pred['mem_usage']*100/2)
                    vm_info = vm_info.append(pd.Series(
                        [vm_id, vm_name, datetime_now, 'undersized', analysis, 'memory', mem, mem*2, False, effect,
                         self.duration, pred_model, timestamp_now], index=vm_info.columns), ignore_index=True)
                elif is_mem_oversized(current, pred):
                    if mem > 1:
                        diag = 'Memory 용량을 {}에서 {}로 감소 권장'.format(mem, mem*2)
                        analysis = '{}일 뒤 mem_usage {:0.3f}%, mem_swapped {}kB로 oversized 상태로 판단'.format(
                            self.duration, pred['mem_usage']*100, current['mem_swap']
                        )
                        effect = '{}일 뒤 mem_usage {:0.3f}% 수준까지 증가 예상'.format(
                            self.duration, pred['mem_usage']*100*2)
                        vm_info = vm_info.append(pd.Series(
                            [vm_id, vm_name, datetime_now, 'oversized', analysis, 'memory', mem, mem//2, False, effect,
                             self.duration, pred_model, timestamp_now], index=vm_info.columns), ignore_index=True)
                if is_filesystem_undersized(current, pred):
                    diag = 'Disk 용량을 {}GB에서 {}GB로 증가 권장'.format(disk, disk*2)
                    disk_remain_days_90pct = (0.9 - current['filesystem_usage']) /\
                                             (pred['filesystem_usage'] - current['filesystem_usage']) * self.duration
                    disk_remain_days_90pct = max(0, disk_remain_days_90pct)
                    analysis = '{}일 뒤 filesystem_usage {:0.3f}%로 undersized 상태로 판단. {:0.1f}일 뒤 90% 수준 도달 예상'\
                        .format(self.duration, pred['filesystem_usage']*100, disk_remain_days_90pct)
                    effect = '{}일 뒤 filesystem_usage {:0.3f}% 수준까지 감소 예상'.format(self.duration, pred['filesystem_usage']*100/2)
                    vm_info = vm_info.append(pd.Series(
                        [vm_id, vm_name, datetime_now, 'undersized', analysis, 'disk', disk, disk*2, False, effect,
                         self.duration, pred_model, timestamp_now], index=vm_info.columns), ignore_index=True)

            return vm_info

        def filter_df_list(df_list, vm_name):
            """
            df list 내 vm id 필터 일괄 적용
            return : df_list
            """
            return [df[df['host_name'] == vm_name].copy() if df is not None else pd.DataFrame() for df in df_list]

        def get_para(df_cpu, df_mem, df_disk, df_net, df_filesystem, df_pred_cpu, df_pred_mem, df_pred_net_in,
                     df_pred_net_out, df_pred_disk_read, df_pred_disk_write, df_pred_filesystem):
            """
            메트릭별 dataframe 내에서 필요 파라미터 추출(사용량, swap memory 등)
            undersized의 경우 최근 값들의 평균으로 동적 임계치 적용
            return : current, pred ( python dictionary)
            """


            now = pd.Timestamp.utcnow()

            current = {}
            pred = {}

            if df_cpu.empty or df_pred_cpu.empty:
                current['cpu_usage'] = 0
                current['cpu_ready'] = 0
                current['cpu_recent'] = 0
                pred['cpu_usage'] = 0

            else:
                current['cpu_usage'] = df_cpu[df_cpu['datetime'] < now].iloc[-1]['mean_cpu_usage']
                current['cpu_ready'] = df_cpu[df_cpu['datetime'] < now].iloc[-1].get('mean_iowait',0)
                current['cpu_recent'] = np.mean(df_cpu[df_cpu['datetime'] < now].iloc[-12:]['mean_cpu_usage'])
                pred['cpu_usage'] = df_pred_cpu.iloc[-1]['predict_value']

            if df_mem.empty or df_pred_mem.empty:
                current['mem_swap'] = 0
                pred['mem_usage'] = 0
            else:
                current['mem_swap'] = df_mem[df_mem['datetime'] < now].iloc[-1].get('mean_swap', 0)
                pred['mem_usage'] = df_pred_mem.iloc[-1]['predict_value']

            if df_disk.empty or df_pred_disk_read.empty or df_pred_disk_write.empty:
                pred['disk_usage'] = 3000
            else:
                pred['disk_usage'] = df_pred_disk_read.iloc[-1]['predict_value'] + \
                                     df_pred_disk_write.iloc[-1]['predict_value']

            if df_net.empty or df_pred_net_in.empty or df_pred_net_out.empty:
                pred['net_usage'] = 3000
            else:
                pred['net_usage'] = df_pred_net_in.iloc[-1]['predict_value'] + \
                                    df_pred_net_out.iloc[-1]['predict_value']

            if df_filesystem.empty or df_pred_filesystem.empty:
                current['filesystem_usage'] = 0.44
                pred['filesystem_usage'] = 0.44
            else:
                current['filesystem_usage'] = np.mean(
                    df_filesystem[df_filesystem['datetime'] < now].iloc[-12:]['mean_filesystem_usage'])
                pred['filesystem_usage'] = np.mean(
                    df_pred_filesystem[pd.to_datetime(df_pred_filesystem['datetime'], utc=True) < now].
                        iloc[-12:]['predict_value'])


            # adaptive threshold
            th_undersized_cpu_used = max(self.th_undersized_cpu_used,
                                         np.mean(df_cpu.iloc[-(5 * 12 * 72):, ]['mean_cpu_usage']) * 0.95)
            th_undersized_mem_used = max(self.th_undersized_mem_used,
                                         np.mean(df_mem.iloc[-(5 * 12 * 72):, ]['mean_memory_usage']) * 0.95)

            thresholds = {'th_undersized_cpu_used' : th_undersized_cpu_used,
                          'th_undersized_mem_used' : th_undersized_mem_used}

            return current, pred, thresholds

        datetime_now = datetime.datetime.now(datetime.timezone.utc).isoformat()[:-9]+"Z"
        timestamp_now = int(datetime.datetime.timestamp(datetime.datetime.utcnow())*1000)

        vm_cpu_usage = pd.DataFrame(columns = ['vm_id', 'cpu_usage_from', 'cpu_usage_to'])

        for vm_name in target_vms:
            current, pred, thresholds = get_para(*filter_df_list([df_cpu, df_mem, df_disk, df_net, df_filesystem] + list(df_pred.values()),
                                                                 vm_name))
            vm_cpu_usage = vm_cpu_usage.append(pd.Series([vm_name, current['cpu_usage'], pred['cpu_usage']], index = vm_cpu_usage.columns), ignore_index = True)

            th_undersized_cpu_used = thresholds['th_undersized_cpu_used']
            th_undersized_mem_used = thresholds['th_undersized_mem_used']

            vm_info = diagnosis_detail(vm_info, vm_name, current, pred, pred_model, provider)

        vm_info['datetime'] = pd.to_datetime(vm_info['datetime']).astype(int)/1000000
        return vm_info, vm_cpu_usage