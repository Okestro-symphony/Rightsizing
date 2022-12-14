import pandas as pd
import numpy as np
from elasticsearch import Elasticsearch
from elasticsearch import ElasticsearchDeprecationWarning
import warnings
warnings.filterwarnings(action='ignore', category=ElasticsearchDeprecationWarning)
warnings.simplefilter(action='ignore', category=FutureWarning)

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


class Right_Sizing:
    def __init__(self):
        project_path = os.path.dirname(os.path.dirname(__file__))
        self.config = configparser.ConfigParser()
        self.config.read(project_path + "/../config.ini")
        self.api_url = 'https://{}/api/v1'.format(self.config['META']['DOMAIN'])
        self.api_key = {'X-Api-key' : self.config['META']['API_KEY']}
        self.vm_env = self.retrieve_meta_data()
        self.es = Elasticsearch(hosts=self.config['ES']['HOST'], port=int(self.config['ES']['PORT']), http_auth=(
            self.config['ES']['USER'], self.config['ES']['PASSWORD']
        ),timeout= 1000)
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
        self.th_zombi_disk_usage = 30
        self.th_zombi_net_usage = 1000
        self.th_zombi_vm_rest_day = 30
        self.th_oversized_filesystem_used = 0.05
        self.th_undersized_filesystem_used = 0.8
        self.model = 'arima'
        self.duration = 30
        self.CI_level = 'standard'

        self.rightsizing_info = pd.DataFrame(columns = ['vm_id', 'vm_name', 'datetime', 'diagnosis_result',
                                                        'analysis_result', 'metric_type', 'resource_current',
                                                        'resource_recommend', 'is_excluded', 'effect_insight',
                                                        'analysis_period', 'prediction_model', 'timestamp'])



    def retrieve_data(self):
        df_cpu = pd.read_csv('cpu_df.csv')
        df_mem = pd.read_csv('mem_df.csv')
        df_disk = pd.read_csv('disk_df.csv')
        df_net = pd.read_csv('net_df.csv')

        return df_cpu, df_mem, df_disk, df_net

    def retrieve_data(self):
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

    def retrieve_pod_data(self):
        config = self.config
        data_dir = config.get('Preprocessing', 'DATA_DIR')

        try:
            df_pod = pd.read_csv(data_dir+"/pod.csv")
            df_pod.loc[:, 'datetime'] = pd.to_datetime(df_pod['datetime'], utc = True)

        except Exception as e:
            standardLog.sending_log('error', e).error('something wrong during pod data load')

        return df_pod

    def retrieve_vm_list(self, df_cpu):
        vm_list = list(df_cpu['host_name'].unique())
        vm_list_new = vm_list.copy()
        for vm in vm_list:
            if (len(df_cpu[df_cpu['host_name']==vm]) < self.duration * 24 * 12 * 0.8 * 0.1) and False:
                vm_list_new.remove(vm)

        return vm_list_new

    def retrieve_prediction_data(self, vm_list, provider):
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

        def diagnosis_detail(vm_info, vm_name, current, pred, pred_model, provider):
            vm_env = self.vm_env
            if provider == 'openstack':
                if vm_env[vm_env['name'] == vm_name].empty:
                    vcpu = 4
                    mem = 8192
                    disk = 50
                    vm_id = 'meta_error'
                else:
                    vcpu = int(vm_env[vm_env['name'] == vm_name].iloc[-1]['vcpus'])
                    mem = int(vm_env[vm_env['name'] == vm_name].iloc[-1]['localMemory'])
                    vm_id = vm_env[vm_env['name'] == vm_name].iloc[-1]['id']
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
                diag = 'resource ???????????? ??????????????? ?????? vm??? ???????????? ?????? ?????? ?????? ??????'

                analysis = '{}??? ??? cpu_ready {:0.3f}%, memory_usage {:0.3f}%, ?????? 15?????? cpu ????????? {:0.3f}%??? resource ????????? ????????? ??????'.format(
                    self.duration, current['cpu_ready']*100, pred['mem_usage']*100, current['cpu_recent']*100 / vcpu
                )

                effect = 'vm ????????? ?????? host ????????? ?????? ??? ?????? ????????? ?????????'
                vm_info = vm_info.append(pd.Series(
                    [vm_id, vm_name, datetime_now, 'over_use', analysis, 'null', 0, 0, False, effect,
                     self.duration, pred_model, timestamp_now], index=vm_info.columns), ignore_index=True)
            elif is_zombi(current, pred):
                diag = '???????????? ?????? vm??? ?????? ?????? ??????'
                analysis = '{}??? ??? net_usage {:0.3f}kB/s, disk_usage {:0.3f}kB/s??? ???????????? ?????? ?????? inactive ????????? ??????'.format(
                    self.duration, pred['net_usage'], pred['disk_usage']
                )
                effect = 'vm ????????? ?????? host ??? vm ?????? ?????????'
                vm_info = vm_info.append(pd.Series(
                    [vm_id, vm_name, datetime_now, 'inactive', analysis, 'null', 0, 0, False, effect,
                     self.duration, pred_model, timestamp_now], index=vm_info.columns), ignore_index=True)
            else:
                if is_cpu_undersized(current, pred):
                    diag = 'vCPU ?????? {}????????? {}?????? ?????? ??????'.format(vcpu, vcpu*2)
                    analysis = '{}??? ??? cpu_usage {:0.3f}%, cpu_ready {:0.3f}%??? undersized ????????? ??????'.format(
                        self.duration, pred['cpu_usage']*100, current['cpu_ready']*100
                    )

                    effect = '{}??? ??? cpu_usage {:0.3f}% ???????????? ?????? ??????'.format(self.duration, pred['cpu_usage']*100/2)
                    vm_info = vm_info.append(pd.Series(
                        [vm_id, vm_name, datetime_now, 'undersized', analysis, 'cpu', vcpu, vcpu*2, False, effect,
                         self.duration, pred_model, timestamp_now], index=vm_info.columns), ignore_index=True)
                elif is_cpu_oversized(current, pred):
                    if vcpu > 1:
                        diag = 'vCPU ?????? {}????????? {}?????? ?????? ??????'.format(vcpu, vcpu//2)
                        analysis = '{}??? ??? cpu_usage {:0.3f}%, cpu_ready {:0.3f}%??? oversized ????????? ??????'.format(
                            self.duration, pred['cpu_usage']*100, current['cpu_ready']*100)

                        effect = '{}??? ??? cpu_usage {:0.3f}% ???????????? ?????? ??????'.format(self.duration,
                                                                              pred['cpu_usage']*100*(vcpu/(vcpu//2)))
                        vm_info = vm_info.append(pd.Series(
                            [vm_id, vm_name, datetime_now, 'oversized', analysis, 'cpu', vcpu, vcpu // 2, False, effect,
                             self.duration, pred_model, timestamp_now], index=vm_info.columns), ignore_index=True)
                if is_mem_undersized(current, pred):
                    diag = 'Memory ????????? {}MB?????? {}MB??? ?????? ??????'.format(mem, mem*2)
                    analysis = '{}??? ??? mem_usage {:0.3f}%, mem_swapped {}kB??? undersized ????????? ??????'.format(
                        self.duration, pred['mem_usage']*100, current['mem_swap'])
                    effect = '{}??? ??? mem_usage {:0.3f}% ???????????? ?????? ??????'.format(self.duration, pred['mem_usage']*100/2)
                    vm_info = vm_info.append(pd.Series(
                        [vm_id, vm_name, datetime_now, 'undersized', analysis, 'memory', mem, mem*2, False, effect,
                         self.duration, pred_model, timestamp_now], index=vm_info.columns), ignore_index=True)
                elif is_mem_oversized(current, pred):
                    if mem > 1:
                        diag = 'Memory ????????? {}?????? {}??? ?????? ??????'.format(mem, mem*2)
                        analysis = '{}??? ??? mem_usage {:0.3f}%, mem_swapped {}kB??? oversized ????????? ??????'.format(
                            self.duration, pred['mem_usage']*100, current['mem_swap']
                        )
                        effect = '{}??? ??? mem_usage {:0.3f}% ???????????? ?????? ??????'.format(
                            self.duration, pred['mem_usage']*100*2)
                        vm_info = vm_info.append(pd.Series(
                            [vm_id, vm_name, datetime_now, 'oversized', analysis, 'memory', mem, mem//2, False, effect,
                             self.duration, pred_model, timestamp_now], index=vm_info.columns), ignore_index=True)
                if is_filesystem_undersized(current, pred):
                    diag = 'Disk ????????? {}GB?????? {}GB??? ?????? ??????'.format(disk, disk*2)
                    disk_remain_days_90pct = (0.9 - current['filesystem_usage']) /\
                                             (pred['filesystem_usage'] - current['filesystem_usage']) * self.duration
                    disk_remain_days_90pct = max(0, disk_remain_days_90pct)
                    analysis = '{}??? ??? filesystem_usage {:0.3f}%??? undersized ????????? ??????. {:0.1f}??? ??? 90% ?????? ?????? ??????'\
                        .format(self.duration, pred['filesystem_usage']*100, disk_remain_days_90pct)
                    effect = '{}??? ??? filesystem_usage {:0.3f}% ???????????? ?????? ??????'.format(self.duration, pred['filesystem_usage']*100/2)
                    vm_info = vm_info.append(pd.Series(
                        [vm_id, vm_name, datetime_now, 'undersized', analysis, 'disk', disk, disk*2, False, effect,
                         self.duration, pred_model, timestamp_now], index=vm_info.columns), ignore_index=True)

            return vm_info

        def filter_df_list(df_list, vm_name):
            return [df[df['host_name'] == vm_name].copy() if df is not None else pd.DataFrame() for df in df_list]

        def get_para(df_cpu, df_mem, df_disk, df_net, df_filesystem, df_pred_cpu, df_pred_mem, df_pred_net_in,
                     df_pred_net_out, df_pred_disk_read, df_pred_disk_write, df_pred_filesystem):

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

    def write_new_data(self, dict):
        ins = self.vm_info.insert().values(dict)
        result = self.conn.execute(ins)

        return None

    def write_data_es(self, vm_info):
        config = self.config
        date = datetime.datetime.now(datetime.timezone.utc).strftime('%Y.%m.%d')
        es = self.es


        for i in range(len(vm_info)):
            doc = vm_info.iloc[i].to_dict()
            res = es.index(index = 'sym-vm-rightsizing-{date}'.format(date = date),
                           body = doc)


        return None

    def run(self):
        start_time = datetime.datetime.now(datetime.timezone.utc)
        try:
            metadata = self.retrieve_meta_data()
            print('{} resource meta data detected'.format(len(metadata)))
        except Exception as e:
            standardLog.sending_log('error', e).error(f'something wrong during loading metadata from')
            reporting.report_result(result='fail', error='read')
        try:
            df_cpu, df_mem, df_disk, df_net, df_filesystem = self.retrieve_data()
        except Exception as e:
            standardLog.sending_log('error', e).error(f'something wrong during loading rawdata')
            reporting.report_result(result='fail', error='read')
        try:
            df_pod = self.retrieve_pod_data()
        except Exception as e:
            print(e)
            print('something wrong during loading pod rawdata')
            standardLog.sending_log('error', e).error(f'something wrong during loading pod rawdata')
            reporting.report_result(result='fail', error='read')
        for provider in self.provider_list:
            if provider == 'openshift':
                try:
                    try:
                        vm_list = self.retrieve_vm_list(df_pod)
                        print('{} pods detected in raw data'.format(len(vm_list)))
                    except Exception as e:
                        standardLog.sending_log('error', e).error(f'something wrong during loading vm list from {provider}')
                        reporting.report_result(result='fail', error='read')
                    try:
                        pred = self.retrieve_prediction_data(vm_list, 'openshift')
                        pred['diskio-read'] = None
                        pred['diskio-write'] = None
                        pred['network-in'] = None
                        pred['network-out'] = None
                        pred['filesystem_usage'] = None

                    except Exception as e:
                        standardLog.sending_log('error', e).error(f'something wrong during loading prediction data from {provider}')
                        reporting.report_result(result='fail', error='read')
                    try:
                        pred_model = pred['cpu']['model'].iloc[0]
                        df_pod_cpu = df_pod[['datetime', 'mean_cpu_usage', 'host_name']]
                        df_pod_mem = df_pod[['datetime', 'mean_memory_usage', 'host_name']]

                        vm_info, _ = self.calculate_type(df_pod_cpu, df_pod_mem, None, None, None,
                                                         pred, vm_list, pred_model, provider)
                        vm_info = vm_info.dropna()
                    except Exception as e:
                        standardLog.sending_log('error', e).error(f'something wrong during calculating type of pods from {pred_model}')
                    try:
                        vm_info['provider'] = 'openshift'
                        tt = self.write_data_es(vm_info)
                    except Exception as e:
                        standardLog.sending_log('error', e).error(f'something wrong during writing data to ES from {provider}')
                        reporting.report_result(result='fail', error=f'write')
                except Exception as e:
                    standardLog.sending_log('error', e).error(f'something wrong during processing openshift')
                    reporting.report_result(result='fail', error=f'Fail to rightsizing : something wrong during processing openshift from {e}')
                    return False
            elif provider == 'openstack':
                try:
                    try:
                        vm_list = self.retrieve_vm_list(df_cpu)
                        print('{} vms detected in raw data'.format(len(vm_list)))
                    except Exception as e:
                        standardLog.sending_log('error', e).error(f'something wrong during loading vm list from {provider}')
                        reporting.report_result(result='fail', error=f'read')
                    try:
                        pred = self.retrieve_prediction_data(vm_list, 'openstack')
                    except Exception as e:
                        standardLog.sending_log('error', e).error(f'something wrong during loading prediction data from {provider}')
                        reporting.report_result(result='fail', error=f'read')
                    try:
                        pred_model = pred['cpu']['model'].iloc[0]
                        vm_info, _ = self.calculate_type(df_cpu, df_mem, df_disk, df_net, df_filesystem,
                                                         pred, vm_list, pred_model, provider)
                        vm_info = vm_info.dropna()
                    except Exception as e:
                        standardLog.sending_log('error', e).error(f'something wrong during calculating type of vms from {pred_model}')
                    try:
                        vm_info['provider'] = 'openstack'
                        tt = self.write_data_es(vm_info)
                    except Exception as e:
                        standardLog.sending_log('error', e).error(f'something wrong during writing data to ES from {provider}')
                        reporting.report_result(result='fail', error=f'write')
                except Exception as e:
                    standardLog.sending_log('error', e).error(f'something wrong during processing openstack')
                    reporting.report_result(result='fail', error=f'Fail to rightsizing : something wrong during processing openshift from {e}')
                    return False

        reporting.report_result(result='success')
        standardLog.sending_log('success').info('Resource Rightsizing success')

        return None

if __name__ == '__main__':

    rightsizing = Right_Sizing()
    rightsizing.run()