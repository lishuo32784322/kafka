# coding:utf-8
# author:ls
import time, datetime, json, re, os, sys, random, shutil
from kafka import KafkaConsumer
import multiprocessing


server_list = ['10.0.4.5:6667', '10.0.4.2:6667', '10.0.4.4:6667']


# 消费者
consumer = KafkaConsumer('test_lishuo_log', auto_offset_reset='earliest', bootstrap_servers=server_list, auto_commit_interval_ms=1000, enable_auto_commit=True, group_id='lishuo')

def process_data(data):
    if data:
        filename = str(time.strftime("%Y-%m-%d_%H-%M", time.localtime(time.time())))+'.log'
        appname,os,v_name,pc_id,v_app,log_time,ip,ab_id,user_id,dev_id,imei,udid,idfa,device_token,v_os,model,brand,facturer,resolution,net,carrierstr=str(data['appName']),str(data['os']),str(data['v_name']),str(data['pc_id']),str(data['v_app']),str(data['log_time']),str(data['ip']),str(data['ab_id']),str(data['user_id']),str(data['dev_id']),str(data['imei']),str(data['udid']),str(data['idfa']),str(data['device_token']),str(data['v_os']),str(data['model']),str(data['brand']),str(data['facturer']),str(data['resolution']),str(data['net']),str(data['carrier'])
        for i in data['event']:
            key_list = []
            for key in i.keys():
                ket_list.append(key)
            keys = ','.join(['%s:%s'%(str(key), i[key]) for key in ket_list]) 
            with open('/mnt/bigdata_workspace/kafka/logs/'+filename, 'a')as f:
                f.write(str(appname+'\t'+os+'\t'+v_name+'\t'+pc_id+'\t'+v_app+'\t'+log_time+'\t'+ip+'\t'+ab_id+'\t'+user_id+'\t'+dev_id+'\t'+imei+'\t'+udid+'\t'+idfa+'\t'+device_token+'\t'+v_os+'\t'+model+'\t'+brand+'\t'+facturer+'\t'+resolution+'\t'+net+'\t'+carrierstr+'\t'+keys)+'\n')


      
pool = multiprocessing.Pool(5)
for message in consumer:
    try:
        data = json.loads(message.value.decode())
        if data:
            pool.apply_async(process_data, args=(data,))
    except Exception as e:
        print(e)
        time.sleep(1)
pool.close()
pool.join()
