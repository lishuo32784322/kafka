# coding:utf-8
# author:ls
import time, datetime, json, re, os, sys, random, shutil
from kafka import KafkaConsumer
import multiprocessing


server_list = ['10.0.4.5:6667', '10.0.4.2:6667', '10.0.4.4:6667']


# 消费者
consumer = KafkaConsumer('prod_bd_app_log', auto_offset_reset='earliest', bootstrap_servers=server_list, auto_commit_interval_ms=1000, enable_auto_commit=True, group_id='lishuo')

#def process_data(data):
      
#pool = multiprocessing.Pool(5)
for message in consumer:
    print(message)
    time.sleep(1)
#pool.close()
#pool.join()
