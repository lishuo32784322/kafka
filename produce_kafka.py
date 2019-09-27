# coding:utf-8
# author:ls
import time, datetime, json, re, os, sys, random, shutil
import gevent.monkey
gevent.monkey.patch_all()
from gevent.pool import Pool
from kafka import KafkaConsumer
from kafka import KafkaProducer

server_list = ['10.0.4.5:6667', '10.0.4.2:6667', '10.0.4.4:6667']
producer = KafkaProducer(bootstrap_servers=server_list,compression_type='gzip')

for i in range(100):
    msg = '{"appName":"百搭%d","os":"and","v_name":"1.2.1","pc_id":"应用宝","v_app":"110","log_time":"155558299%d","ip":"192.168.1.1","ab_id":"53-100,63-200","user_id":"153572667726118901","dev_id":"4ad1ba7b-2c08-395c-ada5-5bda10d30565","imei":"","udid":"897DFD91756D3E953DF550442AF1EBC4","idfa":"3D390C73-6959-410C-9FB1-099F1FF26260","device_token":"","v_os":"5.1.1","model":"HUAWEIGRA-CL10","brand":"HUAWEI","facturer":"HUAWEI","resolution":"1920x1080","net":"1","carrier":"中国联通","event":[{"event_id":"view","source":"1","post_id":"134098527256287657","mtype":"1","impressionId":"f1aa6227a3630b7c08169f8a3a8fc75b","flag":"user_cf,lda","start_time":"1565239413000","end_time":"1565239453000"},{"event_id":"click","source":"1","post_id":"134098527256287657","mtype":"1","impressionId":"f1aa6227a3630b7c08169f8a3a8fc75b","flag":"user_cf,lda","search_word":""},{"event_id":"play","source":"1","post_id":"134098527256287657","mtype":"1","impressionId":"f1aa6227a3630b7c08169f8a3a8fc75b","duration":"413000","play_duration":"213000","flag":"user_cf,lda"}]}'% (i,i)
    producer.send('test_lishuo_log', msg.encode('utf-8'))
producer.close()


