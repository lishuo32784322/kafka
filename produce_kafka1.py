import pickle
import time
from kafka import KafkaProducer
from kafka.errors import kafka_errors
producer = KafkaProducer(
    bootstrap_servers=['10.0.4.5:6667', '10.0.4.2:6667', '10.0.4.4:6667'],
   
)
start_time = time.time()
for i in range(1, 100):
    print('------{}---------'.format(i))
#    msg = '{"appName":"百搭%d","os":"and","v_name":"1.2.1","pc_id":"应用宝","v_app":"110","log_time":"155558299%d","ip":"192.168.1.1","ab_id":"53-100,63-200","user_id":"153572667726118901","dev_id":"4ad1ba7b-2c08-395c-ada5-5bda10d30565","imei":"","udid":"897DFD91756D3E953DF550442AF1EBC4","idfa":"3D390C73-6959-410C-9FB1-099F1FF26260","device_token":"","v_os":"5.1.1","model":"HUAWEIGRA-CL10","brand":"HUAWEI","facturer":"HUAWEI","resolution":"1920x1080","net":"1","carrier":"中国联通","event":[{"event_id":"view","source":"1","post_id":"134098527256287657","mtype":"1","impressionId":"f1aa6227a3630b7c08169f8a3a8fc75b","flag":"user_cf,lda","start_time":"1565239413000","end_time":"1565239453000"},{"event_id":"click","source":"1","post_id":"134098527256287657","mtype":"1","impressionId":"f1aa6227a3630b7c08169f8a3a8fc75b","flag":"user_cf,lda","search_word":""},{"event_id":"play","source":"1","post_id":"134098527256287657","mtype":"1","impressionId":"f1aa6227a3630b7c08169f8a3a8fc75b","duration":"413000","play_duration":"213000","flag":"user_cf,lda"}]}'% (i,i)
#    msg = str({"appName":"百搭","os":"and","v_name":"1.2.1","pc_id":"应用宝","v_app":"110","log_time":"155558299%d","ip":"192.168.1.1","ab_id":"53-100,63-200","user_id":"153572667726118901","dev_id":"4ad1ba7b-2c08-395c-ada5-5bda10d30565","imei":"","udid":"897DFD91756D3E953DF550442AF1EBC4","idfa":"3D390C73-6959-410C-9FB1-099F1FF26260","device_token":"","v_os":"5.1.1","model":"HUAWEIGRA-CL10","brand":"HUAWEI","facturer":"HUAWEI","resolution":"1920x1080","net":"1","carrier":"中国联通","event":{"event_id":"view","source":"1","post_id":"134098527256287657","mtype":"1","impressionId":"f1aa6227a3630b7c08169f8a3a8fc75b","flag":"user_cf,lda","start_time":"1565239413000","end_time":"1565239453000"}})
    msg = '%7B%27appName%27%3A%20%27%E7%99%BE%E6%90%AD%27%2C%20%27os%27%3A%20%27and%27%2C%20%27v_name%27%3A%20%271.2.1%27%2C%20%27pc_id%27%3A%20%27%E5%BA%94%E7%94%A8%E5%AE%9D%27%2C%20%27v_app%27%3A%20%27110%27%2C%20%27log_time%27%3A%20%27155558299%25d%27%2C%20%27ip%27%3A%20%27192.168.1.1%27%2C%20%27ab_id%27%3A%20%2753-100%2C63-200%27%2C%20%27user_id%27%3A%20%27153572667726118901%27%2C%20%27dev_id%27%3A%20%274ad1ba7b-2c08-395c-ada5-5bda10d30565%27%2C%20%27imei%27%3A%20%27%27%2C%20%27udid%27%3A%20%27897DFD91756D3E953DF550442AF1EBC4%27%2C%20%27idfa%27%3A%20%273D390C73-6959-410C-9FB1-099F1FF26260%27%2C%20%27device_token%27%3A%20%27%27%2C%20%27v_os%27%3A%20%275.1.1%27%2C%20%27model%27%3A%20%27HUAWEIGRA-CL10%27%2C%20%27brand%27%3A%20%27HUAWEI%27%2C%20%27facturer%27%3A%20%27HUAWEI%27%2C%20%27resolution%27%3A%20%271920x1080%27%2C%20%27net%27%3A%20%271%27%2C%20%27carrier%27%3A%20%27%E4%B8%AD%E5%9B%BD%E8%81%94%E9%80%9A%27%2C%20%27event%27%3A%20%7B%27event_id%27%3A%20%27view%27%2C%20%27source%27%3A%20%271%27%2C%20%27post_id%27%3A%20%27134098527256287657%27%2C%20%27mtype%27%3A%20%271%27%2C%20%27impressionId%27%3A%20%27f1aa6227a3630b7c08169f8a3a8fc75b%27%2C%20%27flag%27%3A%20%27user_cf%2Clda%27%2C%20%27start_time%27%3A%20%271565239413000%27%2C%20%27end_time%27%3A%20%271565239453000%27%7D%7D'
    future = producer.send(topic="test_ls_log", msg.encode('utf-8'))
    # 同步阻塞,通过调用get()方法进而保证一定程序是有序的.
    try:
        record_metadata = future.get(timeout=2)
        # print(record_metadata.topic)
        # print(record_metadata.partition)
        # print(record_metadata.offset)
    except kafka_errors as e:
        print(str(e))
end_time = time.time()
time_counts = end_time - start_time
print(time_counts)
