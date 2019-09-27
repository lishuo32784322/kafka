import os 
import time 
import vthread
import logging


logging.basicConfig(level=logging.INFO, filename='./logs/export_log.error', datefmt='%Y/%m/%d %H:%M:%S', format='%(asctime)s - %(name)s - %(levelname)s - %(lineno)d - %(module)s - %(message)s')
logger = logging.getLogger(__name__)


@vthread.pool(5)
def to_hive(log_name, dt):
    try:
        os.system('''hive -e "load data local inpath '/mnt/bigdata_workspace/kafka/logs/%s' into table bigdata_test.test_log partition(dt='%s')"'''%(log_name, dt))
        os.remove('/mnt/bigdata_workspace/kafka/logs/%s'%log_name)
        logging.info(log_name)
    except Exception as e:
        logging.error(e)

f_list = os.listdir('/mnt/bigdata_workspace/kafka/logs')
for i in f_list:
    if os.path.splitext(i)[1] == '.log':
        try:
            dt = str(i.split('.')[0].split('_')[0])
            to_hive(i, dt)
        except Exception as e:
            logging.error(e)
