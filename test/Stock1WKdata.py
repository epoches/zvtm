#保存周线joinquantkdata线后赋权数据到数据库,每日执行
from zvtm.domain import  Stock1dHfqKdata
from apscheduler.schedulers.background import BackgroundScheduler
sched = BackgroundScheduler()
from zvtm.informer.informer import EmailInformer
from zvtm import init_log, zvt_config
from zvtm.domain import Stock1wkKdata
from zvtm.domain import Stock1wkHfqKdata

import logging
import time

# 自行更改定定时运行时间
@sched.scheduled_job('cron',day_of_week='sat', hour=1, minute=10)
def run():
    while True:
        email_action = EmailInformer()
        try:
            Stock1wkKdata.record_data(provider='joinquant', sleeping_time=0, day_data=True)
            Stock1wkHfqKdata.record_data(provider='joinquant', sleeping_time=0)
            email_action.send_message(zvt_config['email_username'], 'joinquant record Stock1wKdata finished', '')
            break
        except Exception as e:
            msg = f'joinquant Stock1wKdata :{e}'
            logger.exception(msg)
            email_action.send_message(zvt_config['email_username'], 'joinquant record 1wkdata error', msg)
            time.sleep(60 * 5)



if __name__ == '__main__':
    init_log('Stock1wKdata.log')
    logger = logging.getLogger('__main__')

    run()
    sched.start()
    sched._thread.join()