#保存日线后赋权数据到数据库,每日执行
from zvtm.domain import  StockValuation
from apscheduler.schedulers.background import BackgroundScheduler
sched = BackgroundScheduler()
from zvtm.informer.informer import EmailInformer
from zvtm import init_log, zvt_config

import logging
import time

# 自行更改定定时运行时间
@sched.scheduled_job('cron',day_of_week='mon-fri', hour=15, minute=30)
def run():
    while True:
        email_action = EmailInformer()
        try:
            StockValuation.record_data(provider='joinquant', sleeping_time=0, day_data=True)

            email_action.send_message(zvt_config['email_username'], 'joinquant record StockValuation finished', '')
            break
        except Exception as e:
            msg = f'joinquant StockValuation stock:{e}'
            logger.exception(msg)
            email_action.send_message(zvt_config['email_username'], 'joinquant record StockValuation error', msg)
            time.sleep(60 * 5)



if __name__ == '__main__':
    init_log('StockValuation.log')
    logger = logging.getLogger('__main__')

    run()
    sched.start()
    sched._thread.join()