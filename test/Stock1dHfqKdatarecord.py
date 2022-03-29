#保存日线后赋权数据到数据库,每日执行，增加保存大盘指数000001到数据库
from zvtm.domain import  Stock1dHfqKdata,Index1dKdata,StockTradeDay
from apscheduler.schedulers.background import BackgroundScheduler
sched = BackgroundScheduler()
from zvtm.informer.informer import EmailInformer
from zvtm import init_log, zvt_config

import logging
import time

# 自行更改定定时运行时间
@sched.scheduled_job('cron',day_of_week='mon-fri', hour=15, minute=10)
def run():
    while True:
        email_action = EmailInformer()
        try:
            Stock1dHfqKdata.record_data(provider='joinquant', sleeping_time=0, day_data=True)
            Index1dKdata.record_data(provider='joinquant', code='000001')
            StockTradeDay.record_data(provider='joinquant')
            email_action.send_message(zvt_config['email_username'], 'joinquant record Stock1dHfqKdata Index1dkdata finished', '')
            break
        except Exception as e:
            msg = f'joinquant Stock1dHfqKdata Index1dkdata stock:{e}'
            logger.exception(msg)
            email_action.send_message(zvt_config['email_username'], 'joinquant record Stock Index1dkdata error', msg)
            time.sleep(60 * 5)



if __name__ == '__main__':
    init_log('Stock1dHfqKdata.log')
    logger = logging.getLogger('__main__')

    run()
    sched.start()
    sched._thread.join()