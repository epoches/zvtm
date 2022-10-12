# 9:30开始 周一到周五 股市运行期间对指定数据进行更新
# -*- coding: utf-8 -*-
import logging

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.blocking import BlockingScheduler
import time

from zvtm.domain import StockTradeDay

logger = logging.getLogger(__name__)

sched = BackgroundScheduler()

# -*- coding: utf-8 -*-
import logging

from apscheduler.schedulers.background import BackgroundScheduler
import datetime
from examples.recorder_utils import run_data_recorder
from zvtm import init_log
from zvtm.domain import Stock1mKdata,Stock1mHfqKdata
logger = logging.getLogger(__name__)
from schedule.utils.query_data import get_data
sched = BackgroundScheduler()



def get_stock():
    db = 'stock'
    sql = "select codes from stocks where  \
                   strategy = %s "
    arg = ['stock']
    df = get_data(db=db, sql=sql, arg=arg)
    return df.loc[0,'codes'].split(",")

@sched.scheduled_job('cron',day_of_week='mon-fri', hour=9, minute=30)
def record_stock_data(data_provider="em", entity_provider="em"):
    codes = get_stock()
    # A股后复权行情
    while True:
        now = datetime.datetime.now()
        if ((int(now.strftime("%H")) >= 9) and (int(now.strftime("%H")) <= 12)) or (int(now.strftime("%H")) >= 13  and int(now.strftime("%H")) < 15):
            # run_data_recorder(
            #     domain=Stock1mHfqKdata,
            #     data_provider=data_provider,
            #     entity_provider=entity_provider,
            #     day_data=False,
            #     sleeping_time=0,
            #     codes=codes,
            # )
            Stock1mHfqKdata.record_data(provider='em', sleeping_time=1,codes=codes)
            time.sleep(60)
        else:
            break


if __name__ == "__main__":
    init_log("em_1min_runner.log")
    record_stock_data()

    sched.start()

    sched._thread.join()



