# 9:30开始 周一到周五 股市运行期间对指定数据进行更新
# -*- coding: utf-8 -*-
import logging

from apscheduler.schedulers.background import BackgroundScheduler
import time

logger = logging.getLogger(__name__)

sched = BackgroundScheduler()

# -*- coding: utf-8 -*-
import logging
from apscheduler.schedulers.background import BackgroundScheduler
import datetime
from zvtm import init_log
from zvtm.domain import Stock1mKdata,Stock1mHfqKdata,Stock5mHfqKdata,Stock15mHfqKdata,Stock30mHfqKdata,Stock1hHfqKdata,Block1mKdata,Block5mKdata,Block15mKdata,Block30mKdata,Block1hKdata
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

def get_block():
    db = 'stock'
    sql = "select codes from stocks where  \
                   strategy = %s "
    arg = ['block']
    df = get_data(db=db, sql=sql, arg=arg)
    return df.loc[0,'codes'].split(",")

@sched.scheduled_job('cron',day_of_week='mon-fri', hour=9, minute=30)
def record_stock_data(data_provider="em", entity_provider="em"):
    codes = get_stock()
    blocks = get_block()
    # A股后复权行情
    while True:
        now = datetime.datetime.now()
        if ((int(now.strftime("%H")) >= 9) and (int(now.strftime("%H")) <= 12)) or (int(now.strftime("%H")) >= 13  and int(now.strftime("%H")) <= 15):
            # run_data_recorder(
            #     domain=Stock1mHfqKdata,
            #     data_provider=data_provider,
            #     entity_provider=entity_provider,
            #     day_data=False,
            #     sleeping_time=0,
            #     codes=codes,
            # )
            for code in codes:
                Stock1mHfqKdata.record_data(provider='em', sleeping_time=1,code=code)
                if int(now.strftime("%M")) == 0:
                    Stock5mHfqKdata.record_data(provider='em', sleeping_time=1, code=code)
                    Stock15mHfqKdata.record_data(provider='em', sleeping_time=1, code=code)
                    Stock30mHfqKdata.record_data(provider='em', sleeping_time=1, code=code)
                    Stock1hHfqKdata.record_data(provider='em', sleeping_time=1, code=code)
                if int(now.strftime("%M")) == 10 or int(now.strftime("%M")) == 20 or int(now.strftime("%M")) == 40 or int(now.strftime("%M")) == 50:
                    Stock5mHfqKdata.record_data(provider='em', sleeping_time=1, code=code)
                if int(now.strftime("%M")) == 5:
                    Stock5mHfqKdata.record_data(provider='em', sleeping_time=1, code=code)
                if int(now.strftime("%M")) == 15 or int(now.strftime("%M")) == 45:
                    Stock15mHfqKdata.record_data(provider='em', sleeping_time=1, code=code)
                if int(now.strftime("%M")) == 30:
                    Stock15mHfqKdata.record_data(provider='em', sleeping_time=1, code=code)
                    Stock30mHfqKdata.record_data(provider='em', sleeping_time=1, code=code)

            for block in blocks:
                Block1mKdata.record_data(provider='em', sleeping_time=1, code=block)
                if int(now.strftime("%M")) == 0:
                    Block5mKdata.record_data(provider='em', sleeping_time=1, code=code)
                    Block15mKdata.record_data(provider='em', sleeping_time=1, code=code)
                    Block30mKdata.record_data(provider='em', sleeping_time=1, code=code)
                    Block1hKdata.record_data(provider='em', sleeping_time=1, code=code)
                if int(now.strftime("%M")) == 10 or int(now.strftime("%M")) == 20 or int(now.strftime("%M")) == 40 or int(now.strftime("%M")) == 50:
                    Block5mKdata.record_data(provider='em', sleeping_time=1, code=code)
                if int(now.strftime("%M")) == 5:
                    Block5mKdata.record_data(provider='em', sleeping_time=1, code=code)
                if int(now.strftime("%M")) == 15 or int(now.strftime("%M")) == 45:
                    Block15mKdata.record_data(provider='em', sleeping_time=1, code=code)
                if int(now.strftime("%M")) == 30:
                    Block15mKdata.record_data(provider='em', sleeping_time=1, code=code)
                    Block30mKdata.record_data(provider='em', sleeping_time=1, code=code)
            time.sleep(30)
        else:
            break


if __name__ == "__main__":
    init_log("em_1min_runner.log")
    record_stock_data()

    sched.start()

    sched._thread.join()



