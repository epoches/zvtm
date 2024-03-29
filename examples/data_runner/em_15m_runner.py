# -*- coding: utf-8 -*-
# emfinace更新容易出错
import logging

from apscheduler.schedulers.background import BackgroundScheduler

from zvtm.domain import StockTradeDay

logger = logging.getLogger(__name__)

sched = BackgroundScheduler()

# -*- coding: utf-8 -*-
import logging

from apscheduler.schedulers.background import BackgroundScheduler

from examples.recorder_utils import run_data_recorder
from zvtm import init_log
from zvtm.domain import  Stock15mHfqKdata,Block15mKdata
logger = logging.getLogger(__name__)

sched = BackgroundScheduler()


@sched.scheduled_job('cron',day_of_week='sat', hour=20, minute=10)
def record_stock_data(data_provider="em", entity_provider="em"):
    # A股15m hfq
    run_data_recorder(
        domain=Stock15mHfqKdata,
        data_provider=data_provider,
        entity_provider=entity_provider,
        day_data=False,
        sleeping_time=1,
    )
    run_data_recorder(
        domain=Block15mKdata,
        data_provider=data_provider,
        entity_provider=entity_provider,
        day_data=False,
        sleeping_time=1,
    )
# code = '000683',


if __name__ == "__main__":
    init_log("eastmoney_15m_runner.log")

    record_stock_data()

    sched.start()

    sched._thread.join()
