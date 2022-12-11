# -*- coding: utf-8 -*-
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
from zvtm.domain import Stock5mHfqKdata,Stock5mKdata
logger = logging.getLogger(__name__)

sched = BackgroundScheduler()


@sched.scheduled_job('cron',day_of_week='sun', hour=1, minute=10)
def record_stock_data(data_provider="joinquant", entity_provider="joinquant"):
    # A股行情
    run_data_recorder(
        domain=Stock5mHfqKdata,
        data_provider=data_provider,
        entity_provider=entity_provider,
        day_data=False,
        sleeping_time=0,
        start_timestamp='2020-01-01'
    )
    # A股后复权行情
    run_data_recorder(
        domain=Stock5mKdata,
        data_provider=data_provider,
        entity_provider=entity_provider,
        day_data=False,
        sleeping_time=0,
        start_timestamp='2020-01-01'
    )



if __name__ == "__main__":
    init_log("joinquant_5min_runner.log")

    record_stock_data()

    sched.start()

    sched._thread.join()
