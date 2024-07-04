import logging

from apscheduler.schedulers.background import BackgroundScheduler
from zvtm.domain import Block,BlockStock,Block1dKdata,Block1wkKdata,Block1monKdata


logger = logging.getLogger(__name__)

sched = BackgroundScheduler()

# -*- coding: utf-8 -*-
import logging

from apscheduler.schedulers.background import BackgroundScheduler

from examples.recorder_utils import run_data_recorder
from zvtm import init_log
from zvtm.domain import Stock,StockDetail,Stock1mKdata,Stock1dKdata,Stock1dHfqKdata,Stock1wkHfqKdata,Stock1monHfqKdata

from zvtm.domain import Index1dKdata
from zvtm.domain import  StockValuation


logger = logging.getLogger(__name__)

sched = BackgroundScheduler()


@sched.scheduled_job('cron',day_of_week='sat', hour=1, minute=10)
def record_stock_data(data_provider="em", entity_provider="em"):
    # A股标的
    # run_data_recorder(domain=Stock, data_provider='exchange', force_update=True)
    run_data_recorder(domain=Stock, data_provider='em', force_update=True)


if __name__ == "__main__":
    init_log("em_kdata_runner_stock.log")

    record_stock_data()
