# -*- coding: utf-8 -*-
import logging

from apscheduler.schedulers.background import BackgroundScheduler
from zvtm.domain import Block,BlockStock,Block1dKdata,Block1wkKdata,Block1monKdata,Stock1hHfqKdata,Block1hKdata,Stock15mHfqKdata,Block15mKdata,Stock5mHfqKdata,Block5mKdata,Stock1mHfqKdata,Block1mKdata


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


@sched.scheduled_job('cron',day_of_week='mon-fri', hour=15, minute=10)
def record_stock_data(data_provider="em", entity_provider="em"):
    # A股60m hfq
    run_data_recorder(
        domain=Stock1hHfqKdata,
        data_provider=data_provider,
        entity_provider=entity_provider,
        day_data=False,
        sleeping_time=0,
    )
    run_data_recorder(
        domain=Block1hKdata,
        data_provider=data_provider,
        entity_provider=entity_provider,
        day_data=False,
        sleeping_time=0,
    )
    # A股15m hfq
    run_data_recorder(
        domain=Stock15mHfqKdata,
        data_provider=data_provider,
        entity_provider=entity_provider,
        day_data=False,
        sleeping_time=0,
    )
    run_data_recorder(
        domain=Block15mKdata,
        data_provider=data_provider,
        entity_provider=entity_provider,
        day_data=False,
        sleeping_time=0,
    )
    # A股5m hfq
    run_data_recorder(
        domain=Stock5mHfqKdata,
        data_provider=data_provider,
        entity_provider=entity_provider,
        day_data=False,
        sleeping_time=0,
    )
    run_data_recorder(
        domain=Block5mKdata,
        data_provider=data_provider,
        entity_provider=entity_provider,
        day_data=False,
        sleeping_time=0,
    )
    # A股5m hfq
    run_data_recorder(
        domain=Stock1mHfqKdata,
        data_provider=data_provider,
        entity_provider=entity_provider,
        day_data=False,
        sleeping_time=0,
    )
    run_data_recorder(
        domain=Block1mKdata,
        data_provider=data_provider,
        entity_provider=entity_provider,
        day_data=False,
        sleeping_time=0,
    )




if __name__ == "__main__":
    init_log("em_kdata_1hlower_runner.log")

    record_stock_data()

    sched.start()

    sched._thread.join()
