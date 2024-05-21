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
    # A股标的
    # run_data_recorder(domain=Stock, data_provider="eastmoney", sleeping_time=0)
    # run_data_recorder(domain=StockDetail, data_provider="eastmoney", sleeping_time=0)
    # run_data_recorder(domain=Block, data_provider=data_provider, sleeping_time=0, force_update=False)
    # run_data_recorder(domain=BlockStock, data_provider=data_provider, sleeping_time=0, force_update=False)

    run_data_recorder(
        domain=Stock1dKdata,
        data_provider=data_provider,
        entity_provider=entity_provider,
        day_data=False,
        sleeping_time=0,
    )
    # # A股后复权行情
    run_data_recorder(
        domain=Stock1dHfqKdata,
        data_provider=data_provider,
        entity_provider=entity_provider,
        day_data=False,
        sleeping_time=0,
    )
    # block
    run_data_recorder(
        domain=Block1dKdata,
        data_provider=data_provider,
        entity_provider=entity_provider,
        day_data=False,
        sleeping_time=0,
    )

    # run_data_recorder(
    #     domain=Stock1wkHfqKdata,
    #     data_provider=data_provider,
    #     entity_provider=entity_provider,
    #     day_data=False,
    #     sleeping_time=1,
    # )
    # run_data_recorder(
    #     domain=Stock1monHfqKdata,
    #     data_provider=data_provider,
    #     entity_provider=entity_provider,
    #     day_data=False,
    #     sleeping_time=1,
    # )

    # run_data_recorder(
    #     domain=Block1wkKdata,
    #     data_provider=data_provider,
    #     entity_provider=entity_provider,
    #     day_data=False,
    #     sleeping_time=1,
    # )
    # run_data_recorder(
    #     domain=Block1monKdata,
    #     data_provider=data_provider,
    #     entity_provider=entity_provider,
    #     day_data=False,
    #     sleeping_time=1,
    # )



if __name__ == "__main__":
    init_log("em_kdata_runner.log")

    record_stock_data()

    sched.start()

    sched._thread.join()
