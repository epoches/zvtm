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
from zvtm.domain import Stock, Stock1dHfqKdata
from zvtm.domain import Index1dKdata

from zvtm.domain import  StockValuation


logger = logging.getLogger(__name__)

sched = BackgroundScheduler()


@sched.scheduled_job('cron',day_of_week='mon-fri', hour=15, minute=10)
def record_stock_data(data_provider="joinquant", entity_provider="joinquant"):
    # A股标的
    run_data_recorder(domain=Stock, data_provider=data_provider, sleeping_time=0, force_update=False)
    # 交易日
    run_data_recorder(domain=StockTradeDay, data_provider=data_provider, sleeping_time=0, day_data=False)
    # 上证指数
    run_data_recorder(
        domain=Index1dKdata, data_provider="joinquant", entity_provider="joinquant", code='000001', day_data=False
    )
    # A股后复权行情
    run_data_recorder(
        domain=Stock1dHfqKdata,
        data_provider=data_provider,
        entity_provider=entity_provider,
        day_data=False,
        sleeping_time=0,
    )
    # valuation
    run_data_recorder(
        domain=StockValuation,
        data_provider=data_provider,
        entity_provider=entity_provider,
        day_data=False,
        sleeping_time=0,
    )


if __name__ == "__main__":
    init_log("joinquant_kdata_runner.log")

    record_stock_data()

    sched.start()

    sched._thread.join()
