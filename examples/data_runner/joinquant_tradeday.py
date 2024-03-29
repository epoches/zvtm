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
from zvtm.domain import Index1dKdata,StockMoneyFlow



logger = logging.getLogger(__name__)

sched = BackgroundScheduler()


@sched.scheduled_job('cron',day_of_week='mon-fri', hour=3, minute=10)
def record_stock_data(data_provider="joinquant", entity_provider="joinquant"):
    # 交易日
    run_data_recorder(domain=StockTradeDay, data_provider=data_provider, sleeping_time=0, day_data=False)
    run_data_recorder(
        domain=Index1dKdata, data_provider="joinquant", entity_provider="joinquant", code='000001', day_data=False
    )
    run_data_recorder(
        domain=StockMoneyFlow, data_provider="joinquant", entity_provider="joinquant", day_data=False
    )


if __name__ == "__main__":
    init_log("joinquant_tradeday_runner.log")

    record_stock_data()

    sched.start()

    sched._thread.join()
