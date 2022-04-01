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
from zvtm.domain import  FinanceFactor
logger = logging.getLogger(__name__)

sched = BackgroundScheduler()


@sched.scheduled_job('cron',day_of_week='mon-fri', hour=15, minute=10)
def record_stock_data(data_provider="eastmoney", entity_provider="eastmoney"):
    # A股Finance
    run_data_recorder(
        domain=FinanceFactor,
        data_provider=data_provider,
        entity_provider=entity_provider,
        day_data=False,
        sleeping_time=5,
    )



if __name__ == "__main__":
    init_log("eastmoney_1d_runner.log")

    record_stock_data()

    sched.start()

    sched._thread.join()
