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
from zvtm.domain import  BalanceSheet,FinanceFactor,CashFlowStatement,IncomeStatement
logger = logging.getLogger(__name__)

sched = BackgroundScheduler()


@sched.scheduled_job('cron',day_of_week='mon-fri', hour=14, minute=20)
def record_stock_data(data_provider="eastmoney", entity_provider="eastmoney"):
    # A股Finance
    run_data_recorder(
        domain=BalanceSheet,
        data_provider=data_provider,
        entity_provider=entity_provider,
        day_data=False,
        sleeping_time=1,
        force_update=True,
    )
    #
    run_data_recorder(
        domain=FinanceFactor,
        data_provider=data_provider,
        entity_provider=entity_provider,
        day_data=False,
        sleeping_time=1,
        force_update=True,
    )

    run_data_recorder(
        domain=CashFlowStatement,
        data_provider=data_provider,
        entity_provider=entity_provider,
        day_data=False,
        sleeping_time=1,
        force_update = True,
    )
    #
    run_data_recorder(
        domain=IncomeStatement,
        data_provider=data_provider,
        entity_provider=entity_provider,
        day_data=False,
        sleeping_time=1,
        force_update=True,
    )



if __name__ == "__main__":
    init_log("eastmoney_finance_runner.log")

    record_stock_data()

    sched.start()

    sched._thread.join()
