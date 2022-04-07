# -*- coding: utf-8 -*-
# 保存日线后赋权数据到数据库,每日执行
import logging

from apscheduler.schedulers.background import BackgroundScheduler

from zvtm.domain import StockMoneyFlow
logger = logging.getLogger(__name__)

sched = BackgroundScheduler()

# -*- coding: utf-8 -*-
import logging

from apscheduler.schedulers.background import BackgroundScheduler

from examples.recorder_utils import run_data_recorder
from zvtm import init_log
import datetime

logger = logging.getLogger(__name__)

sched = BackgroundScheduler()


@sched.scheduled_job('cron',day_of_week='mon-fri', hour=1, minute=10)
def record_stock_data(data_provider="joinquant", entity_provider="joinquant"):
    # A股后复权行情
    run_data_recorder(
        domain=StockMoneyFlow,
        data_provider=data_provider,
        entity_provider=entity_provider,
        day_data=False,
        sleeping_time=0,
        end_timestamp=datetime.datetime.now().strftime("%Y-%m-%d"),
    )


if __name__ == "__main__":
    init_log("joinquant_Stock1dMoneyFlow_runner.log")

    record_stock_data()

    sched.start()

    sched._thread.join()
