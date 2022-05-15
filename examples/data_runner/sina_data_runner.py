# -*- coding: utf-8 -*-
import logging
import time

from apscheduler.schedulers.background import BackgroundScheduler

from examples.recorder_utils import run_data_recorder
from zvtm import init_log, zvt_config
from zvtm.domain import *
from zvtm.informer.informer import EmailInformer

logger = logging.getLogger(__name__)

sched = BackgroundScheduler()


@sched.scheduled_job("cron", hour=15, minute=30, day_of_week=3)
def record_block():
    run_data_recorder(domain=Block, data_provider="sina")
    run_data_recorder(domain=Block, data_provider="sina", entity_provider="sina")


@sched.scheduled_job("cron", hour=15, minute=30)
def record_money_flow():
    run_data_recorder(domain=BlockMoneyFlow, data_provider="sina", entity_provider="sina", day_data=True)


if __name__ == "__main__":
    init_log("sina_data_runner.log")

    record_block()
    record_money_flow()

    sched.start()

    sched._thread.join()
