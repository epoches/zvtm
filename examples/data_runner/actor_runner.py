# -*- coding: utf-8 -*-
import logging

from apscheduler.schedulers.background import BackgroundScheduler

from examples.recorder_utils import run_data_recorder
from examples.utils import add_to_eastmoney
from zvtm import init_log, zvt_config
from zvtm.domain import (
    Stock,
    Stock1dHfqKdata,
    Stockhk,
    Stockhk1dHfqKdata,
    Block,
    Block1dKdata,
    BlockCategory,
    StockInstitutionalInvestorHolder,
    StockTopTenFreeHolder,
    StockActorSummary,
)
from zvtm.informer import EmailInformer
from zvtm.utils import next_date, current_date

logger = logging.getLogger(__name__)

sched = BackgroundScheduler()


@sched.scheduled_job("cron", hour=1, minute=00, day_of_week=2)
def record_actor_data(data_provider="em", entity_provider="em"):
    run_data_recorder(
        domain=StockInstitutionalInvestorHolder,
        data_provider=data_provider,
        entity_provider=entity_provider,
        day_data=True,
    )
    run_data_recorder(
        domain=StockTopTenFreeHolder, data_provider=data_provider, entity_provider=entity_provider, day_data=True
    )
    run_data_recorder(
        domain=StockActorSummary, data_provider=data_provider, entity_provider=entity_provider, day_data=True
    )


if __name__ == "__main__":
    init_log("actor_runner.log")

    record_actor_data()

    sched.start()

    sched._thread.join()
