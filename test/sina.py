# -*- coding: utf-8 -*-
import logging
import time

from apscheduler.schedulers.background import BackgroundScheduler

from zvtm import init_log, zvt_config
from zvtm.domain import *
from zvtm.informer.informer import EmailInformer

logger = logging.getLogger(__name__)

Block.record_data(provider='sina', sleeping_time=1)
BlockStock.record_data(provider='sina', sleeping_time=1)
BlockMoneyFlow.record_data(provider='sina', sleeping_time=1)