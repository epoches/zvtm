# -*- coding: utf-8 -*-

from zvtm import zvt_config
from zvtm.informer import EmailInformer

import logging
from apscheduler.schedulers.background import BackgroundScheduler
from zvtm.contract.api import get_db_engine, get_schema_columns
from zvtm import init_log
from zvtm.domain.fundamental.valuation1 import StockValuation1
import requests
import pandas as pd
from zvtm.utils.time_utils import now_pd_timestamp, to_time_str, to_pd_timestamp
from zvtm.utils.pd_utils import pd_is_not_null
from zvtm.contract.api import df_to_db
# from zvtm.contract.api import get_data
from schedule.utils.query_data import get_data as get_data_sch
import datetime
logger = logging.getLogger(__name__)
sched = BackgroundScheduler()
from zvtm.ak.stock.stock_info_em import code_id_map_em
from  zvtm.ak.stock_feature.stock_hist_em import stock_zh_a_hist





def get_datas():
    df = stock_zh_a_hist()
    print(df)


get_datas()