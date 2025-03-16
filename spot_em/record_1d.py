# 实时行情入库  还是有问题 超时
"""
东方财富网-沪深京 A 股-实时行情
https://quote.eastmoney.com/center/gridlist.html#hs_a_board
:return: 实时行情
:rtype: pandas.DataFrame
"""
# 字段
# f2	最新价
# f6	成交额
# f7	振幅
# f8	换手率
# f12	股票代码
# f13	市场
# f14	股票名称
# f15	最高价
# f16	最低价
# f17	开盘价
# f18	昨收
# 市值,流通市值,pe,pb  f20,f21,f9,f23
# -*- coding: utf-8 -*-
import logging
from apscheduler.schedulers.background import BackgroundScheduler
from zvtm import init_log
import requests
import pandas as pd
from zvtm.utils.time_utils import now_pd_timestamp, to_time_str, to_pd_timestamp
from zvtm.utils.pd_utils import pd_is_not_null
from zvtm.contract.api import get_db_engine,get_schema_columns
from zvtm.domain import Stock1dKdata
from zvtm.contract.api import df_to_db
import datetime
from zvtm.informer import EmailInformer
from zvtm import zvt_config

logger = logging.getLogger(__name__)
sched = BackgroundScheduler()

from examples.data_runner.em_stock1d_runner import stock_zh_a_spot_em
@sched.scheduled_job('cron',day_of_week='mon-fri', hour=15, minute=5)
def record_stock_data():
    email_action = EmailInformer()
    msg = f"record stock1d success,数据来源: em"
    try:
        df = stock_zh_a_spot_em()
        force_update=True
        data_schema = Stock1dKdata
        provider ='em'
        db_engine = get_db_engine(provider, data_schema=data_schema)
        schema_cols = get_schema_columns(data_schema)
        cols = set(df.columns.tolist()) & set(schema_cols)
        df.rename(columns={"代码": "code"}, inplace=True)
        df.rename(columns={"名称": "name"}, inplace=True)
        df.rename(columns={"最新价": "close"}, inplace=True)
        df.rename(columns={"最高": "high"}, inplace=True)
        df.rename(columns={"今开": "open"}, inplace=True)
        df.rename(columns={"最低": "low"}, inplace=True)
        df.rename(columns={"成交量": "volume"}, inplace=True)
        df.rename(columns={"成交额": "turnover"}, inplace=True)
        df.rename(columns={"涨跌幅": "change_pct"}, inplace=True)
        df.rename(columns={"换手率": "turnover_rate"}, inplace=True)

        dt = datetime.datetime.now().strftime('%Y-%m-%d')
        for i in range(len(df)):
            entity_id = "{}_{}_{}".format('stock',df.loc[i,"entity"],df.loc[i,"code"])
            df.loc[i,"entity_id"] = entity_id
            df.loc[i,"id"] = "{}_{}".format(to_time_str(entity_id),dt)
            df.loc[i,"timestamp"] = dt
            df.loc[i, "level"] = "1d"
            df.loc[i, "provider"] = "em"
        df = df[schema_cols]
        if pd_is_not_null(df):
            df_to_db(df=df, data_schema=data_schema, provider=provider, force_update=True)
            # df.to_sql(data_schema.__tablename__, db_engine, index=False, if_exists="append")  一次性清库了
        logger.info(msg)
        email_action.send_message(zvt_config["email_username"], msg, msg)
    except Exception as e:
        logger.exception("report error:{}".format(e))
        email_action.send_message(
            zvt_config["email_username"],
            f"record stock1d error",
            f"record stock1d error: {e}",
        )

if __name__ == "__main__":
    init_log("em_kdata1d_runner.log")

    record_stock_data()

    sched.start()

    sched._thread.join()

