# -*- coding: utf-8 -*-

from zvtm import zvt_config
from zvtm.informer import EmailInformer
import demjson
from zvtm.domain import Index1dKdata
import logging
from apscheduler.schedulers.background import BackgroundScheduler
from zvtm.contract.api import get_db_engine, get_schema_columns
from zvtm import init_log
# from zvtm.domain.fundamental.valuation1 import StockValuation1
import requests
import pandas as pd
from zvtm.utils.time_utils import now_pd_timestamp, to_time_str, to_pd_timestamp
from zvtm.utils.pd_utils import pd_is_not_null
from zvtm.contract.api import get_data
from schedule.utils.query_data import get_data as get_data_sch
import datetime
from zvtm.contract.api import df_to_db

logger = logging.getLogger(__name__)
sched = BackgroundScheduler()


def stock_zh_index_daily_em(symbol: str = "sh000913",beg:str=datetime.datetime.now().strftime("%Y%m%d")) -> pd.DataFrame:
    """
    东方财富网-股票指数数据
    https://quote.eastmoney.com/center/hszs.html
    :param symbol: 带市场标识的指数代码
    :type symbol: str
    :return: 指数数据
    :rtype: pandas.DataFrame
    """
    market_map = {"sz": "0", "sh": "1"}
    url = "http://push2his.eastmoney.com/api/qt/stock/kline/get"
    params = {
        "cb": "jQuery1124033485574041163946_1596700547000",
        "secid": f"{market_map[symbol[:2]]}.{symbol[2:]}",
        "ut": "fa5fd1943c7b386f172d6893dbfba10b",
        "fields1": "f1,f2,f3,f4,f5",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58",
        "klt": "101",  # 日频率
        "fqt": "0",
        "beg": beg,
        "end": "20320101",
        "_": "1596700547039",
    }
    r = requests.get(url, params=params)
    data_text = r.text
    data_json = demjson.decode(data_text[data_text.find("{") : -2])
    temp_df = pd.DataFrame([item.split(",") for item in data_json["data"]["klines"]])
    temp_df.columns = ["date", "open", "close", "high", "low", "volume", "amount", "_"]
    temp_df = temp_df[["date", "open", "close", "high", "low", "volume", "amount"]]

    temp_df["open"] = pd.to_numeric(temp_df["open"])
    temp_df["close"] = pd.to_numeric(temp_df["close"])
    temp_df["high"] = pd.to_numeric(temp_df["high"])
    temp_df["low"] = pd.to_numeric(temp_df["low"])
    temp_df["volume"] = pd.to_numeric(temp_df["volume"])
    temp_df["amount"] = pd.to_numeric(temp_df["amount"])
    temp_df['level'] = '1d'
    temp_df['turnover'] = 0
    temp_df['change_pct'] = 0
    temp_df['turnover_rate'] = 0
    temp_df['entity_id'] = 'index_sh_000001'
    temp_df['code'] = '000001'
    temp_df['provider'] = 'em'
    temp_df['name'] = '上证指数'
    for i in range(len(temp_df)):
        temp_df.loc[i,'id'] = "{}_{}".format(temp_df.loc[i, "entity_id"], temp_df.loc[i, "date"])
    # print(temp_df)
    return temp_df




def get_datas():
    dt = datetime.datetime.now().strftime("%Y%m%d")
    df = stock_zh_index_daily_em(symbol="sh000001", beg=dt)
    force_update = True
    data_schema = Index1dKdata
    provider = 'em'
    # db_engine = get_db_engine(provider, data_schema=data_schema)
    schema_cols = get_schema_columns(data_schema)
    cols = set(df.columns.tolist()) & set(schema_cols)
    df.rename(columns={"date": "timestamp"}, inplace=True)
    df = df[schema_cols]
    if pd_is_not_null(df):
        df_to_db(df=df, data_schema=data_schema, provider=provider, force_update=force_update)



def record_stock_data(data_provider="em", entity_provider="em"):
    get_datas()
    msg = f"record emindex1dsh000001 success,数据来源: em"
    logger.info(msg)
    email_action.send_message(zvt_config["email_username"], msg, msg)

@sched.scheduled_job('cron',day_of_week='mon-fri', hour=15, minute=1)
def isopen():
    dt = datetime.datetime.now().strftime('%Y-%m-%d')
    db = 'tushare'
    sql = "select timestamp from trade_day where timestamp = %s "
    arg = [dt]
    df = get_data_sch(db=db, sql=sql, arg=arg)
    if len(df) > 0:
        record_stock_data()

if __name__ == "__main__":
    init_log("em_index1d000001_runner.log")
    email_action = EmailInformer()
    isopen()
    sched.start()
    sched._thread.join()
