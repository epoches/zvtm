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

logger = logging.getLogger(__name__)
sched = BackgroundScheduler()

def stock_zh_a_spot_em() -> pd.DataFrame:
    """
    东方财富网-沪深京 A 股-实时行情
    https://quote.eastmoney.com/center/gridlist.html#hs_a_board
    :return: 实时行情
    :rtype: pandas.DataFrame
    """
    url = "http://82.push2.eastmoney.com/api/qt/clist/get"
    params = {
        "pn": "1",
        "pz": "50000",
        "po": "1",
        "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2",
        "invt": "2",
        "fid": "f3",
        "fs": "m:0 t:6,m:0 t:80,m:1 t:2,m:1 t:23",
        "fields": "f9,f10,f12,f13,f14,f20,f21,f23,f114,f115,f124,f130,f131",
        "_": "1623833739532",
    }
    # f9	市盈率 动 总市值/预估全年净利润
    # f10	量比
    # f12	股票代码
    # f13	市场
    # f14	股票名称
    # f20	总市值
    # f21	流通市值
    # f23	市净率
    # f130	市销率TTM
    # f131	市现率TTM
    # f114	市盈率（静） 总市值/上年度净利润
    # f115	市盈率（TTM）
    r = requests.get(url, params=params)
    data_json = r.json()
    if not data_json["data"]["diff"]:
        return pd.DataFrame()
    temp_df = pd.DataFrame(data_json["data"]["diff"])
    temp_df.columns = [
        "pe_ttm1",
        "lb",
        "code",
        "entity",
        "name",
        "market_cap",
        "circulating_market_cap",
        "pb",
        "pe",
        "pe_ttm",
        "timestamp",
        "ps",
        "pcf",
    ]
    # temp_df.reset_index(inplace=True)
    # temp_df["index"] = temp_df.index + 1
    # temp_df.rename(columns={"index": "序号"}, inplace=True)
    temp_df = temp_df[
        [
            "pe_ttm1",
            "lb",
            "code",
            "entity",
            "name",
            "market_cap",
            "circulating_market_cap",
            "pb",

            "pe",
            "pe_ttm",
            "timestamp",
            "ps",
            "pcf",
        ]
    ]
    temp_df["entity"] = temp_df["entity"].apply(lambda x: 'sz' if x == 0 else 'sh' )
    temp_df["lb"] = pd.to_numeric(temp_df["lb"], errors="coerce")
    temp_df["pb"] = pd.to_numeric(temp_df["pb"], errors="coerce")
    dt = pd.to_datetime(temp_df["timestamp"], unit='s', errors="coerce")[0].replace(hour=0, minute=0, second=0)
    temp_df["timestamp"] = dt
    temp_df["pe"] = pd.to_numeric(temp_df["pe"], errors="coerce")
    temp_df["pe_ttm1"] = pd.to_numeric(temp_df["pe_ttm1"], errors="coerce")
    temp_df["ps"] = pd.to_numeric(temp_df["ps"], errors="coerce")
    temp_df["pcf"] = pd.to_numeric(temp_df["pcf"], errors="coerce")
    temp_df["pe_ttm"] = pd.to_numeric(temp_df["pe_ttm"], errors="coerce")
    temp_df["market_cap"] = pd.to_numeric(temp_df["market_cap"], errors="coerce")
    temp_df["circulating_market_cap"] = pd.to_numeric(temp_df["circulating_market_cap"], errors="coerce")
    return temp_df

def get_datas():
    df = stock_zh_a_spot_em()
    for i in range(len(df)):
        entity_id = "{}_{}_{}".format('stock', df.loc[i, "entity"], df.loc[i, "code"])
        df.loc[i, "entity_id"] = entity_id
        df.loc[i, "id"] = "{}_{}".format(to_time_str(entity_id), df.loc[i, "timestamp"].strftime('%Y-%m-%d'))
    provider = 'em'
    data_schema = StockValuation1

    db_engine = get_db_engine(provider, data_schema=data_schema)
    schema_cols = get_schema_columns(data_schema)
    cols = set(df.columns.tolist()) & set(schema_cols)
    df = df[cols]
    if pd_is_not_null(df):
       rs = df.to_sql(data_schema.__tablename__, db_engine, index=False, if_exists="append")
    return df['timestamp'][0],rs

@sched.scheduled_job('cron',day_of_week='mon-fri', hour=15, minute=10)
def record_stock_data(data_provider="em", entity_provider="em"):
    dt,rs = get_datas()
    msg = f"record {domain.__name__} success,数据来源: {data_provider}"
    msg += dt.strftime('%Y-%m-%d') + rs

    logger.info(msg)
    email_action.send_message(zvt_config["email_username"], msg, msg)



if __name__ == "__main__":
    init_log("em_valuation_runner.log")
    email_action = EmailInformer()
    record_stock_data()
    sched.start()
    sched._thread.join()
