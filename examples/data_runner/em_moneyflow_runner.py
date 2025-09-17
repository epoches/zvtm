# -*- coding: utf-8 -*-

from zvtm import zvt_config
from zvtm.informer import EmailInformer

import logging
from apscheduler.schedulers.background import BackgroundScheduler
from zvtm.contract.api import get_db_engine, get_schema_columns
from zvtm import init_log
import requests
import pandas as pd
from zvtm.utils.time_utils import now_pd_timestamp, to_time_str, to_pd_timestamp
from zvtm.utils.pd_utils import pd_is_not_null
from zvtm.domain import StockMoneyFlow1
from zvtm.contract.api import df_to_db
# from zvtm.contract.api import get_data
from zvtm.utils.query_data import get_data as get_data_sch
import datetime
logger = logging.getLogger(__name__)
sched = BackgroundScheduler()
from examples.data_runner.em_stock1d_runner import market_map
def stock_zh_a_spot_money_em() -> pd.DataFrame:
    """
    东方财富网-沪深京 A 股-实时行情
    https://quote.eastmoney.com/center/gridlist.html#hs_a_board
    :return: 实时行情
    :rtype: pandas.DataFrame
    """
    url = "https://82.push2.eastmoney.com/api/qt/clist/get"
    params = {
        "pn": "1",
        "pz": "20000",
        "po": "1",
        "np": "2",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2",
        "invt": "2",
        "fid": "f3",
        "fs": "m:0 t:6,m:0 t:80,m:1 t:2,m:1 t:23,m:0 t:81 s:2048",
        "fields": "f2,f3,f8,f12,f13,f14,f62,f64,f69,f70,f75,f76,f81,f82,f87,f124",
        "_": "1623833739532",
    }
    # close    f2
    # change_pct    f3
    # turnover_rate    f8
    # f12: 股票代码    #
    # f13: 市场    #
    # f14: 股票名称
    # net_inflows    f62
    # net_inflow_rate
    # net_main_inflows    f64 + f70
    # net_main_inflow_rate    f69 + f75
    # net_huge_inflows    f64
    # net_huge_inflow_rate    f69
    # net_big_inflows    f70
    # net_big_inflow_rate    f75
    # net_medium_inflows    f76
    # net_medium_inflow_rate    f81
    # net_small_inflows    f82
    # net_small_inflow_rate    f87

    r = requests.get(url, params=params)
    data_json = r.json()
    if not data_json["data"]["diff"]:
        return pd.DataFrame()
    temp_df = pd.DataFrame(data_json["data"]["diff"]).T
    temp_df.columns = [
        "close",
        "change_pct",
        "turnover_rate",
        "code",
        "entity",
        "name",
        "net_inflows",
        "net_huge_inflows",
        "net_huge_inflow_rate",
        "net_big_inflows",
        "net_big_inflow_rate",
        "net_medium_inflows",
        "net_medium_inflow_rate",
        "net_small_inflows",
        "net_small_inflow_rate",
        "timestamp",
    ]
    # temp_df.reset_index(inplace=True)
    # temp_df["index"] = temp_df.index + 1
    # temp_df.rename(columns={"index": "序号"}, inplace=True)
    temp_df = temp_df[
        [
            "close",
            "change_pct",
            "turnover_rate",
            "code",
            "entity",
            "name",
            "net_inflows",
            "net_huge_inflows",
            "net_huge_inflow_rate",
            "net_big_inflows",
            "net_big_inflow_rate",
            "net_medium_inflows",
            "net_medium_inflow_rate",
            "net_small_inflows",
            "net_small_inflow_rate",
            "timestamp",
        ]
    ]
    temp_df["entity"] = temp_df['entity'].apply(
        lambda x: market_map.get(x, f'unknown_{x}')  # 保留未知代码便于调试
        )
    dt = pd.to_datetime(temp_df["timestamp"], unit='s', errors="coerce")[0].replace(hour=0, minute=0, second=0)
    temp_df["timestamp"] = dt
    temp_df["close"] = pd.to_numeric(temp_df["close"], errors="coerce")
    temp_df["change_pct"] = pd.to_numeric(temp_df["change_pct"], errors="coerce")
    temp_df["turnover_rate"] = pd.to_numeric(temp_df["turnover_rate"], errors="coerce")
    temp_df["net_inflows"] = pd.to_numeric(temp_df["net_inflows"], errors="coerce")
    temp_df["net_huge_inflows"] = pd.to_numeric(temp_df["net_huge_inflows"], errors="coerce")
    temp_df["net_huge_inflow_rate"] = pd.to_numeric(temp_df["net_huge_inflow_rate"], errors="coerce")
    temp_df["net_big_inflows"] = pd.to_numeric(temp_df["net_big_inflows"], errors="coerce")
    temp_df["net_big_inflow_rate"] = pd.to_numeric(temp_df["net_big_inflow_rate"], errors="coerce")
    temp_df["net_medium_inflows"] = pd.to_numeric(temp_df["net_medium_inflows"], errors="coerce")
    temp_df["net_medium_inflow_rate"] = pd.to_numeric(temp_df["net_medium_inflow_rate"], errors="coerce")
    temp_df["net_small_inflows"] = pd.to_numeric(temp_df["net_small_inflows"], errors="coerce")
    temp_df["net_small_inflow_rate"] = pd.to_numeric(temp_df["net_small_inflow_rate"], errors="coerce")

    return temp_df

def get_datas():
    df = stock_zh_a_spot_money_em()
    # 使用矢量化操作提升性能
    df['entity_id'] = 'stock_' + df['entity'].astype(str) + '_' + df['code'].astype(str)
    df['id'] = to_time_str(df['entity_id']) + '_' + df['timestamp'].dt.strftime('%Y-%m-%d')

    # for i in range(len(df)):
    #     entity_id = "{}_{}_{}".format('stock', df.loc[i, "entity"], df.loc[i, "code"])
    #     df.loc[i, "entity_id"] = entity_id
    #     df.loc[i, "id"] = "{}_{}".format(to_time_str(entity_id), df.loc[i, "timestamp"].strftime('%Y-%m-%d'))
    provider = 'em'
    force_update = True
    data_schema = StockMoneyFlow1
    # db_engine = get_db_engine(provider, data_schema=data_schema)
    schema_cols = get_schema_columns(data_schema)
    cols = set(df.columns.tolist()) & set(schema_cols)

    df = df[cols]
    df.fillna(0, inplace=True)
    if pd_is_not_null(df):
        df_to_db(df=df, data_schema=data_schema, provider=provider, force_update=force_update)
        # saved = saved + len(df_current)
        # current = get_data(
        #     data_schema=data_schema, columns=[data_schema.id], provider=provider, ids=df["id"].tolist()
        # )
        # if pd_is_not_null(current):
        #     df = df[~df["id"].isin(current["id"])]
        #     df.to_sql(data_schema.__tablename__, db_engine, index=False, if_exists="append")



def record_stock_data(data_provider="em", entity_provider="em"):
    get_datas()
    msg = f"record StockMoneyFlow success,数据来源: em"
    logger.info(msg)
    email_action.send_message(zvt_config["email_username"], msg, msg)

@sched.scheduled_job('cron',day_of_week='mon-fri', hour=15, minute=9)
def isopen():
    dt = datetime.datetime.now().strftime('%Y-%m-%d')
    db = 'tushare'
    sql = "select timestamp from trade_day where timestamp = %s "
    arg = [dt]
    df = get_data_sch(db=db, sql=sql, arg=arg)
    if len(df) > 0:
        record_stock_data()

if __name__ == "__main__":
    init_log("em_moneyflow_runner.log")
    email_action = EmailInformer()
    isopen()
    sched.start()
    sched._thread.join()
