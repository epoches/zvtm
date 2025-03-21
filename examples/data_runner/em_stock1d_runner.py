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
import sys
sys.path.insert(0, sys.path[0]+"/../")
sys.path.insert(0, sys.path[0]+"/../../")
sys.path.insert(0, sys.path[0]+"/../../../")
import os
current_script_path = os.path.abspath(__file__)
# 向上回退 3 层目录，定位到项目根目录（根据实际情况调整层数）
project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_script_path)))
# 将项目根目录添加到 sys.path
sys.path.insert(0, project_root)
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
from zvtm.utils.query_data import get_data

logger = logging.getLogger(__name__)
sched = BackgroundScheduler()

market_map = {
    0: 'sz',  # 深市
    1: 'sh',  # 沪市
    2: 'bj',  # 北交所
    3: 'sh',  # 科创板归沪市
    5: 'sz'   # 创业板归深市
}
#
# def stock_zh_a_spot_em() -> pd.DataFrame:
#     """
#     东方财富网-沪深 A 股-实时行情
#     https://quote.eastmoney.com/center/gridlist.html#hs_a_board
#     :return: 实时行情
#     :rtype: pandas.DataFrame
#     """
#     url = "https://82.push2.eastmoney.com/api/qt/clist/get"
#     params = {
#         "pn": "1",
#         "pz": "20000",
#         "po": "1",
#         "np": "2",
#         "ut": "bd1d9ddb04089700cf9c27f6f7426281",
#         "fltt": "2",
#         "invt": "2",
#         "fid": "f3",
#         "fs": "m:0 t:6,m:0 t:80,m:1 t:2,m:1 t:23,m:0 t:81 s:2048",
#         "fields": "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,"
#         "f20,f21,f23,f24,f25,f22,f11,f62,f128,f136,f115,f152",
#         "_": "1623833739532",
#     }
#     r = requests.get(url, params=params, timeout=15)
#     data_json = r.json()
#     if not data_json["data"]["diff"]:
#         return pd.DataFrame()
#     temp_df = pd.DataFrame(data_json["data"]["diff"]).T
#     temp_df.columns = [
#         "_",
#         "最新价",
#         "涨跌幅",
#         "涨跌额",
#         "成交量",
#         "成交额",
#         "振幅",
#         "换手率",
#         "市盈率-动态",
#         "量比",
#         "5分钟涨跌",
#         "代码",
#         "entity",
#         "名称",
#         "最高",
#         "最低",
#         "今开",
#         "昨收",
#         "总市值",
#         "流通市值",
#         "涨速",
#         "市净率",
#         "60日涨跌幅",
#         "年初至今涨跌幅",
#         "-",
#         "-",
#         "-",
#         "-",
#         "-",
#         "-",
#         "-",
#     ]
#     # temp_df = pd.DataFrame(data_json["data"]["diff"])
#     # temp_df.columns = [
#     #     "_",
#     #     "最新价",
#     #     "涨跌幅",
#     #     "涨跌额",
#     #     "成交量",
#     #     "成交额",
#     #     "振幅",
#     #     "换手率",
#     #     "市盈率-动态",
#     #     "量比",
#     #     "5分钟涨跌",
#     #     "代码",
#     #     "entity",
#     #     "名称",
#     #     "最高",
#     #     "最低",
#     #     "今开",
#     #     "昨收",
#     #     "总市值",
#     #     "流通市值",
#     #     "涨速",
#     #     "市净率",
#     #     "60日涨跌幅",
#     #     "年初至今涨跌幅",
#     #     "-",
#     #     "-",
#     #     "-",
#     #     "-",
#     #     "-",
#     #     "-",
#     #     "-",
#     # ]
#     temp_df.reset_index(inplace=True)
#     temp_df["index"] = temp_df.index + 1
#     temp_df.rename(columns={"index": "序号"}, inplace=True)
#     temp_df = temp_df[
#         [
#             "序号",
#             "代码",
#             "名称",
#             "最新价",
#             "涨跌幅",
#             "涨跌额",
#             "成交量",
#             "成交额",
#             "振幅",
#             "最高",
#             "最低",
#             "今开",
#             "昨收",
#             "量比",
#             "换手率",
#             "市盈率-动态",
#             "市净率",
#             "总市值",
#             "流通市值",
#             "涨速",
#             "5分钟涨跌",
#             "60日涨跌幅",
#             "年初至今涨跌幅",
#             "entity",
#         ]
#     ]
#     temp_df["最新价"] = pd.to_numeric(temp_df["最新价"], errors="coerce")
#     temp_df["涨跌幅"] = pd.to_numeric(temp_df["涨跌幅"], errors="coerce")/100
#     temp_df["涨跌额"] = pd.to_numeric(temp_df["涨跌额"], errors="coerce")
#     temp_df["成交量"] = pd.to_numeric(temp_df["成交量"], errors="coerce")
#     temp_df["成交额"] = pd.to_numeric(temp_df["成交额"], errors="coerce")
#     temp_df["振幅"] = pd.to_numeric(temp_df["振幅"], errors="coerce")
#     temp_df["最高"] = pd.to_numeric(temp_df["最高"], errors="coerce")
#     temp_df["最低"] = pd.to_numeric(temp_df["最低"], errors="coerce")
#     temp_df["今开"] = pd.to_numeric(temp_df["今开"], errors="coerce")
#     temp_df["昨收"] = pd.to_numeric(temp_df["昨收"], errors="coerce")
#     temp_df["量比"] = pd.to_numeric(temp_df["量比"], errors="coerce")
#     temp_df["换手率"] = pd.to_numeric(temp_df["换手率"], errors="coerce")/100
#     temp_df["市盈率-动态"] = pd.to_numeric(temp_df["市盈率-动态"], errors="coerce")
#     temp_df["市净率"] = pd.to_numeric(temp_df["市净率"], errors="coerce")
#     temp_df["总市值"] = pd.to_numeric(temp_df["总市值"], errors="coerce")
#     temp_df["流通市值"] = pd.to_numeric(temp_df["流通市值"], errors="coerce")
#     temp_df["涨速"] = pd.to_numeric(temp_df["涨速"], errors="coerce")
#     temp_df["5分钟涨跌"] = pd.to_numeric(temp_df["5分钟涨跌"], errors="coerce")
#     temp_df["60日涨跌幅"] = pd.to_numeric(temp_df["60日涨跌幅"], errors="coerce")
#     temp_df["年初至今涨跌幅"] = pd.to_numeric(temp_df["年初至今涨跌幅"], errors="coerce")
#     temp_df["entity"] = temp_df['entity'].apply(
#         lambda x: market_map.get(x, f'unknown_{x}')  # 保留未知代码便于调试
#         )
#     return temp_df
print("Project Root:", project_root)
print("Current sys.path:", sys.path)
from zvtm.ak.utils.func import fetch_paginated_data
def stock_zh_a_spot_em() -> pd.DataFrame:
    """
    东方财富网-沪深京 A 股-实时行情
    https://quote.eastmoney.com/center/gridlist.html#hs_a_board
    :return: 实时行情
    :rtype: pandas.DataFrame
    """
    url = "https://82.push2.eastmoney.com/api/qt/clist/get"
    params = {
        "pn": "1",
        "pz": "100",
        "po": "1",
        "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2",
        "invt": "2",
        "fid": "f12",
        "fs": "m:0 t:6,m:0 t:80,m:1 t:2,m:1 t:23,m:0 t:81 s:2048",
        "fields": "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,"
        "f20,f21,f23,f24,f25,f22,f11,f62,f128,f136,f115,f152",
        "_": "1623833739532",
    }
    temp_df = fetch_paginated_data(url, params)
    temp_df.columns = [
        "index",
        "_",
        "最新价",
        "涨跌幅",
        "涨跌额",
        "成交量",
        "成交额",
        "振幅",
        "换手率",
        "市盈率-动态",
        "量比",
        "5分钟涨跌",
        "代码",
        "entity",
        "名称",
        "最高",
        "最低",
        "今开",
        "昨收",
        "总市值",
        "流通市值",
        "涨速",
        "市净率",
        "60日涨跌幅",
        "年初至今涨跌幅",
        "-",
        "-",
        "-",
        "-",
        "-",
        "-",
        "-",
    ]
    temp_df.rename(columns={"index": "序号"}, inplace=True)
    temp_df = temp_df[
        [
            "序号",
            "代码",
            "名称",
            "最新价",
            "涨跌幅",
            "涨跌额",
            "成交量",
            "成交额",
            "振幅",
            "最高",
            "最低",
            "今开",
            "昨收",
            "量比",
            "换手率",
            "市盈率-动态",
            "市净率",
            "总市值",
            "流通市值",
            "涨速",
            "5分钟涨跌",
            "60日涨跌幅",
            "年初至今涨跌幅",
            "entity",
        ]
    ]
    temp_df["最新价"] = pd.to_numeric(temp_df["最新价"], errors="coerce")
    temp_df["涨跌幅"] = pd.to_numeric(temp_df["涨跌幅"], errors="coerce")
    temp_df["涨跌额"] = pd.to_numeric(temp_df["涨跌额"], errors="coerce")
    temp_df["成交量"] = pd.to_numeric(temp_df["成交量"], errors="coerce")
    temp_df["成交额"] = pd.to_numeric(temp_df["成交额"], errors="coerce")
    temp_df["振幅"] = pd.to_numeric(temp_df["振幅"], errors="coerce")
    temp_df["最高"] = pd.to_numeric(temp_df["最高"], errors="coerce")
    temp_df["最低"] = pd.to_numeric(temp_df["最低"], errors="coerce")
    temp_df["今开"] = pd.to_numeric(temp_df["今开"], errors="coerce")
    temp_df["昨收"] = pd.to_numeric(temp_df["昨收"], errors="coerce")
    temp_df["量比"] = pd.to_numeric(temp_df["量比"], errors="coerce")
    temp_df["换手率"] = pd.to_numeric(temp_df["换手率"], errors="coerce")
    temp_df["市盈率-动态"] = pd.to_numeric(temp_df["市盈率-动态"], errors="coerce")
    temp_df["市净率"] = pd.to_numeric(temp_df["市净率"], errors="coerce")
    temp_df["总市值"] = pd.to_numeric(temp_df["总市值"], errors="coerce")
    temp_df["流通市值"] = pd.to_numeric(temp_df["流通市值"], errors="coerce")
    temp_df["涨速"] = pd.to_numeric(temp_df["涨速"], errors="coerce")
    temp_df["5分钟涨跌"] = pd.to_numeric(temp_df["5分钟涨跌"], errors="coerce")
    temp_df["60日涨跌幅"] = pd.to_numeric(temp_df["60日涨跌幅"], errors="coerce")
    temp_df["年初至今涨跌幅"] = pd.to_numeric(
        temp_df["年初至今涨跌幅"], errors="coerce"
    )
    temp_df["entity"] = temp_df['entity'].apply(
        lambda x: market_map.get(x, f'unknown_{x}')  # 保留未知代码便于调试
        )
    return temp_df




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
            df_to_db(df=df, data_schema=data_schema, provider=provider, force_update=force_update)
        logger.info(msg)
        email_action.send_message(zvt_config["email_username"], msg, msg)
    except Exception as e:
        logger.exception("report error:{}".format(e))
        email_action.send_message(
            zvt_config["email_username"],
            f"record stock1d error",
            f"record stock1d error: {e}",
        )

@sched.scheduled_job('cron',day_of_week='mon-fri', hour=15, minute=5)
def isopen():
    # dt = '2024-11-23'
    dt = datetime.datetime.now().strftime('%Y-%m-%d')
    db = 'tushare'
    sql = "select timestamp from trade_day where timestamp = %s "
    arg = [dt]
    df = get_data(db=db, sql=sql, arg=arg)
    if len(df) > 0:
        record_stock_data()
    # print(df)
if __name__ == "__main__":
    init_log("em_kdata1d_runner.log")
    isopen()
    # record_stock_data()

    sched.start()

    sched._thread.join()

