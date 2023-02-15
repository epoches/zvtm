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

def code_id_map_em() -> dict:
    """
    东方财富-股票和市场代码
    http://quote.eastmoney.com/center/gridlist.html#hs_a_board
    :return: 股票和市场代码
    :rtype: dict
    """
    url = "http://80.push2.eastmoney.com/api/qt/clist/get"
    params = {
        "pn": "1",
        "pz": "50000",
        "po": "1",
        "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2",
        "invt": "2",
        "fid": "f3",
        "fs": "m:1 t:2,m:1 t:23",
        "fields": "f12",
        "_": "1623833739532",
    }
    r = requests.get(url, params=params)
    data_json = r.json()
    if not data_json["data"]["diff"]:
        return dict()
    temp_df = pd.DataFrame(data_json["data"]["diff"])
    temp_df["market_id"] = 1
    temp_df.columns = ["sh_code", "sh_id"]
    code_id_dict = dict(zip(temp_df["sh_code"], temp_df["sh_id"]))
    params = {
        "pn": "1",
        "pz": "50000",
        "po": "1",
        "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2",
        "invt": "2",
        "fid": "f3",
        "fs": "m:0 t:6,m:0 t:80",
        "fields": "f12",
        "_": "1623833739532",
    }
    r = requests.get(url, params=params)
    data_json = r.json()
    if not data_json["data"]["diff"]:
        return dict()
    temp_df_sz = pd.DataFrame(data_json["data"]["diff"])
    temp_df_sz["sz_id"] = 0
    code_id_dict.update(dict(zip(temp_df_sz["f12"], temp_df_sz["sz_id"])))
    params = {
        "pn": "1",
        "pz": "50000",
        "po": "1",
        "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2",
        "invt": "2",
        "fid": "f3",
        "fs": "m:0 t:81 s:2048",
        "fields": "f12",
        "_": "1623833739532",
    }
    r = requests.get(url, params=params)
    data_json = r.json()
    if not data_json["data"]["diff"]:
        return dict()
    temp_df_sz = pd.DataFrame(data_json["data"]["diff"])
    temp_df_sz["bj_id"] = 0
    code_id_dict.update(dict(zip(temp_df_sz["f12"], temp_df_sz["bj_id"])))
    return code_id_dict



def stock_zh_a_hist(
    symbol: str = "000001",
    period: str = "daily",
    start_date: str = "20230210",
    end_date: str = "20500101",
    adjust: str = "",
) -> pd.DataFrame:
    """
    东方财富网-行情首页-沪深京 A 股-每日行情
    https://quote.eastmoney.com/concept/sh603777.html?from=classic
    :param symbol: 股票代码
    :type symbol: str
    :param period: choice of {'daily', 'weekly', 'monthly'}
    :type period: str
    :param start_date: 开始日期
    :type start_date: str
    :param end_date: 结束日期
    :type end_date: str
    :param adjust: choice of {"qfq": "前复权", "hfq": "后复权", "": "不复权"}
    :type adjust: str
    :return: 每日行情
    :rtype: pandas.DataFrame
    """
    code_id_dict = code_id_map_em()
    adjust_dict = {"qfq": "1", "hfq": "2", "": "0"}
    period_dict = {"daily": "101", "weekly": "102", "monthly": "103"}
    url = "http://push2his.eastmoney.com/api/qt/stock/kline/get"

    params = {
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f9,f10,f20,f21,f23,f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f114,f115,f116,f130,f131",
        "ut": "7eea3edcaed734bea9cbfc24409ed989",
        "klt": period_dict[period],
        "fqt": adjust_dict[adjust],
        "secid": f"{code_id_dict[symbol]}.{symbol}",
        "beg": start_date,
        "end": end_date,
        "_": "1623766962675",
    }
    r = requests.get(url, params=params)
    data_json = r.json()
    if not (data_json["data"] and data_json["data"]["klines"]):
        return pd.DataFrame()
    temp_df = pd.DataFrame(
        [item.split(",") for item in data_json["data"]["klines"]]
    )
    temp_df.columns = [
        "pe_ttm1",
        "lb",
        "market_cap",
        "circulating_market_cap",
        "pb",
        "日期",
        "开盘",
        "收盘",
        "最高",
        "最低",
        "成交量",
        "成交额",
        "振幅",
        "涨跌幅",
        "涨跌额",
        "pe",
        "pe_ttm",
        "换手率",
        "ps",
        "pcf",
    ]
    temp_df.index = pd.to_datetime(temp_df["日期"])
    temp_df.reset_index(inplace=True, drop=True)
    temp_df["lb"] = pd.to_numeric(temp_df["lb"], errors="coerce")
    temp_df["pb"] = pd.to_numeric(temp_df["pb"], errors="coerce")
    temp_df["pe"] = pd.to_numeric(temp_df["pe"], errors="coerce")
    temp_df["pe_ttm1"] = pd.to_numeric(temp_df["pe_ttm1"], errors="coerce")
    temp_df["ps"] = pd.to_numeric(temp_df["ps"], errors="coerce")
    temp_df["pcf"] = pd.to_numeric(temp_df["pcf"], errors="coerce")
    temp_df["pe_ttm"] = pd.to_numeric(temp_df["pe_ttm"], errors="coerce")
    temp_df["market_cap"] = pd.to_numeric(temp_df["market_cap"], errors="coerce")
    temp_df["circulating_market_cap"] = pd.to_numeric(temp_df["circulating_market_cap"], errors="coerce")
    temp_df["开盘"] = pd.to_numeric(temp_df["开盘"])
    temp_df["收盘"] = pd.to_numeric(temp_df["收盘"])
    temp_df["最高"] = pd.to_numeric(temp_df["最高"])
    temp_df["最低"] = pd.to_numeric(temp_df["最低"])
    temp_df["成交量"] = pd.to_numeric(temp_df["成交量"])
    temp_df["成交额"] = pd.to_numeric(temp_df["成交额"])
    temp_df["振幅"] = pd.to_numeric(temp_df["振幅"])
    temp_df["涨跌幅"] = pd.to_numeric(temp_df["涨跌幅"])
    temp_df["涨跌额"] = pd.to_numeric(temp_df["涨跌额"])
    temp_df["换手率"] = pd.to_numeric(temp_df["换手率"])
    temp_df["code"] = data_json["data"]["code"]
    temp_df["entity"] = data_json["data"]["market"]
    temp_df["name"] = data_json["data"]["name"]
    temp_df["entity"] = temp_df["entity"].apply(lambda x: 'sz' if x == 0 else 'sh')
    # f9	市盈率 动 总市值/预估全年净利润
    # f10	量比
    # f20	总市值
    # f21	流通市值
    # f23	市净率
    # f130	市销率TTM
    # f131	市现率TTM
    # f114	市盈率（静） 总市值/上年度净利润
    # f115	市盈率（TTM）

    return temp_df


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
    df = stock_zh_a_hist()
    print(df)


get_datas()