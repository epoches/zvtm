# -*- coding: utf-8 -*-
import logging
from apscheduler.schedulers.background import BackgroundScheduler
from zvtm import init_log
import requests
import pandas as pd
from zvtm.utils.time_utils import now_pd_timestamp, to_time_str, to_pd_timestamp
from zvtm.utils.pd_utils import pd_is_not_null
from zvtm.contract.api import get_db_engine,get_schema_columns
from zvtm.domain import Stock1dHfqKdata
from zvtm.contract.api import df_to_db
import datetime
from zvtm.informer import EmailInformer
from zvtm import zvt_config


logger = logging.getLogger(__name__)
sched = BackgroundScheduler()


# @lru_cache()
def code_id_map_em() -> dict:
    """
    东方财富-股票和市场代码
    https://quote.eastmoney.com/center/gridlist.html#hs_a_board
    :return: 股票和市场代码
    :rtype: dict
    """
    url = "https://80.push2.eastmoney.com/api/qt/clist/get"
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
    r = requests.get(url, timeout=15, params=params)
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
    r = requests.get(url, timeout=15, params=params)
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
    r = requests.get(url, timeout=15, params=params)
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
    start_date: str = "19700101",
    end_date: str = "20500101",
    adjust: str = "",
    timeout: float = None,
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
    :param timeout: choice of None or a positive float number
    :type timeout: float
    :return: 每日行情
    :rtype: pandas.DataFrame
    """
    code_id_dict = code_id_map_em()
    adjust_dict = {"qfq": "1", "hfq": "2", "": "0"}
    period_dict = {"daily": "101", "weekly": "102", "monthly": "103"}
    url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
    params = {
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f116",
        "ut": "7eea3edcaed734bea9cbfc24409ed989",
        "klt": period_dict[period],
        "fqt": adjust_dict[adjust],
        "secid": f"{code_id_dict[symbol]}.{symbol}",
        "beg": start_date,
        "end": end_date,
        "_": "1623766962675",
    }
    r = requests.get(url, params=params, timeout=timeout)
    data_json = r.json()
    if not (data_json["data"] and data_json["data"]["klines"]):
        return pd.DataFrame()
    temp_df = pd.DataFrame([item.split(",") for item in data_json["data"]["klines"]])
    temp_df["股票代码"] = symbol
    temp_df["entity"] = data_json['data']["market"]
    temp_df["名称"] = data_json['data']["name"]
    temp_df.columns = [
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
        "换手率",
        "股票代码",
        "entity",
        "名称",
    ]
    temp_df["日期"] = pd.to_datetime(temp_df["日期"], errors="coerce").dt.date
    temp_df["开盘"] = pd.to_numeric(temp_df["开盘"], errors="coerce")
    temp_df["收盘"] = pd.to_numeric(temp_df["收盘"], errors="coerce")
    temp_df["最高"] = pd.to_numeric(temp_df["最高"], errors="coerce")
    temp_df["最低"] = pd.to_numeric(temp_df["最低"], errors="coerce")
    temp_df["成交量"] = pd.to_numeric(temp_df["成交量"], errors="coerce")
    temp_df["成交额"] = pd.to_numeric(temp_df["成交额"], errors="coerce")
    temp_df["振幅"] = pd.to_numeric(temp_df["振幅"], errors="coerce")
    temp_df["涨跌幅"] = pd.to_numeric(temp_df["涨跌幅"], errors="coerce")
    temp_df["涨跌额"] = pd.to_numeric(temp_df["涨跌额"], errors="coerce")
    temp_df["换手率"] = pd.to_numeric(temp_df["换手率"], errors="coerce")
    temp_df["entity"] = temp_df["entity"].apply(lambda x: 'sz' if x == 0 else 'sh')
    temp_df = temp_df[
        [
            "日期",
            "股票代码",
            "entity",
            "名称",
            "开盘",
            "收盘",
            "最高",
            "最低",
            "成交量",
            "成交额",
            "振幅",
            "涨跌幅",
            "涨跌额",
            "换手率",
        ]
    ]
    return temp_df

from bs4 import BeautifulSoup
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/114.0.0.0 Safari/537.36"
}
def stock_a_indicator_lg(symbol: str = "000001") -> pd.DataFrame:
    """
    市盈率, 市净率, 股息率数据接口
    https://legulegu.com/stocklist
    :param symbol: 通过 ak.stock_a_indicator_lg(symbol="all") 来获取所有股票的代码
    :type symbol: str
    :return: 市盈率, 市净率, 股息率查询
    :rtype: pandas.DataFrame
    """
    if symbol == "all":
        url = "https://legulegu.com/stocklist"
        r = requests.get(url, headers=headers)
        soup = BeautifulSoup(r.text, features="lxml")
        node_list = soup.find_all(attrs={"class": "col-xs-6"})
        href_list = [item.find("a")["href"] for item in node_list]
        title_list = [item.find("a")["title"] for item in node_list]
        temp_df = pd.DataFrame([title_list, href_list]).T
        temp_df.columns = ["stock_name", "short_url"]
        temp_df["code"] = temp_df["short_url"].str.split("/", expand=True).iloc[:, -1]
        del temp_df["short_url"]
        temp_df = temp_df[["code", "stock_name"]]
        return temp_df
    else:
        url = "https://legulegu.com/api/s/base-info/"
        token = get_token_lg()
        params = {"token": token, "id": symbol}
        r = requests.post(
            url,
            params=params,
            **get_cookie_csrf(url="https://legulegu.com/"),
        )
        temp_json = r.json()
        temp_df = pd.DataFrame(
            temp_json["data"]["items"],
            columns=temp_json["data"]["fields"],
        )
        temp_df["trade_date"] = pd.to_datetime(temp_df["trade_date"]).dt.date
        temp_df[temp_df.columns[1:]] = temp_df[temp_df.columns[1:]].astype(float)
        temp_df.sort_values(by=["trade_date"], inplace=True, ignore_index=True)
        if len(set(temp_df["trade_date"])) <= 0:
            raise ValueError("数据获取失败, 请检查是否输入正确的股票代码")
        return temp_df



def record_stock_data():
    email_action = EmailInformer()
    msg = f"record stock1d success,数据来源: em"
    try:
        # 获取股票代码
        stock_a_indicator_lg_all_df = stock_a_indicator_lg(symbol="all")
        # print(stock_a_indicator_lg_all_df)
       #  df = pd.DataFrame(columns=['日期', '股票代码', 'entity', '名称', '开盘', '收盘', '最高', '最低', '成交量', '成交额',
       # '振幅', '涨跌幅', '涨跌额', '换手率'])
        list_of_df = []
        dt = datetime.datetime.now().strftime('%Y%m%d')
        for code in stock_a_indicator_lg_all_df['code']:
            df_code = stock_zh_a_hist(
                symbol=code,
                period="daily",
                start_date=dt,
                end_date=dt,
                adjust="hfq",
            )
            list_of_df.append(df_code)
        df = pd.concat(list_of_df, ignore_index=True)
        # concat数据
        force_update=True
        data_schema = Stock1dHfqKdata
        provider ='em'
        db_engine = get_db_engine(provider, data_schema=data_schema)
        schema_cols = get_schema_columns(data_schema)
        cols = set(df.columns.tolist()) & set(schema_cols)
        df.rename(columns={"日期": "timestamp"}, inplace=True)
        df.rename(columns={"股票代码": "code"}, inplace=True)
        df.rename(columns={"名称": "name"}, inplace=True)
        df.rename(columns={"收盘": "close"}, inplace=True)
        df.rename(columns={"最高": "high"}, inplace=True)
        df.rename(columns={"开盘": "open"}, inplace=True)
        df.rename(columns={"最低": "low"}, inplace=True)
        df.rename(columns={"成交量": "volume"}, inplace=True)
        df.rename(columns={"成交额": "turnover"}, inplace=True)
        df.rename(columns={"涨跌幅": "change_pct"}, inplace=True)
        df.rename(columns={"换手率": "turnover_rate"}, inplace=True)

        # dt = datetime.datetime.now().strftime('%Y-%m-%d')
        for i in range(len(df)):
            entity_id = "{}_{}_{}".format('stock',df.loc[i,"entity"],df.loc[i,"code"])
            df.loc[i,"entity_id"] = entity_id
            df.loc[i,"id"] = "{}_{}".format(to_time_str(entity_id),df.loc[i,"timestamp"])
            df.loc[i,"timestamp"] = df.loc[i,"timestamp"]
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
from schedule.utils.query_data import get_data
@sched.scheduled_job('cron',day_of_week='mon-fri', hour=15, minute=5)
def isopen():
    dt = datetime.datetime.now().strftime('%Y-%m-%d')
    db = 'tushare'
    sql = "select timestamp from trade_day where timestamp = %s "
    arg = [dt]
    df = get_data(db=db, sql=sql, arg=arg)
    if len(df) > 0:
        record_stock_data()

if __name__ == "__main__":
    init_log("em_kdatahfq1d_runner.log")

    isopen()

    sched.start()

    sched._thread.join()



    # stock_zh_a_hist_df = stock_zh_a_hist(
    #     symbol="000001",
    #     period="daily",
    #     start_date="20240717",
    #     end_date="20240717",
    #     adjust="hfq",
    # )
    # print(stock_zh_a_hist_df)
