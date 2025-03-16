#!/usr/bin/env python
# -*- coding:utf-8 -*-
# 获取东财的eastmmoney 的历史数据 20250302 发现自20250219之后就没有money信息了

# 先获取money历史信息 给出函数


# 获取所有股票信息


# 获取交易日历信息


# 循环获取股票money信息 筛选出指定日期数据 进行保存


"""
Date: 2023/1/29 17:00
Desc: 东方财富网-数据中心-资金流向
https://data.eastmoney.com/zjlx/detail.html
"""
import json
import time

import pandas as pd
import requests
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

import datetime
logger = logging.getLogger(__name__)
sched = BackgroundScheduler()
# email_action = EmailInformer()

def stock_individual_fund_flow(
    stock: str = "600094", market: str = "sh"
) -> pd.DataFrame:
    """
    东方财富网-数据中心-资金流向-个股
    https://data.eastmoney.com/zjlx/detail.html
    :param stock: 股票代码
    :type stock: str
    :param market: 股票市场; 上海证券交易所: sh, 深证证券交易所: sz, 北京证券交易所: bj;
    :type market: str
    :return: 近期个股的资金流数据
    :rtype: pandas.DataFrame
    """
    market_map = {"sh": 1, "sz": 0, "bj": 0}
    url = "http://push2his.eastmoney.com/api/qt/stock/fflow/daykline/get"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36",
    }
    params = {
        "lmt": "0",
        "klt": "101",
        "secid": f"{market_map[market]}.{stock}",
        "fields1": "f1,f2,f3,f7",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62,f63,f64,f65",
        "ut": "b2884a393a59ad64002292a3e90d46a5",
        # "cb": "jQuery183003743205523325188_1589197499471",
        "_": int(time.time() * 1000),
    }
    r = requests.get(url, params=params, headers=headers)
    json_data = r.json()
    content_list = json_data["data"]["klines"]
    temp_df = pd.DataFrame([item.split(",") for item in content_list])
    temp_df.columns = [
        "日期",
        "主力净流入-净额",
        "小单净流入-净额",
        "中单净流入-净额",
        "大单净流入-净额",
        "超大单净流入-净额",
        "主力净流入-净占比",
        "小单净流入-净占比",
        "中单净流入-净占比",
        "大单净流入-净占比",
        "超大单净流入-净占比",
        "收盘价",
        "涨跌幅",
        "-",
        "-",
    ]
    temp_df = temp_df[
        [
            "日期",
            "收盘价",
            "涨跌幅",
            "主力净流入-净额",
            "主力净流入-净占比",
            "超大单净流入-净额",
            "超大单净流入-净占比",
            "大单净流入-净额",
            "大单净流入-净占比",
            "中单净流入-净额",
            "中单净流入-净占比",
            "小单净流入-净额",
            "小单净流入-净占比",
        ]
    ]
    temp_df["主力净流入-净额"] = pd.to_numeric(temp_df["主力净流入-净额"])
    temp_df["小单净流入-净额"] = pd.to_numeric(temp_df["小单净流入-净额"])
    temp_df["中单净流入-净额"] = pd.to_numeric(temp_df["中单净流入-净额"])
    temp_df["大单净流入-净额"] = pd.to_numeric(temp_df["大单净流入-净额"])
    temp_df["超大单净流入-净额"] = pd.to_numeric(temp_df["超大单净流入-净额"])
    temp_df["主力净流入-净占比"] = pd.to_numeric(temp_df["主力净流入-净占比"])
    temp_df["小单净流入-净占比"] = pd.to_numeric(temp_df["小单净流入-净占比"])
    temp_df["中单净流入-净占比"] = pd.to_numeric(temp_df["中单净流入-净占比"])
    temp_df["大单净流入-净占比"] = pd.to_numeric(temp_df["大单净流入-净占比"])
    temp_df["超大单净流入-净占比"] = pd.to_numeric(temp_df["超大单净流入-净占比"])
    temp_df["收盘价"] = pd.to_numeric(temp_df["收盘价"])
    temp_df["涨跌幅"] = pd.to_numeric(temp_df["涨跌幅"])
    return temp_df

# stock_individual_fund_flow_df = stock_individual_fund_flow(
#     stock="300532", market="sz"
# )
# print(stock_individual_fund_flow_df)
def process_fund_flow_data(raw_df, stock_code):
    column_mapping = {
        "日期": "timestamp",
        "收盘价": "close",
        "涨跌幅": "change_pct",
        "主力净流入-净额": "net_main_inflows",
        "主力净流入-净占比": "net_main_inflow_rate",
        "超大单净流入-净额": "net_huge_inflows",
        "超大单净流入-净占比": "net_huge_inflow_rate",
        "大单净流入-净额": "net_big_inflows",
        "大单净流入-净占比": "net_big_inflow_rate",
        "中单净流入-净额": "net_medium_inflows",
        "中单净流入-净占比": "net_medium_inflow_rate",
        "小单净流入-净额": "net_small_inflows",
        "小单净流入-净占比": "net_small_inflow_rate"
    }

    processed_df = (
        raw_df
            .assign(code=lambda x: str(stock_code), name=lambda x: None)
            .rename(columns=column_mapping)
            .pipe(lambda df: df.assign(
            timestamp=pd.to_datetime(df['timestamp'], format='%Y-%m-%d', errors='coerce')
        ))
            .astype({
            'code': 'str',
            'name': 'object',
            'close': 'float',
            'change_pct': 'float',
            'net_main_inflows': 'float',
            'net_main_inflow_rate': 'float',
            'net_huge_inflows': 'float',
            'net_huge_inflow_rate': 'float',
            'net_big_inflows': 'float',
            'net_big_inflow_rate': 'float',
            'net_medium_inflows': 'float',
            'net_medium_inflow_rate': 'float',
            'net_small_inflows': 'float',
            'net_small_inflow_rate': 'float'
        })
            # 增加异常数据过滤
            .dropna(subset=['timestamp'])
            .reindex(columns=[
            'code', 'name', 'timestamp', 'close', 'change_pct',
            'net_main_inflows', 'net_main_inflow_rate',
            'net_huge_inflows', 'net_huge_inflow_rate',
            'net_big_inflows', 'net_big_inflow_rate',
            'net_medium_inflows', 'net_medium_inflow_rate',
            'net_small_inflows', 'net_small_inflow_rate'
        ])
    )

    if 'net_inflow_rate' not in processed_df.columns:
        processed_df['net_inflow_rate'] = processed_df['net_main_inflow_rate']

    return processed_df


# 使用示例
# processed_df = process_fund_flow_data(stock_individual_fund_flow_df, "300532")
# print(processed_df)




def get_datas(stock_codes,stock_df):
    """
    批量处理并保存多只股票资金流数据
    :param stock_codes: 股票代码列表，如 ["300532", "600000"]
    :paranm stock_df: 股票数据列表
    """
    provider = 'em'
    force_update = True
    data_schema = StockMoneyFlow1
    # db_engine = get_db_engine(provider, data_schema=data_schema)

    for code in stock_codes:
        try:
            # 获取股票基本信息
            stock_info = stock_df[stock_df['code'].astype(str) == code]
            if stock_info.empty:
                logger.error(f"股票 {code} 在codename.txt中不存在")
                continue
            stock_name = stock_info.iloc[0].get('name')
            exchange = stock_info.iloc[0].get('exchange')
            if not exchange:
                logger.error(f"股票 {code} 缺少交易所信息")
                continue

            # 获取原始数据
            raw_df = stock_individual_fund_flow(stock=code, market=exchange)

            # 处理数据
            processed_df = process_fund_flow_data(raw_df, code)

            # 添加系统字段
            processed_df['entity_id'] = 'stock_' + exchange + "_" + str(code)

            processed_df['id'] = processed_df['entity_id'] + '_' + processed_df['timestamp'].dt.strftime('%Y-%m-%d')
            processed_df['name'] = stock_name  # 添加股票名称
            # processed_df['created_timestamp'] = datetime.now()

            # 字段对齐
            schema_cols = get_schema_columns(data_schema)
            df_to_save = processed_df.reindex(columns=schema_cols)

            # 分股票保存
            if pd_is_not_null(df_to_save):
                df_to_db(
                    df=df_to_save,
                    data_schema=data_schema,
                    provider=provider,
                    force_update=force_update,
                    sub_size=1000
                )
                logger.info(f"成功保存股票 {code} {len(df_to_save)} 条新资金流数据")

        except Exception as e:
            logger.error(f"处理股票 {code} 时发生异常：{str(e)}")



def record_stock_data(data_provider="em", entity_provider="em"):
    # 获取全量股票代码
    import pandas as pd

    stock_df = pd.read_csv(r'd:\codename.txt', sep='_', header=None, names=['exchange', 'code', 'name'],dtype={'code': str} )
    # print(stock_df)
    stock_codes = stock_df['code'].unique().tolist()
    # stock_codes = ['600001']
    # 分批处理（每批50只）
    batch_size = 50
    for i in range(0, len(stock_codes), batch_size):
        batch_codes = stock_codes[i:i + batch_size]
        get_datas(batch_codes,stock_df)

    msg = f"成功记录 {len(stock_codes)} 只股票资金流数据，数据来源: em"
    logger.info(msg)
    email_action.send_message(zvt_config["email_username"], msg, msg)





if __name__ == "__main__":
    init_log("em_moneyflow_hist_runner.log")
    email_action = EmailInformer()
    record_stock_data()
    sched.start()
    sched._thread.join()

