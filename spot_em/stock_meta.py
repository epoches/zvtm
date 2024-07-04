# -*- coding:utf-8 -*-
# 定期执行 确保退市股票都被剔除
"""
Date: 2023/2/19 19:00
Desc: 股票基本信息
"""
import json
import warnings
from io import BytesIO
from functools import lru_cache
import requests

import pandas as pd

def stock_info_sz_delist(symbol: str = "暂停上市公司") -> pd.DataFrame:
    """
    深证证券交易所-暂停上市公司-终止上市公司
    http://www.szse.cn/market/stock/suspend/index.html
    :param symbol: choice of {"暂停上市公司", "终止上市公司"}
    :type symbol: str
    :return: 暂停上市公司 or 终止上市公司 的数据
    :rtype: pandas.DataFrame
    """
    indicator_map = {"暂停上市公司": "tab1", "终止上市公司": "tab2"}
    url = "http://www.szse.cn/api/report/ShowReport"
    params = {
        "SHOWTYPE": "xlsx",
        "CATALOGID": "1793_ssgs",
        "TABKEY": indicator_map[symbol],
        "random": "0.6935816432433362",
    }
    r = requests.get(url, params=params)
    with warnings.catch_warnings(record=True):
        warnings.simplefilter("always")
        temp_df = pd.read_excel(BytesIO(r.content))
        if temp_df.empty:
            return pd.DataFrame()
        temp_df["证券代码"] = temp_df["证券代码"].astype("str").str.zfill(6)
        temp_df['上市日期'] = pd.to_datetime(temp_df['上市日期']).dt.date
        temp_df['终止上市日期'] = pd.to_datetime(temp_df['终止上市日期']).dt.date
        return temp_df

from schedule.utils.query_data import get_data,update_data
def update_stock(code,end_date):
    db = 'em_stock_meta'
    sql = 'UPDATE stock set end_date=%s where code = %s '
    arg = [end_date,code]
    update_data(db=db, sql=sql, arg=arg)


stock_info_sz_delist_df = stock_info_sz_delist(symbol="终止上市公司")
print(stock_info_sz_delist_df)
for i in range(len(stock_info_sz_delist_df)):
    update_stock(stock_info_sz_delist_df['证券代码'].iloc[i],stock_info_sz_delist_df['终止上市日期'].iloc[i].strftime('%Y-%m-%d'))