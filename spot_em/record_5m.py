# -*- coding: utf-8 -*-
import logging
import random
from typing import Union
from decimal import *
import demjson3
import pandas as pd
import requests

logger = logging.getLogger(__name__)

def chrome_copy_header_to_dict(src):
    lines = src.split("\n")
    header = {}
    if lines:
        for line in lines:
            try:
                index = line.index(":")
                key = line[:index]
                value = line[index + 1 :]
                if key and value:
                    header.setdefault(key.strip(), value.strip())
            except Exception:
                pass
    return header


DEFAULT_HEADER = chrome_copy_header_to_dict(
    """
User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36
"""
)
def to_pd_timestamp(the_time) -> pd.Timestamp:
    if the_time is None:
        return None
    if type(the_time) == int:
        return pd.Timestamp.fromtimestamp(the_time / 1000)

    if type(the_time) == float:
        return pd.Timestamp.fromtimestamp(the_time)

    return pd.Timestamp(the_time)

none_values = ["不变", "--", "-", "新进"]

def pct_to_float(the_str, default=None):
    if the_str in none_values:
        return None

    try:
        return float(Decimal(the_str.replace("%", "")) / Decimal(100))
    except Exception as e:
        logger.exception(e)
        return default


def to_float(the_str, default=None):
    if not the_str:
        return default
    if the_str in none_values:
        return None

    if "%" in the_str:
        return pct_to_float(the_str)
    try:
        scale = 1.0
        if the_str[-2:] == "万亿":
            the_str = the_str[0:-2]
            scale = 1000000000000
        elif the_str[-1] == "亿":
            the_str = the_str[0:-1]
            scale = 100000000
        elif the_str[-1] == "万":
            the_str = the_str[0:-1]
            scale = 10000
        if not the_str:
            return default
        return float(Decimal(the_str.replace(",", "")) * Decimal(scale))
    except Exception as e:
        logger.error("the_str:{}".format(the_str))
        logger.exception(e)
        return default
def value_to_pct(value, default=0):
    return value / 100 if value else default

# code = '000001'
code = '600039'
sec_id = '1' + '.' + code
url = f"https://push2his.eastmoney.com/api/qt/stock/kline/get?secid={sec_id}&klt=5&fqt=0&lmt=912&end=20500000&iscca=1&fields1=f1,f2,f3,f4,f5,f6,f7,f8&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62,f63,f64&ut=f057cbcbce2a86e2866ab8877db1d059&forcect=1"
resp = requests.get(url, headers=DEFAULT_HEADER)
resp.raise_for_status()
results = resp.json()
data = results["data"]
kdatas = []

if data:
    klines = data["klines"]
    name = data["name"]

    for result in klines:
        fields = result.split(",")
        the_timestamp = to_pd_timestamp(fields[0])
        open = to_float(fields[1])
        close = to_float(fields[2])
        high = to_float(fields[3])
        low = to_float(fields[4])
        volume = to_float(fields[5])
        turnover = to_float(fields[6])
        # 7 振幅
        change_pct = value_to_pct(to_float(fields[8]))
        # 9 变动
        turnover_rate = value_to_pct(to_float(fields[10]))

        kdatas.append(
            dict(
                timestamp=the_timestamp,
                provider="em",
                code=code,
                name=name,
                open=open,
                close=close,
                high=high,
                low=low,
                volume=volume,
                turnover=turnover,
                turnover_rate=turnover_rate,
                change_pct=change_pct,
            )
        )
    print(kdatas)