# -*- coding: utf-8 -*-
import logging
import random
from typing import Union

import demjson3
import pandas as pd
import requests
from zvtm.api.kdata import generate_kdata_id, get_kdata_schema
from zvtm.utils import pd_is_not_null
from zvtm.contract.api import df_to_db
from zvtm.api import generate_kdata_id, value_to_pct
from zvtm.contract import ActorType, AdjustType, IntervalLevel, Exchange, TradableType, get_entity_exchanges
from zvtm.contract.api import decode_entity_id
from zvtm.domain import BlockCategory
from zvtm.recorders.consts import DEFAULT_HEADER
from zvtm.utils import to_pd_timestamp, to_float, json_callback_param, now_timestamp, to_time_str
exchange_map_em_flag = {
    #: 深证交易所
    Exchange.sz: 0,
    #: 上证交易所
    Exchange.sh: 1,
}

def to_em_entity_flag(exchange: Union[Exchange, str]):
    exchange = Exchange(exchange)
    return exchange_map_em_flag.get(exchange, exchange)
def to_em_sec_id(entity_id):
    entity_type, exchange, code = decode_entity_id(entity_id)
    # 主力合约
    if entity_type == "future" and code[-1].isalpha():
        code = code + "m"
    if entity_type == "currency" and "CNYC" in code:
        return f"120.{code}"
    return f"{to_em_entity_flag(exchange)}.{code}"
def get_exchange(code):
    if code >= "333333":
        return "sh"
    else:
        return "sz"
def get_kdata(entity_id, level=IntervalLevel.LEVEL_1MON, adjust_type=AdjustType.qfq, limit=10000):
    entity_type, exchange, code = decode_entity_id(entity_id)
    level = IntervalLevel(level)
    # limit = abs(limit)
    sec_id = to_em_sec_id(entity_id)
    # f131 结算价
    # f133 持仓
    # 目前未获取
    # 'https://push2his.eastmoney.com/api/qt/stock/kline/get?secid=0.000001&klt=103&fqt=2&lmt=2&end=20500000&iscca=1&fields1=f1,f2,f3,f4,f5,f6,f7,f8&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62,f63,f64&ut=f057cbcbce2a86e2866ab8877db1d059&forcect=1'
    url = f"https://push2his.eastmoney.com/api/qt/stock/kline/get?secid={sec_id}&klt=103&fqt=2&lmt=12&end=20500000&iscca=1&fields1=f1,f2,f3,f4,f5,f6,f7,f8&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62,f63,f64&ut=f057cbcbce2a86e2866ab8877db1d059&forcect=1"

    resp = requests.get(url, headers=DEFAULT_HEADER)
    resp.raise_for_status()
    results = resp.json()
    resp.close()
    data = results["data"]

    kdatas = []

    if data:
        klines = data["klines"]
        name = data["name"]

        for result in klines:
            # "2000-01-28,1005.26,1012.56,1173.12,982.13,3023326,3075552000.00"
            # "2021-08-27,19.39,20.30,20.30,19.25,1688497,3370240912.00,5.48,6.01,1.15,3.98,0,0,0"
            # time,open,close,high,low,volume,turnover
            # "2022-04-13,10708,10664,10790,10638,402712,43124771328,1.43,0.57,60,0.00,4667112399583576064,4690067230254170112,1169270784"
            fields = result.split(",")
            the_timestamp = to_pd_timestamp(fields[0])

            the_id = generate_kdata_id(entity_id=entity_id, timestamp=the_timestamp, level=level)

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
                    id=the_id,
                    timestamp=the_timestamp,
                    entity_id=entity_id,
                    provider="em",
                    code=code,
                    name=name,
                    level=level.value,
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
    if kdatas:
        df = pd.DataFrame.from_records(kdatas)
        return df
if __name__ == "__main__":
    code = '601788'
    entity_id = 'stock_' + get_exchange(code) + '_' + code
    print(entity_id)
    df = get_kdata(entity_id=entity_id)
    data_schema = get_kdata_schema(entity_type="stock", level=IntervalLevel.LEVEL_1MON, adjust_type=AdjustType.hfq)
    if pd_is_not_null(df):
        df_to_db(df=df, data_schema=data_schema, provider='eastmoney', force_update=True)
    else:
        print(f"no kdata for {entity.id}")