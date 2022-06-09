# 获取东财数据 板块和股票

import requests
import json
import pandas as pd
from typing import Union
import demjson3
from zvtm.contract import ActorType, AdjustType, IntervalLevel, Exchange, TradableType, get_entity_exchanges
from zvtm.contract.api import decode_entity_id
from zvtm.domain import BlockCategory
from zvtm.recorders.consts import DEFAULT_HEADER
from zvtm.utils import to_pd_timestamp, to_float, json_callback_param, now_timestamp, to_time_str
from zvtm.recorders.em.em_api import get_future_list,to_em_entity_flag
from zvtm.contract.api import df_to_db
from zvtm.contract.recorder import Recorder
from zvtm.domain import Block, BlockCategory,BlockStock

def get_hangye_data():
    ret_data = []
    try:
        url = 'https://push2.eastmoney.com/api/qt/clist/get?cb=jQuery112307879834664846898_1630941013041&fid=f62&po=1&pz=60&pn=1&np=1&fltt=2&invt=2&ut=b2884a393a59ad64002292a3e90d46a5&fs=m%3A90+t%3A2&fields=f12%2Cf14%2Cf2%2Cf3%2Cf62%2Cf184%2Cf66%2Cf69%2Cf72%2Cf75%2Cf78%2Cf81%2Cf84%2Cf87%2Cf204%2Cf205%2Cf124%2Cf1%2Cf13'
        ret = requests.get(url)
        if ret.status_code == 200:
            data =str(ret.content,encoding='utf-8')
            data = data.replace('jQuery112307879834664846898_1630941013041(','').strip()
            data = data[:-1].replace(")",'')
            #print(data)
            data = json.loads(data)
            #print(data)
            if 'data' in data:
                data = data['data']
                if 'diff' in data:
                    for item in data['diff']:
                        if 'f62' in item and float(item['f62']) > 0  and 'f66' in item and float(item['f66']) > 0 : # 主力流入 超大单流入
                            if 'f3' in item and float(item['f3']) > 0: # 上涨
                                ret_data.append(item)

    except Exception as e:
        pass
    return ret_data



def get_gegu_data(item):
    global total_zhangting
    total_zhangting = 0
    ret_data = []
    try:
        url = 'https://push2.eastmoney.com/api/qt/clist/get?ut=bd1d9ddb04089700cf9c27f6f7426281&pi=0&pz=100&po=1&fid=f62&fs=b:%s&&fields=f2,f3,f5,f6,f7,f10,f8,f9,f12,f13,f14,f15,f16,f17,f18,f19,f20,f23,f62,f66&cb=jQuery112408385759504702417_1630941255158&_=1630941255159' % (
        item['f12'])
        print(url)
        ret = requests.get(url)
        if ret.status_code == 200:
            data = str(ret.content, encoding='utf-8')
            data = data.replace('jQuery112408385759504702417_1630941255158(', '').strip()
            data = data[:-1].replace(")", '')
            # print(data)
            data = json.loads(data)
            if 'data' in data:
                data = data['data']
                total = int(data['total'])
                if total > 100:
                    total = 100
                if 'diff' in data:
                    data = data['diff']
                    for i in range(0, total - 1, 1):
                        item = data[str(i)]
                        if item['f14'].upper().find('ST') > -1:  # 跳过st股票
                            continue
                        if item['f12'].startswith("300"):  # 过滤掉创业板
                            continue
                        if float(item['f3']) > 960:  # 涨停股计数 可能量比没有达到2
                            total_zhangting = total_zhangting + 1
                        # if item['f10'] < filter_liangbi:  # 根据量能比进行过滤
                        #     continue

                        if 'f62' in item and float(item['f62']) > 0 and 'f66' in item and float(
                                item['f66']) > 0:  # 主力流入 超大单流入
                            # if 'f3' in item and float(item['f3']) > 500: # 上涨
                            ret_data.append(item)
    except Exception as e:
        pass
    return ret_data

def get_stock_data(item,
    entity_type: Union[TradableType, str] = "block",
    exchange: Union[Exchange, str] = 'cn',
    limit: int = 10000,

):
    dfs = []
    # 行业
    entity_flag = f"fs=b:{item['code']}"
    fields = "f1,f2,f3,f4,f12,f13,f14"
    url = f"https://push2.eastmoney.com/api/qt/clist/get?np=1&fltt=2&invt=2&fields={fields}&pn=1&pz={limit}&fid=f62&po=1&{entity_flag}&ut=f057cbcbce2a86e2866ab8877db1d059&forcect=1&cb=cbCallbackMore&&callback=jQuery34109676853980006124_{now_timestamp() - 1}&_={now_timestamp()}"
    resp = requests.get(url, headers=DEFAULT_HEADER)
    resp.raise_for_status()
    result = json_callback_param(resp.text)
    data = result["data"]["diff"]
    df = pd.DataFrame.from_records(data=data)
    df = df[["f12", "f13", "f14"]]
    df.columns = ["stock_code", "exchange", "stock_name"]
    df["code"] = item["code"]
    df["name"] = item["name"]
    df["exchange"] = exchange
    df["entity_type"] = entity_type
    df["id"] = df[["entity_type", "exchange", "code","stock_code"]].apply(lambda x: "_".join(x.astype(str)), axis=1)
    df["entity_id"] = df["id"]
    #df["category"] = block_category

    dfs.append(df)
    return pd.concat(dfs)

items = Block.query_data(provider='em')

#items = get_hangye_data()

for i in range(len(items)):
    #item_stock_data = get_gegu_data(item)
    df = get_stock_data(items.iloc[i])
    df_to_db(df=df, data_schema=BlockStock, provider="em", force_update=True)
    # item_stock_data = get_gegu_data(item)
    # data = item_stock_data['f12', 'f14']
    # df = pd.DataFrame.from_records(data=data)
    # for bs in item_stock_data:
    #     data = bs['f12','f14']
    #     df = pd.DataFrame.from_records(data=data)
