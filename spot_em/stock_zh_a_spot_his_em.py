# 对历史没获取的数据补缺 东方财富网 - 行情首页 - 沪深京A股 - 每日行情 历史数据
# 获取 日 周 月数据 以及 分钟数据 还有盘前数据
# https://push2his.eastmoney.com/api/qt/stock/kline/get?cb=jQuery112407372648993479995_1675913352824&fields1=f1%2Cf2%2Cf3%2Cf4%2Cf5%2Cf6&fields2=f51%2Cf52%2Cf53%2Cf54%2Cf55%2Cf56%2Cf57%2Cf58%2Cf59%2Cf60%2Cf61&ut=7eea3edcaed734bea9cbfc24409ed989&klt=101&fqt=1&secid=1.603777&beg=0&end=20500000&_=1675913352885
import requests
import pandas as pd
from zvtm.utils.pd_utils import pd_is_not_null
from zvtm.utils.time_utils import to_time_str, now_pd_timestamp, TIME_FORMAT_DAY, TIME_FORMAT_ISO8601
from zvtm.contract.api import df_to_db
from zvtm.domain import  Stock1dKdata,Stock1dHfqKdata,Stock1hKdata,Stock1mHfqKdata,Stock1monKdata
from zvtm.contract.api import get_db_engine,get_schema_columns
import logging
from zvtm import init_log
logger = logging.getLogger(__name__)
init_log("em_kdata1d_his_runner.log")

from zvtm.ak.stock.stock_info_em import code_id_map_em

from bs4 import BeautifulSoup
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/114.0.0.0 Safari/537.36"
}
from zvtm.ak.stock_feature.stock_a_indicator import stock_a_indicator_lg


from  zvtm.ak.stock_feature.stock_hist_em import stock_zh_a_hist,stock_zh_a_hist_min_em,stock_zh_a_hist_pre_min_em




def split_list_equal_parts(lst, parts=3):
    length = len(lst)
    size = length // parts
    remainder = length % parts
    return [lst[i * size + min(i, remainder):(i + 1) * size + min(i + 1, remainder)] for i in range(parts)]



def list_to_db(df,logger):
    force_update = True
    data_schema = Stock1dKdata
    provider = 'em'
    db_engine = get_db_engine(provider, data_schema=data_schema)
    schema_cols = get_schema_columns(data_schema)
    cols = set(df.columns.tolist()) & set(schema_cols)
    df.rename(columns={"代码": "code"}, inplace=True)
    df.rename(columns={"日期": "timestamp"}, inplace=True)
    df.rename(columns={"名称": "name"}, inplace=True)
    df.rename(columns={"收盘": "close"}, inplace=True)
    df.rename(columns={"最高": "high"}, inplace=True)
    df.rename(columns={"开盘": "open"}, inplace=True)
    df.rename(columns={"最低": "low"}, inplace=True)
    df.rename(columns={"成交量": "volume"}, inplace=True)
    df.rename(columns={"成交额": "turnover"}, inplace=True)
    df.rename(columns={"涨跌幅": "change_pct"}, inplace=True)
    df.rename(columns={"换手率": "turnover_rate"}, inplace=True)
    import datetime
    # dt = datetime.datetime.now().strftime('%Y-%m-%d')
    for i in range(len(df)):
        entity_id = "{}_{}_{}".format('stock', df.loc[i, "entity"], df.loc[i, "code"])
        df.loc[i, "entity_id"] = entity_id
        df.loc[i, "id"] = "{}_{}".format(to_time_str(entity_id), df.loc[i, "timestamp"])
        df.loc[i, "timestamp"] = df.loc[i, "timestamp"]
        df.loc[i, "level"] = "1d"
        df.loc[i, "provider"] = "em"
    df = df[schema_cols]
    if pd_is_not_null(df):
        df_to_db(df=df, data_schema=data_schema, provider=provider, force_update=force_update)
    else:
        logger.info(f"no kdata for {entity.id}")

def fetch_and_store_data(code_list, start_date, logger):
    list_of_df = []
    for code in code_list['code']:
        df = stock_zh_a_hist(symbol=code, start_date=start_date, adjust='', period='daily')
        list_of_df.append(df)
        # 可以选择在这里添加time.sleep(1)来避免请求过快

    df = pd.concat(list_of_df, ignore_index=True)
    list_to_db(df, logger)

#
stock_a_indicator_lg_all_df = stock_a_indicator_lg(symbol="all")
list1,list2,list3 = split_list_equal_parts(stock_a_indicator_lg_all_df)
dt = '20240718'
list_of_df = []
import time
# stock_a_indicator_lg_all_df = ['601788','002162']
# for code in list1['code']: #['code']
#     df = stock_zh_a_hist(symbol=code,start_date=dt,adjust='',period='daily')
#     # print(df)
#     list_of_df.append(df)
#     # time.sleep(1)
# df = pd.concat(list_of_df, ignore_index=True)
# list_to_db(df,logger)
fetch_and_store_data(list1, dt, logger)
time.sleep(100)
fetch_and_store_data(list2, dt, logger)
time.sleep(100)
fetch_and_store_data(list3, dt, logger)