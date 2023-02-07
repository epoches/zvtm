"""
东方财富网-沪深京 A 股-实时行情
https://quote.eastmoney.com/center/gridlist.html#hs_a_board
:return: 实时行情
:rtype: pandas.DataFrame
"""
# 字段
# f2	最新价
# f3	涨跌幅
# f4	涨跌额
# f5	总手
# f6	成交额
# f7	振幅
# f8	换手率
# f9	市盈率
# f10	量比
# f11	5分钟涨跌幅
# f12	股票代码
# f13	市场
# f14	股票名称
# f15	最高价
# f16	最低价
# f17	开盘价
# f18	昨收
# f20	总市值
# f21	流通市值
# f22	涨速
# f23	市净率
# f24	60日涨跌幅
# f25	年初至今涨跌幅
# f26	上市日期
# f28	昨日结算价
# f30	现手
# f31	买入价
# f32	卖出价
# f33	委比
# f34	外盘
# f35	内盘
# f36	人均持股数
# f37	净资产收益率(加权)
# f38	总股本
# f39	流通股
# f40	营业收入
# f41	营业收入同比
# f42	营业利润
# f43	投资收益
# f44	利润总额
# f45	净利润
# f46	净利润同比
# f47	未分配利润
# f48	每股未分配利润
# f49	毛利率
# f50	总资产
# f51	流动资产
# f52	固定资产
# f53	无形资产
# f54	总负债
# f55	流动负债
# f56	长期负债
# f57	资产负债比率
# f58	股东权益
# f59	股东权益比
# f60	公积金
# f61	每股公积金
# f62	主力净流入
# f63	集合竞价
# f64	超大单流入
# f65	超大单流出
# f66	超大单净额
# f69	超大单净占比
# f70	大单流入
# f71	大单流出
# f72	大单净额
# f75	大单净占比
# f76	中单流入
# f77	中单流出
# f78	中单净额
# f81	中单净占比
# f82	小单流入
# f83	小单流出
# f84	小单净额
# f87	小单净占比
# f88	当日DDX
# f89	当日DDY
# f90	当日DDZ
# f91	5日DDX
# f92	5日DDY
# f94	10日DDX
# f95	10日DDY
# f97	DDX飘红天数(连续)
# f98	DDX飘红天数(5日)
# f99	DDX飘红天数(10日)
# f100	行业
# f101	板块领涨股
# f102	地区板块
# f103	备注
# f104	上涨家数
# f105	下跌家数
# f106	平家家数
# f112	每股收益
# f113	每股净资产
# f114	市盈率（静）
# f115	市盈率（TTM）
# f124	当前交易时间
# f128	板块领涨股
# f129	净利润
# f130	市销率TTM
# f131	市现率TTM
# f132	总营业收入TTM
# f133	股息率
# f134	行业板块的成分股数
# f135	净资产
# f138	净利润TTM
# f221	更新日期
# f400	pre：盘前时间
# after：盘后时间
# period：盘中时间
# f51,f52,f53,f54,f55,
# timestamp,open,close,high,low
# f56,f57,f58,f59,f60,f61,f62,f63,f64
# volume,turnover,震幅,change_pct,change,turnover_rate
# 市值,流通市值,pe,pb  f20,f21,f9,f23
# 'f51': '日期',
# 'f52': '开盘',
# 'f53': '收盘',
# 'f54': '最高',
# 'f55': '最低',
# 'f56': '成交量',
# 'f57': '成交额',
# 'f58': '振幅',
# 'f59': '涨跌幅',
# 'f60': '涨跌额',
# 'f61': '换手率',
import requests
import pandas as pd
from zvtm.utils.time_utils import now_pd_timestamp, to_time_str, to_pd_timestamp
from zvtm.utils.pd_utils import pd_is_not_null



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

df = stock_zh_a_spot_em()

for i in range(len(df)):
    entity_id = "{}_{}_{}".format('stock',df.loc[i,"entity"],df.loc[i,"code"])
    df.loc[i,"entity_id"] = entity_id
    df.loc[i,"id"] = "{}_{}".format(to_time_str(entity_id),df.loc[i,"timestamp"].strftime('%Y-%m-%d'))

# print(df)
provider = 'em'
from zvtm.domain.fundamental.valuation1 import StockValuation1
data_schema = StockValuation1
force_update=True

from zvtm.contract.api import get_db_engine,get_schema_columns
db_engine = get_db_engine(provider, data_schema=data_schema)

schema_cols = get_schema_columns(data_schema)
cols = set(df.columns.tolist()) & set(schema_cols)

df = df[cols]
if pd_is_not_null(df):
    # saved = saved + len(df_current)
    df.to_sql(data_schema.__tablename__, db_engine, index=False, if_exists="append")
