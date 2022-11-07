# 板块和大盘1分钟线更新
from zvtm.domain import Block1mKdata,Index1mKdata,StockTradeDay,Stock1mHfqKdata,Stock5mHfqKdata,Stock15mHfqKdata,Stock30mHfqKdata,Stock1hHfqKdata,Stock4hHfqKdata,Block5mKdata,Block15mKdata,Block30mKdata,Block1hKdata
import datetime
# 东财无法获取历史板块数据 只能获取当天数据
df0 = StockTradeDay.query_data(provider='joinquant',start_timestamp='2020-01-01',end_timestamp='2022-11-07')
# for i in range(1,len(df0),1):
#     print(df0.iloc[-i]['timestamp'])
#     if i < len(df0)-1:
#         Block1mKdata.record_data(provider="em", code='BK0473',sleeping_time=1,start_timestamp = df0.iloc[-i]['timestamp'].strftime("%Y-%m-%d"))
# Block1mKdata.record_data(provider='em', sleeping_time=1)
# Block5mKdata.record_data(provider='em', sleeping_time=1,code = 'BK0473')
# Block15mKdata.record_data(provider='em', sleeping_time=1,code = 'BK0473')
# Block30mKdata.record_data(provider='em', sleeping_time=1,code = 'BK0473')
# Block1hKdata.record_data(provider='em', sleeping_time=1,code = 'BK0473')
# for i in range(0, len(df0), 1):
#     print(df0.iloc[i]['timestamp'])
#     if i < len(df0) - 1:
#         Index1mKdata.record_data(provider='joinquant',code='000001', sleeping_time=1,
#         start_timestamp = df0.iloc[i]['timestamp'].strftime("%Y-%m-%d %H:%M:%S"), end_timestamp = df0.iloc[i + 1][
#             'timestamp'].strftime("%Y-%m-%d %H:%M:%S"))

# Stock1mHfqKdata.record_data(provider='em',code='601788', sleeping_time=1)
# Stock5mHfqKdata.record_data(provider='em',code='601788', sleeping_time=1)
# Stock15mHfqKdata.record_data(provider='em',code='601788', sleeping_time=1)
# Stock30mHfqKdata.record_data(provider='em',code='601788', sleeping_time=1)
# Stock1hHfqKdata.record_data(provider='em',code='601788', sleeping_time=1)

for i in range(0, len(df0), 1):
    print(df0.iloc[i]['timestamp'])
    if i < len(df0) - 8:
        Stock1mHfqKdata.record_data(provider='joinquant',code='000792',start_timestamp = df0.iloc[i]['timestamp'].strftime("%Y-%m-%d"), end_timestamp = df0.iloc[i + 1]['timestamp'].strftime("%Y-%m-%d %H:%M:%S"))
