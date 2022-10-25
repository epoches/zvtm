# 板块和大盘1分钟线更新
from zvtm.domain import Block1mKdata,Index1mKdata,StockTradeDay,Stock1mHfqKdata
import datetime
# 东财无法获取历史板块数据 只能获取当天数据
df0 = StockTradeDay.query_data(provider='joinquant',start_timestamp='2022-05-01',end_timestamp='2022-10-24')
for i in range(1,len(df0),1):
    print(df0.iloc[-i]['timestamp'])
    if i < len(df0)-1:
        Block1mKdata.record_data(provider="em", code='BK0473',sleeping_time=1,start_timestamp = df0.iloc[-i]['timestamp'].strftime("%Y-%m-%d"))

# for i in range(0, len(df0), 1):
#     print(df0.iloc[i]['timestamp'])
#     if i < len(df0) - 1:
#         Index1mKdata.record_data(provider='joinquant',code='000001', sleeping_time=1,
#         start_timestamp = df0.iloc[i]['timestamp'].strftime("%Y-%m-%d %H:%M:%S"), end_timestamp = df0.iloc[i + 1][
#             'timestamp'].strftime("%Y-%m-%d %H:%M:%S"))
# Stock1mHfqKdata(provider='em',code='601788',start_timestamp='2022-10-25')
# Stock1mHfqKdata(provider='joinquant',code='601788',start_timestamp= datetime.datetime.now() + datetime.timedelta(hours=-16))
