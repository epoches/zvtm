#保存数据到数据库,5分钟需要保存的
from zvtm.domain import Stock5mHfqKdata,StockTradeDay
# Stock5mHfqKdata.record_data(provider='joinquant',code='601398', sleeping_time=5,
#                             )
# start_timestamp = '2019-01-01', end_timestamp = '2020-02-27'
code = '600113'
df0 = StockTradeDay.query_data(provider='joinquant',start_timestamp='2020-01-01',end_timestamp='2022-11-02')
for i in range(0,len(df0),100):
    print(df0.iloc[i]['timestamp'])
    if i < len(df0)-100:
      Stock5mHfqKdata.record_data(provider='joinquant',code=code,
                           start_timestamp = df0.iloc[i]['timestamp'].strftime("%Y-%m-%d"), end_timestamp = df0.iloc[i+100]['timestamp'].strftime("%Y-%m-%d")) #,code='000156',force_update=True,

Stock5mHfqKdata.record_data(provider='joinquant',code=code)