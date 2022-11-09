#保存数据到数据库,5分钟需要保存的
from zvtm.domain import Stock15mHfqKdata,StockTradeDay

# Stock15mHfqKdata.record_data(provider='em', sleeping_time=1)
# Stock15mHfqKdata.record_data(provider='joinquant',code='000156', sleeping_time=0,
#     start_timestamp = '2015-02-11', end_timestamp = '2015-2-17')#,code='000156',force_update=True

code = '600113'
df0 = StockTradeDay.query_data(provider='joinquant',start_timestamp='2020-01-01',end_timestamp='2022-12-31')
for i in range(0,len(df0),30):
    print(df0.iloc[i]['timestamp'])
    if i < len(df0)-30:
      Stock15mHfqKdata.record_data(provider='joinquant',code=code,
                            start_timestamp = df0.iloc[i]['timestamp'].strftime("%Y-%m-%d"), end_timestamp = df0.iloc[i+30]['timestamp'].strftime("%Y-%m-%d")) #,code='000156',force_update=True,

Stock15mHfqKdata.record_data(provider='joinquant',code=code)