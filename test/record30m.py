#保存数据到数据库,每月需要保存的
from zvtm.domain import Stock,StockTradeDay,Stock30mHfqKdata,Stock30mKdata

#Stock1mHfqKdata.record_data(provider='joinquant',code='301071', sleeping_time=10,start_timestamp = "2022-04-27 11:00:00")
code = '600113'
df0 = StockTradeDay.query_data(provider='joinquant',start_timestamp='2020-01-01',end_timestamp='2022-11-07')
for i in range(0,len(df0),50):
    print(df0.iloc[i]['timestamp'])
    if i < len(df0)-500:
        Stock30mHfqKdata.record_data(provider='joinquant',code=code, sleeping_time=10,
                            start_timestamp = df0.iloc[i]['timestamp'].strftime("%Y-%m-%d"), end_timestamp = df0.iloc[i+500]['timestamp'].strftime("%Y-%m-%d")) #,code='000156',force_update=True,



Stock30mHfqKdata.record_data(provider='joinquant',code=code)