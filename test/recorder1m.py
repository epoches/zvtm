#保存数据到数据库,每月需要保存的
from zvtm.domain import Stock, StockTradeDay,Stock1monHfqKdata,Stock1mHfqKdata
#StockTradeDay.record_data(provider='joinquant', sleeping_time=0, day_data=True) #,code='000338'
#Stock.record_data(provider='joinquant', sleeping_time=0, day_data=True) #,code='000338'
#Stock1monHfqKdata.record_data(provider='joinquant', sleeping_time=0, day_data=True)
#Stock1mHfqKdata.record_data(provider='joinquant',code='301071', sleeping_time=10,start_timestamp = "2022-04-27 11:00:00")
df0 = StockTradeDay.query_data(provider='joinquant',start_timestamp='2008-04-22',end_timestamp='2022-04-27')
for i in range(0,len(df0),20):
    print(df0.iloc[i]['timestamp'])
    if i < len(df0)-20:
      Stock1mHfqKdata.record_data(provider='joinquant',code='600251', sleeping_time=10,
                            start_timestamp = df0.iloc[i]['timestamp'].strftime("%Y-%m-%d"), end_timestamp = df0.iloc[i+20]['timestamp'].strftime("%Y-%m-%d")) #,code='000156',force_update=True,