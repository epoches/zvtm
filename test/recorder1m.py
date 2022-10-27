#保存数据到数据库,每月需要保存的
from zvtm.domain import Stock, StockTradeDay,Stock1monHfqKdata,Stock1mHfqKdata,Index1mKdata
#StockTradeDay.record_data(provider='joinquant', sleeping_time=0, day_data=True) #,code='000338'
#Stock.record_data(provider='joinquant', sleeping_time=0, day_data=True) #,code='000338'
#Stock1monHfqKdata.record_data(provider='joinquant', sleeping_time=0, day_data=True)
#Stock1mHfqKdata.record_data(provider='joinquant',code='301071', sleeping_time=10,start_timestamp = "2022-04-27 11:00:00")
df0 = StockTradeDay.query_data(provider='joinquant',start_timestamp='2019-10-31',end_timestamp='2022-04-29')
for i in range(0,len(df0),20):
    print(df0.iloc[i]['timestamp'])
    if i < len(df0)-20:
        Index1mKdata.record_data(provider='joinquant',code='000001', sleeping_time=1,
                            start_timestamp = df0.iloc[i]['timestamp'].strftime("%Y-%m-%d"), end_timestamp = df0.iloc[i+20]['timestamp'].strftime("%Y-%m-%d")) #,code='000156',force_update=True,
        # Stock1mHfqKdata.record_data(provider='joinquant',code='600770', sleeping_time=10,
        #                     start_timestamp = df0.iloc[i]['timestamp'].strftime("%Y-%m-%d"), end_timestamp = df0.iloc[i+20]['timestamp'].strftime("%Y-%m-%d")) #,code='000156',force_update=True,