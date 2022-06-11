#保存数据到数据库
from zvtm.domain import Stock, StockTradeDay, Stock1dHfqKdata,Stock1wkKdata,Stock1dKdata
#StockTradeDay.record_data(provider='joinquant', sleeping_time=1, day_data=True) #,code='000338'
#Stock.record_data(provider='joinquant',code='000338', sleeping_time=1, day_data=True) #,code='000338'
#Stock1dKdata.record_data(provider='joinquant',code='600251', sleeping_time=0, day_data=True)
#Stock1dHfqKdata.record_data(provider='joinquant',code='000338', sleeping_time=0, day_data=True)
#StockTradeDay.record_data(provider='joinquant', sleeping_time=1, day_data=True) #,code='000338'

df0 = StockTradeDay.query_data(provider='joinquant',start_timestamp='2005-01-01',end_timestamp='2022-04-22')
for i in range(0,len(df0),1200):
    print(df0.iloc[i]['timestamp'])
    if i < len(df0)-1200:
        Stock1dKdata.record_data(provider='joinquant',code='600251', sleeping_time=10,
                            start_timestamp = df0.iloc[i]['timestamp'].strftime("%Y-%m-%d"), end_timestamp = df0.iloc[i+1200]['timestamp'].strftime("%Y-%m-%d"))