#保存数据到数据库,每月需要保存的
from zvtm.domain import Stock, StockTradeDay,Stock1monHfqKdata,Stock1mHfqKdata,Index1mKdata
#StockTradeDay.record_data(provider='joinquant', sleeping_time=0, day_data=True) #,code='000338'
#Stock.record_data(provider='joinquant', sleeping_time=0, day_data=True) #,code='000338'
#Stock1monHfqKdata.record_data(provider='joinquant', sleeping_time=0, day_data=True)
#Stock1mHfqKdata.record_data(provider='joinquant',code='301071', sleeping_time=10,start_timestamp = "2022-04-27 11:00:00")
# df0 = StockTradeDay.query_data(provider='joinquant',start_timestamp='2022-08-16',end_timestamp='2022-09-19')
# for i in range(0,len(df0),10):
#     print(df0.iloc[i]['timestamp'])
#     if i < len(df0)-10:
#         # Index1mKdata.record_data(provider='joinquant',code='000001', sleeping_time=1,
#         #                     start_timestamp = df0.iloc[i]['timestamp'].strftime("%Y-%m-%d"), end_timestamp = df0.iloc[i+20]['timestamp'].strftime("%Y-%m-%d")) #,code='000156',force_update=True,
#         Stock1mHfqKdata.record_data(provider='joinquant',code='601788', sleeping_time=1,
#                             start_timestamp = df0.iloc[i]['timestamp'].strftime("%Y-%m-%d"), end_timestamp = df0.iloc[i+10]['timestamp'].strftime("%Y-%m-%d")) #,code='000156',force_update=True,

Stock1mHfqKdata.record_data(provider='joinquant',code='601788', sleeping_time=1, start_timestamp = '2022-07-07',end_timestamp='2022-07-08')
# Index1mKdata.record_data(provider='joinquant',code='000001', sleeping_time=1,
#                             start_timestamp = '2022-09-19' , end_timestamp = '2022-09-19')