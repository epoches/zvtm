from zvtm.domain import  Stock1hHfqKdata,StockTradeDay,Stock1hKdata
Stock1hKdata.record_data(provider='joinquant',code='600251', sleeping_time=10)
# df0 = StockTradeDay.query_data(provider='joinquant',start_timestamp='2005-01-01',end_timestamp='2022-04-22')
# for i in range(0,len(df0),1200):
#     print(df0.iloc[i]['timestamp'])
#     if i < len(df0)-1200:
#          # Stock1hHfqKdata.record_data(provider='joinquant',code='600251', sleeping_time=10,
#          #                    start_timestamp = df0.iloc[i]['timestamp'].strftime("%Y-%m-%d"), end_timestamp = df0.iloc[i+1200]['timestamp'].strftime("%Y-%m-%d"))
#          Stock1hKdata.record_data(provider='joinquant',code='600251', sleeping_time=10,
#                             start_timestamp = df0.iloc[i]['timestamp'].strftime("%Y-%m-%d"), end_timestamp = df0.iloc[i+1200]['timestamp'].strftime("%Y-%m-%d"))