from zvtm.domain import Index1hKdata,StockTradeDay
Index1hKdata.record_data(provider="joinquant", code='000001', end_timestamp = '2019-12-30', day_data=False,sleeping_time=10)
Index1hKdata.record_data(provider="joinquant", code='000001', end_timestamp = '2020-12-30', day_data=False,sleeping_time=10)
Index1hKdata.record_data(provider="joinquant", code='000001', end_timestamp = '2021-12-30', day_data=False,sleeping_time=10)
Index1hKdata.record_data(provider="joinquant", code='000001', end_timestamp = '2022-04-21', day_data=False,sleeping_time=10)
# df0 = StockTradeDay.query_data(provider='joinquant',start_timestamp='2006-05-30',end_timestamp='2021-12-31')
# for i in range(len(df0)):
#     print(df0.iloc[i]['timestamp'])
#     if i < len(df0)-1:
#         Index1hKdata.record_data(provider="joinquant", code='000001',start_timestamp = df0.iloc[i]['timestamp'].strftime("%Y-%m-%d"), end_timestamp = df0.iloc[i+1]['timestamp'].strftime("%Y-%m-%d"), day_data=False,sleeping_time=20)
