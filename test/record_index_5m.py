from zvtm.domain import Index5mKdata,StockTradeDay
#Index5mKdata.record_data(provider="joinquant", code='000001',start_timestamp = '2018-01-01', end_timestamp = '2018-06-30', day_data=False)
#Index5mKdata.record_data(provider="joinquant", code='000001',start_timestamp = '2018-07-01', end_timestamp = '2018-12-31', day_data=False)
df0 = StockTradeDay.query_data(provider='joinquant',start_timestamp='2015-01-01',end_timestamp='2015-12-31')
for i in range(len(df0)):
    print(df0.iloc[i]['timestamp'])
    if i < len(df0)-1:
        Index5mKdata.record_data(provider="joinquant", code='000001',start_timestamp = df0.iloc[i]['timestamp'].strftime("%Y-%m-%d"), end_timestamp = df0.iloc[i+1]['timestamp'].strftime("%Y-%m-%d"), day_data=False,sleeping_time=5)
