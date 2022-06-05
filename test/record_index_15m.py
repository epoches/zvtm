from zvtm.domain import Index15mKdata,StockTradeDay

df0 = StockTradeDay.query_data(provider='joinquant',start_timestamp='2015-11-22',end_timestamp='2022-05-21')
for i in range(0,len(df0),300):
    print(df0.iloc[i]['timestamp'])
    if i < len(df0)-300:
        Index15mKdata.record_data(provider='joinquant',code='000001', sleeping_time=2,
                            start_timestamp = df0.iloc[i]['timestamp'].strftime("%Y-%m-%d"), end_timestamp = df0.iloc[i+300]['timestamp'].strftime("%Y-%m-%d")) #,code='000156',force_update=True,