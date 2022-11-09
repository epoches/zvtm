from zvtm.domain import  Stock1hHfqKdata,StockTradeDay

code = '600113'
df0 = StockTradeDay.query_data(provider='joinquant',start_timestamp='2020-01-01',end_timestamp='2022-12-22')
for i in range(0,len(df0),1200):
    # print(df0.iloc[i]['timestamp'])
    if i < len(df0)-1200:
         Stock1hHfqKdata.record_data(provider='joinquant',code=code,
                            start_timestamp = df0.iloc[i]['timestamp'].strftime("%Y-%m-%d"), end_timestamp = df0.iloc[i+1200]['timestamp'].strftime("%Y-%m-%d"))

Stock1hHfqKdata.record_data(provider='joinquant',code=code)