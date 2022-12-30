from zvtm.domain import Stock5mKdata,StockTradeDay

# df0 = StockTradeDay.query_data(provider='joinquant',start_timestamp='2020-01-01',end_timestamp='2022-12-31')
# for i in range(0,len(df0),100):
#     print(df0.iloc[i]['timestamp'])
#     if i < len(df0)-100:
#       Stock5mKdata.record_data(provider='em',sleeping_time=0,#code=code,
#                            start_timestamp = df0.iloc[i]['timestamp'].strftime("%Y-%m-%d"), end_timestamp = df0.iloc[i+100]['timestamp'].strftime("%Y-%m-%d")) #,code='000156',force_update=True,

Stock5mKdata.record_data(provider='em',sleeping_time=0)