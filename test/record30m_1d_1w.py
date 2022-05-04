#保存数据到数据库,每月需要保存的
from zvtm.domain import StockTradeDay,Stock30mKdata,Stock1dKdata,Stock1wkKdata
code= '000661'
#Stock1mHfqKdata.record_data(provider='joinquant',code='301071', sleeping_time=10,start_timestamp = "2022-04-27 11:00:00")
df0 = StockTradeDay.query_data(provider='joinquant',start_timestamp='2005-01-01',end_timestamp='2022-04-29')
for i in range(0,len(df0),500):
    print(df0.iloc[i]['timestamp'])
    if i < len(df0)-500:
        Stock30mKdata.record_data(provider='joinquant',code=code, sleeping_time=10,
                            start_timestamp = df0.iloc[i]['timestamp'].strftime("%Y-%m-%d"), end_timestamp = df0.iloc[i+500]['timestamp'].strftime("%Y-%m-%d")) #,code='000156',force_update=True,

for j in range(0, len(df0), 2000):
    print(df0.iloc[i]['timestamp'])
    if j < len(df0) - 2000:
        Stock1dKdata.record_data(provider='joinquant', code=code, sleeping_time=10,
                                  start_timestamp=df0.iloc[j]['timestamp'].strftime("%Y-%m-%d"),
                                  end_timestamp=df0.iloc[j + 2000]['timestamp'].strftime(
                                      "%Y-%m-%d"))
for k in range(0, len(df0), 10000):
    print(df0.iloc[i]['timestamp'])
    if k < len(df0) - 10000:
        Stock1wkKdata.record_data(provider='joinquant', code=code, sleeping_time=10,
                                  start_timestamp=df0.iloc[k]['timestamp'].strftime("%Y-%m-%d"),
                                  end_timestamp=df0.iloc[k + 1000]['timestamp'].strftime(
                                      "%Y-%m-%d"))