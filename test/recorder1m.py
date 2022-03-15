#保存数据到数据库,每月需要保存的
from zvtm.domain import Stock, StockTradeDay,Stock1monHfqKdata
StockTradeDay.record_data(provider='joinquant', sleeping_time=0, day_data=True) #,code='000338'
Stock.record_data(provider='joinquant', sleeping_time=0, day_data=True) #,code='000338'
Stock1monHfqKdata.record_data(provider='joinquant', sleeping_time=0, day_data=True)
