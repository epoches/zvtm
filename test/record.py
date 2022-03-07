#保存数据到数据库
from zvtm.domain import Stock, StockTradeDay, Stock1dHfqKdata,Stock1wkKdata
#StockTradeDay.record_data(provider='joinquant', sleeping_time=1, day_data=True) #,code='000338'
#Stock.record_data(provider='joinquant',code='000338', sleeping_time=1, day_data=True) #,code='000338'
Stock1dHfqKdata.record_data(provider='joinquant', sleeping_time=0, day_data=True)
