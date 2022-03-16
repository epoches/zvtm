#特殊出错数据保存数据到数据库
from zvtm.domain import  Stock1dHfqKdata,StockValuation,Stock1dKdata,FundStock


#StockValuation.record_data(provider='joinquant',code='600472', sleeping_time=0)
#StockValuation.record_data(provider='joinquant',code='000018', sleeping_time=0)
#StockValuation.record_data(provider='joinquant',code='000406', sleeping_time=0)
FundStock.record_data(provider='joinquant',code='000002', sleeping_time=0)