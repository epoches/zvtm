#保存数据到数据库,每日需要保存的
from zvtm.domain import  Stock1dHfqKdata,StockValuation,Stock1dKdata

#Stock1dHfqKdata.record_data(provider='joinquant', sleeping_time=0, day_data=True)
#StockValuation.record_data(provider='joinquant', sleeping_time=0)
Stock1dKdata.record_data(provider='joinquant', sleeping_time=0,code='000156',
    start_timestamp = '2022-01-01', end_timestamp = '2022-03-02')#,code='000156',force_update=True,