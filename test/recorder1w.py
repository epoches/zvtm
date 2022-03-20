#保存数据到数据库,每周需要保存的
from zvtm.domain import Stock1wkKdata
from zvtm.domain import Fund, FundStock, Stock1wkHfqKdata, StockValuation,Stock1dKdata,StockMoneyFlow,IndexMoneyFlow
from zvtm.domain import Etf,EtfStock
#from zvtm.domain import *
#Stock1wkKdata.record_data(provider='joinquant', sleeping_time=0, day_data=True)
#Stock1wkHfqKdata.record_data(provider='joinquant', sleeping_time=0)
# 基金和基金持仓数据
#Fund.record_data(provider='joinquant', sleeping_time=0)
#FundStock.record_data(provider='joinquant', sleeping_time=0)
#StockMoneyFlow.record_data(provider='joinquant', sleeping_time=0) #get_money_flow WinError 10060] 由于连接方在一段时间后没有正确答复或连接的主机没有反应，连接尝试失败。
IndexMoneyFlow.record_data(provider='joinquant', sleeping_time=0)



Etf.record_data(provider='joinquant', sleeping_time=0)
EtfStock.record_data(provider='joinquant', sleeping_time=0)