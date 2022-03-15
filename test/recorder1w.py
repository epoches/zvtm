#保存数据到数据库,每周需要保存的
from zvtm.domain import Stock1wkKdata
from zvtm.domain import Fund, FundStock, Stock1wkHfqKdata, StockValuation,Stock1dKdata,StockMoneyFlow,IndexMoneyFlow
from zvtm.domain import Etf,EtfStock
# Stock1wkKdata.record_data(provider='joinquant', sleeping_time=0, day_data=True)
# Stock1wkHfqKdata.record_data(provider='joinquant', sleeping_time=0)
# 基金和基金持仓数据
# Fund.record_data(provider='joinquant', sleeping_time=0)
# FundStock.record_data(provider='joinquant', sleeping_time=0)
StockMoneyFlow.record_data(provider='joinquant', sleeping_time=0)
IndexMoneyFlow.record_data(provider='joinquant', sleeping_time=0)

Etf.record_data(provider='joinquant', sleeping_time=0)
EtfStock.record_data(provider='joinquant', sleeping_time=0)