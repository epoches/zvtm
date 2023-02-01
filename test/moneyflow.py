from zvtm.domain import  StockMoneyFlow,IndexMoneyFlow,BlockMoneyFlow,Block,BlockStock
# 不是vip没法弄
BlockMoneyFlow.record_data(provider='sina', sleeping_time=0)
Block.record_data(provider='sina', sleeping_time=0)
#BlockStock.record_data(provider='sina', sleeping_time=1)
#IndexMoneyFlow.record_data(provider='joinquant', sleeping_time=0)
#StockMoneyFlow.record_data(provider='joinquant', sleeping_time=0)