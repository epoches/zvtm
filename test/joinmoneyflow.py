from zvtm.domain import  StockMoneyFlow,IndexMoneyFlow,BlockMoneyFlow,Block,BlockStock

# BlockMoneyFlow.record_data(provider='joinquant', sleeping_time=0)  没有实现
# Block.record_data(provider='joinquant', sleeping_time=0) 没有实现
# BlockStock.record_data(provider='joinquant', sleeping_time=0) 没有实现
IndexMoneyFlow.record_data(provider='joinquant', sleeping_time=0)
StockMoneyFlow.record_data(provider='joinquant', sleeping_time=0)