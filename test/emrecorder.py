# -*- coding: utf-8 -*-
import logging
import time

from apscheduler.schedulers.background import BackgroundScheduler

from zvtm import init_log, zvt_config
from zvtm.domain import *
from zvtm.informer.informer import EmailInformer

# Stock.record_data(provider='eastmoney', sleeping_time=5)
# StockDetail.record_data(provider='eastmoney', sleeping_time=5)
# Stock.record_data(provider='em', sleeping_time=5)
# StockDetail.record_data(provider='em', sleeping_time=5)
#FinanceFactor.record_data(provider='eastmoney', sleeping_time=5)
#BalanceSheet.record_data(provider='eastmoney', sleeping_time=5)
#IncomeStatement.record_data(provider='eastmoney', sleeping_time=5)
#CashFlowStatement.record_data(provider='eastmoney', sleeping_time=5)
#DividendFinancing.record_data(provider='eastmoney', sleeping_time=5)
#HolderTrading.record_data(provider='eastmoney', sleeping_time=5) #出错了 id有问题 stock_sz_000961_2022-01-12_中南城市建设投资有限公司
#ManagerTrading.record_data(provider='eastmoney', sleeping_time=5) #和上一个一样
# TopTenHolder.record_data(provider='eastmoney', sleeping_time=5)
# TopTenTradableHolder.record_data(provider='eastmoney', sleeping_time=5)
# StockInstitutionalInvestorHolder.record_data(provider='em', sleeping_time=5)
# StockTopTenFreeHolder.record_data(provider='em', sleeping_time=5)
# StockActorSummary.record_data(provider='em', sleeping_time=5)
# Index.record_data(provider='exchange', sleeping_time=5)
# IndexStock.record_data(provider='exchange', sleeping_time=5)
# Index1dKdata.record_data(provider='joinquant', sleeping_time=0,code='000001')
Stock1dHfqKdata.record_data(provider='em', sleeping_time=0)
# Block1dKdata.record_data(provider='em', sleeping_time=0,codes=['BK0726', 'BK0726', 'BK0725', 'BK0425', 'BK0481', 'BK0545'])