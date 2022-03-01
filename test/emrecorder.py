# -*- coding: utf-8 -*-
import logging
import time

from apscheduler.schedulers.background import BackgroundScheduler

from zvtm import init_log, zvt_config
from zvtm.domain import *
from zvtm.informer.informer import EmailInformer

Stock.record_data(provider='eastmoney', sleeping_time=0)
StockDetail.record_data(provider='eastmoney', sleeping_time=0)
FinanceFactor.record_data(provider='eastmoney', sleeping_time=0)
BalanceSheet.record_data(provider='eastmoney', sleeping_time=0)
IncomeStatement.record_data(provider='eastmoney', sleeping_time=0)
CashFlowStatement.record_data(provider='eastmoney', sleeping_time=0)
DividendFinancing.record_data(provider='eastmoney', sleeping_time=0)
HolderTrading.record_data(provider='eastmoney', sleeping_time=0)
ManagerTrading.record_data(provider='eastmoney', sleeping_time=0)
TopTenHolder.record_data(provider='eastmoney', sleeping_time=0)
TopTenTradableHolder.record_data(provider='eastmoney', sleeping_time=0)
StockInstitutionalInvestorHolder.record_data(provider='em', sleeping_time=0)
StockTopTenFreeHolder.record_data(provider='em', sleeping_time=0)
StockActorSummary.record_data(provider='em', sleeping_time=0)
Index.record_data(provider='exchange', sleeping_time=0)
IndexStock.record_data(provider='exchange', sleeping_time=0)
Index1dKdata.record_data(provider='em', sleeping_time=0)
