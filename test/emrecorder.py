# -*- coding: utf-8 -*-
import logging
import time

from apscheduler.schedulers.background import BackgroundScheduler

from zvtm import init_log, zvt_config
from zvtm.domain import *
from zvtm.informer.informer import EmailInformer

#Stock.record_data(provider='eastmoney', sleeping_time=5)
#StockDetail.record_data(provider='eastmoney', sleeping_time=5)
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
Stock1dHfqKdata.record_data(provider='em', sleeping_time=0,codes=['002772', '000017', '603042', '603559', '001202', '603191', '600353', '000878', '001267', '002835', '605060', '000815', '001236', '603496', '000948', '000798', '002843', '603931', '000670', '603726', '003005', '002528', '603283', '600707', '002559', '000850', '603985', '002576', '600082', '002487', '600536', '002952', '000540', '003043', '002943', '003023', '000034', '603138', '002180', '002474', '301168', '603667', '002531', '603786', '601567', '002101', '600933', '603507', '002472', '003033', '000066', '603912', '002837', '603338', '603636', '601958', '002777', '002997', '002197', '300658'])
Block1dKdata.record_data(provider='em', sleeping_time=0,codes=['BK0433', 'BK0429', 'BK0448', 'BK0736', 'BK0422', 'BK0457', 'BK0459', 'BK0478', 'BK0425', 'BK0735', 'BK0545', 'BK0470', 'BK0447', 'BK0737', 'BK0433', 'BK0545', 'BK1039', 'BK1036', 'BK0456', 'BK0737', 'BK0910', 'BK1038', 'BK0545', 'BK0436', 'BK0545', 'BK1030', 'BK0451', 'BK1032', 'BK0447', 'BK1038', 'BK0451', 'BK0545', 'BK0545', 'BK0735', 'BK0737', 'BK0735', 'BK0447', 'BK1031', 'BK0545', 'BK1032', 'BK0481', 'BK0457', 'BK0481', 'BK0481', 'BK1032', 'BK0481', 'BK0481', 'BK0735', 'BK0910', 'BK0910', 'BK0739', 'BK0447', 'BK1027', 'BK0447', 'BK0481', 'BK0735', 'BK1035'])