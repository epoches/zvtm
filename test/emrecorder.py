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
Stock1dHfqKdata.record_data(provider='em', sleeping_time=0,codes=['002196', '603658', '000899', '002836', '603032', '002976', '300832', '002631', '603122', '000856', '000668', '603815', '002808', '600272', '002175', '603839', '605365', '600791', '002607', '002659', '601975', '601579', '001236', '600693', '002329', '001231', '600763', '605369', '688016', '000518', '688389', '300396', '002622', '000659', '603351', '002871', '002795', '002640', '000009', '000819', '688656', '000503', '000661', '002118', '600276', '300463', '002362', '603108', '600780', '688029', '603259', '600686', '002675', '301159', '603987', '002332', '688617', '002551', '603882', '002173', '600280', '002821', '300633', '600079', '605186', '605300', '002980', '002571', '002897', '600586', '601137', '002468', '600636', '603688', '002020', '600732', '688787'])
Block1dKdata.record_data(provider='em', sleeping_time=0,codes=['BK1030', 'BK1041', 'BK0428', 'BK0470', 'BK0421', 'BK1037', 'BK1041', 'BK0476', 'BK1041', 'BK0910', 'BK0451', 'BK0425', 'BK1038', 'BK1042', 'BK0458', 'BK0436', 'BK0456', 'BK0451', 'BK0740', 'BK0740', 'BK0450', 'BK0477', 'BK0738', 'BK0482', 'BK0438', 'BK0730', 'BK0727', 'BK1041', 'BK1041', 'BK1044', 'BK1041', 'BK1041', 'BK0457', 'BK0733', 'BK0465', 'BK0545', 'BK0545', 'BK0484', 'BK0539', 'BK0464', 'BK1041', 'BK0737', 'BK1044', 'BK1040', 'BK0465', 'BK1041', 'BK0737', 'BK0727', 'BK0428', 'BK1041', 'BK0727', 'BK1029', 'BK0465', 'BK0737', 'BK1041', 'BK0465', 'BK1041', 'BK1041', 'BK0727', 'BK0727', 'BK0482', 'BK0727', 'BK1041', 'BK0465', 'BK0727', 'BK0438', 'BK0458', 'BK0440', 'BK0448', 'BK0546', 'BK0478', 'BK0422', 'BK0740', 'BK1020', 'BK0465', 'BK1031', 'BK0447'])