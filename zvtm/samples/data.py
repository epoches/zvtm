# -*- coding: utf-8 -*-
from zvtm.contract import IntervalLevel
from zvtm.factors.algorithm import MaTransformer, MacdTransformer
from zvtm.factors.ma.ma_factor import CrossMaFactor
from zvtm.domain import *
def getdata():
    Stock.record_data(provider='joinquant')
    # Stock.record_data(provider='eastmoney')
    # FinanceFactor.record_data(provider='joinquant')
    # BalanceSheet.record_data(provider='joinquant')
    # IncomeStatement.record_data(provider='joinquant')
    # CashFlowStatement.record_data(provider='joinquant')