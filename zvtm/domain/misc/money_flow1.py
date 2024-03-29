# -*- coding: utf-8 -*-
from sqlalchemy import Column, String, Float
from sqlalchemy.orm import declarative_base

from zvtm.contract import Mixin
from zvtm.contract.register import register_schema

MoneyFlowBase = declarative_base()



class StockMoneyFlow1(MoneyFlowBase, Mixin):
    __tablename__ = "stock_money_flow"

    code = Column(String(length=32))
    name = Column(String(length=32))

    #: 收盘价
    close = Column(Float)
    change_pct = Column(Float)
    turnover_rate = Column(Float)

    #: 净流入
    net_inflows = Column(Float)
    #: 净流入率
    net_inflow_rate = Column(Float)

    #: 主力=超大单+大单
    net_main_inflows = Column(Float)
    net_main_inflow_rate = Column(Float)
    #: 超大单
    net_huge_inflows = Column(Float)
    net_huge_inflow_rate = Column(Float)
    #: 大单
    net_big_inflows = Column(Float)
    net_big_inflow_rate = Column(Float)

    #: 中单
    net_medium_inflows = Column(Float)
    net_medium_inflow_rate = Column(Float)
    #: 小单
    net_small_inflows = Column(Float)
    net_small_inflow_rate = Column(Float)


register_schema(providers=["em"], db_name="money_flow1", schema_base=MoneyFlowBase, entity_type="stock")

# the __all__ is generated
__all__ = ["StockMoneyFlow1"]
