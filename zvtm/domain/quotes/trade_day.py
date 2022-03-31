# -*- coding: utf-8 -*-
from sqlalchemy.orm import declarative_base

from zvtm.contract import Mixin
from zvtm.contract.register import register_schema

TradeDayBase = declarative_base()


class StockTradeDay(TradeDayBase, Mixin):
    __tablename__ = "stock_trade_day"


register_schema(providers=["joinquant"], db_name="trade_day", schema_base=TradeDayBase)

# the __all__ is generated
__all__ = ["StockTradeDay"]
