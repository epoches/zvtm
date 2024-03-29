# -*- coding: utf-8 -*-
from sqlalchemy import Column, String, Float
from sqlalchemy.orm import declarative_base

from zvtm.contract import Mixin
from zvtm.contract.register import register_schema

ValuationBase = declarative_base()


class StockValuation1(ValuationBase, Mixin):
    __tablename__ = "stock_valuation"

    code = Column(String(length=32))
    name = Column(String(length=32))
    # f38	总股本
    # f39	流通股
    #: 总股本(股)
    capitalization = Column(Float)
    #: 公司已发行的普通股股份总数(包含A股，B股和H股的总股本)
    circulating_cap = Column(Float)
    #: 市值
    market_cap = Column(Float)
    #: 流通市值
    circulating_market_cap = Column(Float)
    #: 量比
    lb = Column(Float)
    #: 静态pe
    pe = Column(Float)
    #: 动态pe
    pe_ttm = Column(Float)
    #: 动态pe 市盈率 动 总市值/预估全年净利润
    pe_ttm1 = Column(Float)
    #: 市净率
    pb = Column(Float)
    #: 市销率
    ps = Column(Float)
    #: 市现率
    pcf = Column(Float)



register_schema(providers=["em"], db_name="valuation1", schema_base=ValuationBase, entity_type="stock")

# the __all__ is generated
__all__ = ["StockValuation1"]
