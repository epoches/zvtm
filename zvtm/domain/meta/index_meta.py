# -*- coding: utf-8 -*-

from sqlalchemy import Column, String, Float
from sqlalchemy.orm import declarative_base

from zvtm.contract import Portfolio, PortfolioStockHistory
from zvtm.contract.register import register_schema, register_entity

IndexMetaBase = declarative_base()


# 指数
@register_entity(entity_type='index')
class Index(IndexMetaBase, Portfolio):
    __tablename__ = 'indexs'

    # 发布商
    publisher = Column(String(length=64))
    # 类别
    # see IndexCategory
    category = Column(String(length=64))
    # 基准点数
    base_point = Column(Float)


class IndexStock(IndexMetaBase, PortfolioStockHistory):
    __tablename__ = 'index_stock'


register_schema(providers=['exchange','joinquant'], db_name='index_meta', schema_base=IndexMetaBase)
# the __all__ is generated
__all__ = ['Index', 'IndexStock']