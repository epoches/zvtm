# -*- coding: utf-8 -*-

from sqlalchemy import Column, String, DateTime, BigInteger, Float
from sqlalchemy.orm import declarative_base

from zvtm.contract import TradableEntity
from zvtm.contract.register import register_schema, register_entity

StockMetaBase = declarative_base()


# 个股
@register_entity(entity_type='stock')
class Stock(StockMetaBase, TradableEntity):
    __tablename__ = 'stock'


# 个股详情
class StockDetail(StockMetaBase, TradableEntity):
    __tablename__ = 'stock_detail'

    # 所属行业
    industries = Column(String(length=128))
    # 行业指数
    industry_indices = Column(String(length=128))
    # 所属板块
    concept_indices = Column(String(length=128))
    # 所属区域
    area_indices = Column(String(length=128))

    # 成立日期
    date_of_establishment = Column(DateTime)
    # 公司简介
    profile = Column(String(length=4096))
    # 主营业务
    main_business = Column(String(length=512))
    # 发行量(股)
    issues = Column(BigInteger)
    # 发行价格
    price = Column(Float)
    # 募资净额(元)
    raising_fund = Column(Float)
    # 发行市盈率
    issue_pe = Column(Float)
    # 网上中签率
    net_winning_rate = Column(Float)


register_schema(providers=['exchange', 'joinquant', 'eastmoney', 'em'], db_name='stock_meta',
                schema_base=StockMetaBase)
# the __all__ is generated
__all__ = ['Stock', 'StockDetail']
