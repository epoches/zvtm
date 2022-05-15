# -*- coding: utf-8 -*-
from sqlalchemy.ext.declarative import declarative_base

from zvtm.contract.register import register_schema, register_entity
from zvtm.contract.schema import TradableEntity

FutureMetaBase = declarative_base()


@register_entity(entity_type="future")
class Future(FutureMetaBase, TradableEntity):
    __tablename__ = "future"


register_schema(providers=["em"], db_name="future_meta", schema_base=FutureMetaBase)

# the __all__ is generated
__all__ = ["Future"]
