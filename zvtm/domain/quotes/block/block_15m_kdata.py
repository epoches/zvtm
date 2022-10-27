# -*- coding: utf-8 -*-
# this file is generated by gen_kdata_schema function, dont't change it
from sqlalchemy.orm import declarative_base

from zvtm.contract.register import register_schema
from zvtm.domain.quotes import BlockKdataCommon

KdataBase = declarative_base()


class Block15mKdata(KdataBase, BlockKdataCommon):
    __tablename__ = "block_15m_kdata"


register_schema(providers=["em"], db_name="block_15m_kdata", schema_base=KdataBase, entity_type="block")

# the __all__ is generated
__all__ = ["Block15mKdata"]