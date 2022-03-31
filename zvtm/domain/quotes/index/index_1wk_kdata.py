# -*- coding: utf-8 -*-
# this file is generated by gen_kdata_schema function, dont't change it
from sqlalchemy.orm import declarative_base

from zvtm.contract.register import register_schema
from zvtm.domain.quotes import IndexKdataCommon

KdataBase = declarative_base()


class Index1wkKdata(KdataBase, IndexKdataCommon):
    __tablename__ = "index_1wk_kdata"


register_schema(providers=['em', 'sina','joinquant'], db_name='index_1wk_kdata', schema_base=KdataBase, entity_type='index')

# the __all__ is generated
__all__ = ["Index1wkKdata"]
