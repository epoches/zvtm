# -*- coding: utf-8 -*-
# this file is generated by gen_kdata_schema function, dont't change it
from sqlalchemy.orm import declarative_base

from zvtm.contract.register import register_schema
from zvtm.domain.quotes import BlockKdataCommon

KdataBase = declarative_base()


class Block1monKdata(KdataBase, BlockKdataCommon):
    __tablename__ = 'block_1mon_kdata'


register_schema(providers=['em'], db_name='block_1mon_kdata', schema_base=KdataBase, entity_type='block')

# the __all__ is generated
__all__ = ['Block1monKdata']