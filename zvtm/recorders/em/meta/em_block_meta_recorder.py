# -*- coding: utf-8 -*-

from zvtm.contract.api import df_to_db
from zvtm.contract.recorder import Recorder
from zvtm.domain import Block, BlockCategory,BlockStock
from zvtm.recorders.em import em_api


class EMBlockRecorder(Recorder):
    provider = "em"
    data_schema = Block

    def run(self):
        for block_category in [BlockCategory.concept, BlockCategory.industry]:
            df = em_api.get_tradable_list(entity_type="block", block_category=block_category)
            self.logger.info(df)
            df_to_db(df=df, data_schema=self.data_schema, provider=self.provider, force_update=self.force_update)


class EMBlockStockRecorder(Recorder):
    provider = "em"
    data_schema = BlockStock

    def run(self):
        for block_category in [BlockCategory.concept, BlockCategory.industry]:
            df = em_api.get_tradable_list(entity_type="block", block_category=block_category)
            self.logger.info(df)
            df_to_db(df=df, data_schema=self.data_schema, provider=self.provider, force_update=self.force_update)

if __name__ == "__main__":
    recorder = EMBlockRecorder()
    recorder.run()
    # stockrecorder = EMBlockStockRecorder()
    # stockrecorder.run()
# the __all__ is generated
__all__ = ["EMBlockRecorder","EMBlockStockRecorder"]
