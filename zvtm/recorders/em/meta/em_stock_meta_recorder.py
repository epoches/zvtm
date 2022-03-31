# -*- coding: utf-8 -*-

from zvtm.contract import Exchange
from zvtm.contract.api import df_to_db
from zvtm.contract.recorder import Recorder
from zvtm.domain import Stock
from zvtm.recorders.em import em_api


class EMStockRecorder(Recorder):
    provider = "em"
    data_schema = Stock

    def run(self):
        for exchange in [Exchange.sh, Exchange.sz]:
            df = em_api.get_tradable_list(entity_type="stock", exchange=exchange)
            self.logger.info(df)
            df_to_db(df=df, data_schema=self.data_schema, provider=self.provider, force_update=self.force_update)


if __name__ == "__main__":
    recorder = EMStockRecorder()
    recorder.run()
# the __all__ is generated
__all__ = ["EMStockRecorder"]
