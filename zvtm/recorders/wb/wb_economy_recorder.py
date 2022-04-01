# -*- coding: utf-8 -*-

from zvtm.contract.api import df_to_db
from zvtm.contract.recorder import FixedCycleDataRecorder
from zvtm.domain import Country, Economy
from zvtm.recorders.wb import wb_api
from zvtm.utils import current_date


class WBEconomyRecorder(FixedCycleDataRecorder):
    entity_schema = Country
    data_schema = Economy
    entity_provider = "wb"
    provider = "wb"

    def record(self, entity, start, end, size, timestamps):
        date = None
        if start:
            date = f"{start.year}:{current_date().year}"
        df = wb_api.get_economy_data(entity_id=entity.id, date=date)
        df["name"] = entity.name
        df_to_db(df=df, data_schema=self.data_schema, provider=self.provider, force_update=self.force_update)


if __name__ == "__main__":
    entity_ids = ["country_galaxy_CN", "country_galaxy_US"]
    r = WBEconomyRecorder(entity_ids=entity_ids)
    r.run()
# the __all__ is generated
__all__ = ["WBEconomyRecorder"]
