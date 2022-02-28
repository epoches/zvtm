# -*- coding: utf-8 -*-
from typing import List

import pandas as pd

from zvtm.api.utils import to_report_period_type, value_to_pct
from zvtm.contract import ActorType
from zvtm.contract.api import df_to_db
from zvtm.contract.recorder import TimestampsDataRecorder
from zvtm.domain import Stock, ActorMeta
from zvtm.domain.actor.stock_actor import StockTopTenHolder, StockInstitutionalInvestorHolder
from zvtm.recorders.em.em_api import get_holder_report_dates, get_holders
from zvtm.utils import to_pd_timestamp, to_time_str


class EMStockTopTenRecorder(TimestampsDataRecorder):
    entity_provider = 'joinquant'
    entity_schema = Stock

    provider = 'em'
    data_schema = StockTopTenHolder

    def init_timestamps(self, entity_item) -> List[pd.Timestamp]:
        result = get_holder_report_dates(code=entity_item.code)
        if result:
            return [to_pd_timestamp(item['END_DATE']) for item in result]

    def on_finish_entity(self, entity):
        super().on_finish_entity(entity)
        super().on_finish_entity(entity)
        holders = StockTopTenHolder.query_data(entity_id=entity.id,
                                               filters=[StockTopTenHolder.holding_values == None],
                                               session=self.session, return_type='domain')
        for holder in holders:
            ii = StockInstitutionalInvestorHolder.query_data(entity_id=entity.id,
                                                             filters=[
                                                                 StockInstitutionalInvestorHolder.holding_values > 1,
                                                                 StockInstitutionalInvestorHolder.holding_ratio > 0.01,
                                                                 StockInstitutionalInvestorHolder.timestamp == holder.timestamp],
                                                             limit=1, return_type='domain')
            if ii:
                holder.holding_values = holder.holding_ratio * ii[0].holding_values / ii[0].holding_ratio
        self.session.commit()

    def record(self, entity, start, end, size, timestamps):
        for timestamp in timestamps:
            the_date = to_time_str(timestamp)
            result = get_holders(code=entity.code, end_date=the_date)
            if result:
                holders = []
                new_actors = []
                for item in result:
                    # 机构
                    if item['IS_HOLDORG'] == '1':
                        domains: List[ActorMeta] = ActorMeta.query_data(filters=[ActorMeta.code == item['HOLDER_CODE']],
                                                                        return_type='domain')
                        if not domains:
                            actor_type = ActorType.corporation.value
                            actor = ActorMeta(entity_id=f'{actor_type}_cn_{item["HOLDER_CODE"]}',
                                              id=f'{actor_type}_cn_{item["HOLDER_CODE"]}',
                                              entity_type=actor_type,
                                              exchange='cn',
                                              code=item["HOLDER_CODE"],
                                              name=item["HOLDER_NAME"])
                        else:
                            actor = domains[0]
                    else:
                        actor_type = ActorType.individual.value
                        actor = ActorMeta(entity_id=f'{actor_type}_cn_{item["HOLDER_NAME"]}',
                                          id=f'{actor_type}_cn_{item["HOLDER_NAME"]}',
                                          entity_type=actor_type,
                                          exchange='cn',
                                          code=item["HOLDER_NAME"],
                                          name=item["HOLDER_NAME"])
                        new_actors.append(actor.__dict__)
                    holder = {'id': f'{entity.entity_id}_{the_date}_{actor.entity_id}',
                              'entity_id': entity.entity_id,
                              'timestamp': timestamp,
                              'code': entity.code,
                              'name': entity.name,

                              'actor_id': actor.entity_id,
                              'actor_type': actor.entity_type,
                              'actor_code': actor.code,
                              'actor_name': actor.name,

                              'report_date': timestamp,
                              'report_period': to_report_period_type(timestamp),

                              'holding_numbers': item['HOLD_NUM'],
                              'holding_ratio': value_to_pct(item['HOLD_NUM_RATIO'], default=0)}
                    holders.append(holder)
                if holders:
                    df = pd.DataFrame.from_records(holders)
                    df_to_db(data_schema=self.data_schema, df=df, provider=self.provider,
                             force_update=True)
                if new_actors:
                    df = pd.DataFrame.from_records(new_actors)
                    df_to_db(data_schema=ActorMeta, df=df, provider=self.provider,
                             force_update=False)


if __name__ == '__main__':
    EMStockTopTenRecorder(codes=['000002']).run()
# the __all__ is generated
__all__ = ['EMStockTopTenRecorder']