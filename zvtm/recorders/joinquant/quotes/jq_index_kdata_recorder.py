# -*- coding: utf-8 -*-
import argparse

import pandas as pd
from jqdatapy.api import get_token, get_bars

from zvtm import init_log, zvt_config
from zvtm.api.kdata import generate_kdata_id, get_kdata_schema, get_kdata
from zvtm.contract import IntervalLevel
from zvtm.contract.api import df_to_db
from zvtm.contract.recorder import FixedCycleDataRecorder
from zvtm.domain import Index, IndexKdataCommon
from zvtm.recorders.joinquant.common import to_jq_trading_level, to_jq_entity_id
from zvtm.utils.pd_utils import pd_is_not_null
from zvtm.utils.time_utils import to_time_str, TIME_FORMAT_DAY, TIME_FORMAT_ISO8601


class JqChinaIndexKdataRecorder(FixedCycleDataRecorder):
    entity_provider = 'joinquant'
    entity_schema = Index

    # 数据来自jq
    provider = 'joinquant'

    # 只是为了把recorder注册到data_schema
    data_schema = IndexKdataCommon

    def __init__(self,
                 force_update=True,
                 sleeping_time=10,
                 exchanges=None,
                 entity_ids=None,
                 code=None,
                 codes=None,
                 day_data=False,
                 entity_filters=None,
                 ignore_failed=True,
                 real_time=False,
                 fix_duplicate_way='ignore',
                 start_timestamp=None,
                 end_timestamp=None,
                 level=IntervalLevel.LEVEL_1DAY,
                 kdata_use_begin_time=False,
                 one_day_trading_minutes=24 * 60) -> None:
        level = IntervalLevel(level)
        self.data_schema = get_kdata_schema(entity_type='index', level=level)
        self.jq_trading_level = to_jq_trading_level(level)
        get_token(zvt_config['jq_username'], zvt_config['jq_password'], force=True)
        super().__init__(force_update, sleeping_time, exchanges, entity_ids, code, codes, day_data, entity_filters,
                         ignore_failed, real_time, fix_duplicate_way, start_timestamp, end_timestamp, level,
                         kdata_use_begin_time, one_day_trading_minutes)

    def init_entities(self):
        super().init_entities()
        # ignore no data index
        self.entities = [entity for entity in self.entities if
                         entity.code not in ['310001', '310002', '310003', '310004']]

    def generate_domain_id(self, entity, original_data):
        return generate_kdata_id(entity_id=entity.id, timestamp=original_data['timestamp'], level=self.level)

    def record(self, entity, start, end, size, timestamps):
        if not self.end_timestamp:
            df = get_bars(to_jq_entity_id(entity),
                          count=size,
                          unit=self.jq_trading_level,
                          # fields=['date', 'open', 'close', 'low', 'high', 'volume', 'money']
                          )
        else:
            end_timestamp = to_time_str(self.end_timestamp)
            df = get_bars(to_jq_entity_id(entity),
                          count=size,
                          unit=self.jq_trading_level,
                          # fields=['date', 'open', 'close', 'low', 'high', 'volume', 'money'],
                          end_date=end_timestamp)
        if pd_is_not_null(df):
            df['name'] = entity.name
            df.rename(columns={'money': 'turnover', 'date': 'timestamp'}, inplace=True)

            df['entity_id'] = entity.id
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df['provider'] = 'joinquant'
            df['level'] = self.level.value
            df['code'] = entity.code

            def generate_kdata_id(se):
                if self.level >= IntervalLevel.LEVEL_1DAY:
                    return "{}_{}".format(se['entity_id'], to_time_str(se['timestamp'], fmt=TIME_FORMAT_DAY))
                else:
                    return "{}_{}".format(se['entity_id'], to_time_str(se['timestamp'], fmt=TIME_FORMAT_ISO8601))

            df['id'] = df[['entity_id', 'timestamp']].apply(generate_kdata_id, axis=1)

            df = df.drop_duplicates(subset='id', keep='last')

            df_to_db(df=df, data_schema=self.data_schema, provider=self.provider, force_update=self.force_update)

        return None


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--level', help='trading level', default='1d', choices=[item.value for item in IntervalLevel])
    parser.add_argument('--codes', help='codes', default=['000001'], nargs='+')

    args = parser.parse_args()

    level = IntervalLevel(args.level)
    codes = args.codes

    init_log('jq_china_stock_{}_kdata.log'.format(args.level))
    JqChinaIndexKdataRecorder(level=level, sleeping_time=0, codes=codes, real_time=False).run()

    print(get_kdata(entity_id='index_sh_000001', limit=10))
# the __all__ is generated
__all__ = ['JqChinaIndexKdataRecorder']