# -*- coding: utf-8 -*-
import pandas as pd

from zvtm.contract import IntervalLevel
from zvtm.contract.api import df_to_db
from zvtm.contract.recorder import FixedCycleDataRecorder
from zvtm.domain import IndexMoneyFlow, Index, StockMoneyFlow
from zvtm.utils import pd_is_not_null, to_time_str


class JoinquantIndexMoneyFlowRecorder(FixedCycleDataRecorder):
    entity_provider = 'exchange'
    entity_schema = Index

    provider = 'joinquant'
    data_schema = IndexMoneyFlow

    def __init__(self, force_update=True, sleeping_time=10, exchanges=None, entity_ids=None, code=None,codes=None, day_data=False,
                 entity_filters=None, ignore_failed=True, real_time=False, fix_duplicate_way='ignore',
                 start_timestamp=None, end_timestamp=None, level=IntervalLevel.LEVEL_1DAY, kdata_use_begin_time=False,
                 one_day_trading_minutes=24 * 60) -> None:
        # 上证指数，深证成指，创业板指，科创板
        support_codes = ['000001', '399001', '399006', '000688']
        if not codes:
            codes = support_codes
        else:
            codes = list(set(codes) & set(support_codes))
        super().__init__(force_update, sleeping_time, exchanges, entity_ids, code, codes, day_data, entity_filters,
                         ignore_failed, real_time, fix_duplicate_way, start_timestamp, end_timestamp, level,
                         kdata_use_begin_time, one_day_trading_minutes)

    def record(self, entity, start, end, size, timestamps):
        # 上证
        if entity.code == '000001':
            all_df = StockMoneyFlow.query_data(provider=self.provider, start_timestamp=start,
                                               filters=[StockMoneyFlow.entity_id.like('stock_sh%')])
        # 深证
        elif entity.code == '399001':
            all_df = StockMoneyFlow.query_data(provider=self.provider, start_timestamp=start,
                                               filters=[StockMoneyFlow.entity_id.like('stock_sz%')])
        # 创业板
        elif entity.code == '399006':
            all_df = StockMoneyFlow.query_data(provider=self.provider, start_timestamp=start,
                                               filters=[StockMoneyFlow.code.like('300%')])
        # 科创板
        elif entity.code == '000688':
            all_df = StockMoneyFlow.query_data(provider=self.provider, start_timestamp=start,
                                               filters=[StockMoneyFlow.code.like('688%')])

        if pd_is_not_null(all_df):
            g = all_df.groupby('timestamp')
            for timestamp, df in g:
                se = pd.Series({'id': "{}_{}".format(entity.id, to_time_str(timestamp)),
                                'entity_id': entity.id,
                                'timestamp': timestamp,
                                'code': entity.code,
                                'name': entity.name})
                for col in ['net_main_inflows', 'net_huge_inflows', 'net_big_inflows', 'net_medium_inflows',
                            'net_small_inflows']:
                    se[col] = df[col].sum()

                for col in ['net_main_inflow_rate', 'net_huge_inflow_rate', 'net_big_inflow_rate',
                            'net_medium_inflow_rate',
                            'net_small_inflow_rate']:
                    se[col] = df[col].sum() / len(df)

                index_df = se.to_frame().T

                self.logger.info(index_df)

                df_to_db(df=index_df, data_schema=self.data_schema, provider=self.provider,
                         force_update=self.force_update)

        return None


if __name__ == '__main__':
    JoinquantIndexMoneyFlowRecorder(start_timestamp='2020-12-01').run()
# the __all__ is generated
__all__ = ['JoinquantIndexMoneyFlowRecorder']