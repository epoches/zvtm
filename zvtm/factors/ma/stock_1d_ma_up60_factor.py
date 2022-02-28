# -*- coding: utf-8 -*-
from typing import List, Union

import pandas as pd

from zvtm.contract import AdjustType
from zvtm.contract import IntervalLevel, TradableEntity
from zvtm.contract.drawer import Drawer
from zvtm.contract.factor import Accumulator
from zvtm.contract.factor import Transformer
from zvtm.contract.reader import DataReader
from zvtm.domain import Stock, Stock1dKdata
from zvtm.factors.technical_factor import TechnicalFactor
from zvtm.utils.time_utils import now_pd_timestamp


class MaUpTransformer(Transformer):
    def __init__(self, window=20) -> None:
        super().__init__()
        self.window = window

    def transform(self, input_df) -> pd.DataFrame:
        top_df = input_df['close'].groupby(level=0).rolling(window=self.window, min_periods=self.window).mean()
        top_df = top_df.reset_index(level=0, drop=True)
        input_df['mean'] = top_df


        return input_df


class MaUpFactor(TechnicalFactor):
    def __init__(self, entity_schema: TradableEntity = Stock, provider: str = None, entity_provider: str = None,
                 entity_ids: List[str] = None, exchanges: List[str] = None, codes: List[str] = None,
                 start_timestamp: Union[str, pd.Timestamp] = None, end_timestamp: Union[str, pd.Timestamp] = None,
                 columns: List = ['id', 'entity_id', 'timestamp', 'level', 'open', 'close', 'high', 'low'],
                 filters: List = None, order: object = None, limit: int = None,
                 level: Union[str, IntervalLevel] = IntervalLevel.LEVEL_1DAY, category_field: str = 'entity_id',
                 time_field: str = 'timestamp', computing_window: int = None, keep_all_timestamp: bool = False,
                 fill_method: str = 'ffill', effective_number: int = None,
                 accumulator: Accumulator = None, need_persist: bool = False, only_compute_factor: bool = False,
                 factor_name: str = None, clear_state: bool = False, only_load_factor: bool = False,
                 adjust_type: Union[AdjustType, str] = None, window=30) -> None:
        self.adjust_type = adjust_type

        transformer = MaUpTransformer(window=window)

        super().__init__(entity_schema, provider, entity_provider, entity_ids, exchanges, codes, start_timestamp,
                         end_timestamp, columns, filters, order, limit, level, category_field, time_field,
                         computing_window, keep_all_timestamp, fill_method, effective_number, transformer, accumulator,
                         need_persist, only_compute_factor, factor_name, clear_state, only_load_factor, adjust_type)


if __name__ == '__main__':
    factor = MaUpFactor(codes=['601099'], start_timestamp='2021-01-01',
                             end_timestamp=now_pd_timestamp(),
                             level=IntervalLevel.LEVEL_1DAY, window=60)
    print(factor.factor_df)

    Stock1dKdata.record_data(code='601099', provider='joinquant',start_timestamp='2021-01-01',
                             end_timestamp=now_pd_timestamp())
    data_reader1 = Stock1dKdata.query_data(code='601099', provider='joinquant',start_timestamp='2021-01-01',
                             end_timestamp=now_pd_timestamp())


    drawer = Drawer(main_df=data_reader1, factor_df_list=[factor.factor_df[['mean']]])
    drawer.draw_kline(show=True)
# the __all__ is generated
__all__ = ['MaUpTransformer', 'MaUpFactor']