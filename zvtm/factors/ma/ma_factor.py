# -*- coding: utf-8 -*-
import argparse
from typing import List, Union, Type

import pandas as pd

from zvtm.contract import IntervalLevel, TradableEntity, AdjustType
from zvtm.contract.api import get_entities, get_schema_by_name
from zvtm.contract.factor import Accumulator
from zvtm.contract.factor import Transformer
from zvtm.domain import Stock
from zvtm.factors.algorithm import MaTransformer, MaAndVolumeTransformer
from zvtm.factors.technical_factor import TechnicalFactor
from zvtm.utils.time_utils import now_pd_timestamp


def get_ma_factor_schema(entity_type: str, level: Union[IntervalLevel, str] = IntervalLevel.LEVEL_1DAY):
    if type(level) == str:
        level = IntervalLevel(level)

    schema_str = "{}{}MaFactor".format(entity_type.capitalize(), level.value.capitalize())

    return get_schema_by_name(schema_str)


class MaFactor(TechnicalFactor):
    def __init__(
        self,
        entity_schema: Type[TradableEntity] = Stock,
        provider: str = None,
        entity_provider: str = None,
        entity_ids: List[str] = None,
        exchanges: List[str] = None,
        codes: List[str] = None,
        start_timestamp: Union[str, pd.Timestamp] = None,
        end_timestamp: Union[str, pd.Timestamp] = None,
        columns: List = None,
        filters: List = None,
        order: object = None,
        limit: int = None,
        level: Union[str, IntervalLevel] = IntervalLevel.LEVEL_1DAY,
        category_field: str = "entity_id",
        time_field: str = "timestamp",
        computing_window: int = None,
        keep_all_timestamp: bool = False,
        fill_method: str = "ffill",
        effective_number: int = None,
        need_persist: bool = False,
        only_compute_factor: bool = False,
        factor_name: str = None,
        clear_state: bool = False,
        only_load_factor: bool = False,
        adjust_type: Union[AdjustType, str] = None,
        windows=None,
    ) -> None:
        if need_persist:
            self.factor_schema = get_ma_factor_schema(entity_type=entity_schema.__name__, level=level)

        if not windows:
            windows = [5, 10, 34, 55, 89, 144, 120, 250]
        self.windows = windows
        transformer: Transformer = MaTransformer(windows=windows)

        super().__init__(
            entity_schema,
            provider,
            entity_provider,
            entity_ids,
            exchanges,
            codes,
            start_timestamp,
            end_timestamp,
            columns,
            filters,
            order,
            limit,
            level,
            category_field,
            time_field,
            computing_window,
            keep_all_timestamp,
            fill_method,
            effective_number,
            transformer,
            None,
            need_persist,
            only_compute_factor,
            factor_name,
            clear_state,
            only_load_factor,
            adjust_type,
        )


class CrossMaFactor(MaFactor):
    def compute_result(self):
        super().compute_result()
        cols = [f"ma{window}" for window in self.windows]
        s = self.factor_df[cols[0]] > self.factor_df[cols[1]]
        current_col = cols[1]
        for col in cols[2:]:
            s = s & (self.factor_df[current_col] > self.factor_df[col])
            current_col = col

        print(self.factor_df[s])
        self.result_df = s.to_frame(name="filter_result")


class VolumeUpMaFactor(TechnicalFactor):
    def __init__(
        self,
        entity_schema: Type[TradableEntity] = Stock,
        provider: str = None,
        entity_provider: str = None,
        entity_ids: List[str] = None,
        exchanges: List[str] = None,
        codes: List[str] = None,
        start_timestamp: Union[str, pd.Timestamp] = None,
        end_timestamp: Union[str, pd.Timestamp] = None,
        columns: List = None,
        filters: List = None,
        order: object = None,
        limit: int = None,
        level: Union[str, IntervalLevel] = IntervalLevel.LEVEL_1DAY,
        category_field: str = "entity_id",
        time_field: str = "timestamp",
        computing_window: int = None,
        keep_all_timestamp: bool = False,
        fill_method: str = "ffill",
        effective_number: int = None,
        accumulator: Accumulator = None,
        need_persist: bool = False,
        only_compute_factor: bool = False,
        factor_name: str = None,
        clear_state: bool = False,
        only_load_factor: bool = False,
        adjust_type: Union[AdjustType, str] = None,
        windows=None,
        vol_windows=None,
        turnover_threshold=300000000,
        turnover_rate_threshold=0.02,
        up_intervals=40,
        over_mode="and",
    ) -> None:
        if not windows:
            windows = [250]
        if not vol_windows:
            vol_windows = [30]

        self.windows = windows
        self.vol_windows = vol_windows
        self.turnover_threshold = turnover_threshold
        self.turnover_rate_threshold = turnover_rate_threshold
        self.up_intervals = up_intervals
        self.over_mode = over_mode

        transformer: Transformer = MaAndVolumeTransformer(windows=windows, vol_windows=vol_windows)

        super().__init__(
            entity_schema,
            provider,
            entity_provider,
            entity_ids,
            exchanges,
            codes,
            start_timestamp,
            end_timestamp,
            columns,
            filters,
            order,
            limit,
            level,
            category_field,
            time_field,
            computing_window,
            keep_all_timestamp,
            fill_method,
            effective_number,
            transformer,
            accumulator,
            need_persist,
            only_compute_factor,
            factor_name,
            clear_state,
            only_load_factor,
            adjust_type,
        )

    def compute_result(self):
        super().compute_result()

        # 价格刚上均线
        cols = [f"ma{window}" for window in self.windows]
        filter_up = (self.factor_df["close"] > self.factor_df[cols[0]]) & (
            self.factor_df["close"] < 1.1 * self.factor_df[cols[0]]
        )
        for col in cols[1:]:
            if self.over_mode == "and":
                filter_up = filter_up & (
                    (self.factor_df["close"] > self.factor_df[col])
                    & (self.factor_df["close"] < 1.1 * self.factor_df[col])
                )
            else:
                filter_up = filter_up | (
                    (self.factor_df["close"] > self.factor_df[col])
                    & (self.factor_df["close"] < 1.1 * self.factor_df[col])
                )
        # 放量
        if self.vol_windows:
            vol_cols = [f"vol_ma{window}" for window in self.vol_windows]
            filter_vol = self.factor_df["volume"] > 2 * self.factor_df[vol_cols[0]]
            for col in vol_cols[1:]:
                filter_vol = filter_vol & (self.factor_df["volume"] > 2 * self.factor_df[col])

        # 成交额，换手率过滤
        filter_turnover = (self.factor_df["turnover"] > self.turnover_threshold) & (
            self.factor_df["turnover_rate"] > self.turnover_rate_threshold
        )
        s = filter_up & filter_vol & filter_turnover

        # 突破后的时间周期 up_intervals
        s[s == False] = None
        s = s.groupby(level=0).fillna(method="ffill", limit=self.up_intervals)
        s[s.isna()] = False

        # 还在均线附近
        # 1)刚突破
        # 2)突破后，回调到附近
        filter_result = filter_up & s & filter_turnover

        self.result_df = filter_result.to_frame(name="filter_result")


class CrossMaVolumeFactor(VolumeUpMaFactor):
    def compute_result(self):
        # 均线多头排列
        cols = [f"ma{window}" for window in self.windows]
        filter_se = self.factor_df[cols[0]] > self.factor_df[cols[1]]
        current_col = cols[1]
        for col in cols[2:]:
            filter_se = filter_se & (self.factor_df[current_col] > self.factor_df[col])
            current_col = col

        filter_se = filter_se & (self.factor_df["turnover"] > self.turnover_threshold)

        print(self.factor_df[filter_se])
        self.result_df = filter_se.to_frame(name="filter_result")


if __name__ == "__main__":
    print("start")
    parser = argparse.ArgumentParser()
    parser.add_argument("--level", help="trading level", default="1d", choices=[item.value for item in IntervalLevel])
    parser.add_argument("--start", help="start code", default="000001")
    parser.add_argument("--end", help="end code", default="000005")

    args = parser.parse_args()

    level = IntervalLevel(args.level)
    start = args.start
    end = args.end

    entities = get_entities(
        provider="eastmoney",
        entity_type="stock",
        columns=[Stock.entity_id, Stock.code],
        filters=[Stock.code >= start, Stock.code < end],
    )

    codes = entities.index.to_list()

    factor = VolumeUpMaFactor(
        entity_ids=["stock_sz_000338"],
        start_timestamp="2020-01-01",
        end_timestamp=now_pd_timestamp(),
        level=level,
        need_persist=False,
    )
    print(factor.result_df)
# the __all__ is generated
__all__ = ["get_ma_factor_schema", "MaFactor", "CrossMaFactor", "VolumeUpMaFactor", "CrossMaVolumeFactor"]
