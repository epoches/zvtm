# -*- coding: utf-8 -*-
import pandas as pd
# from jqdatapy import get_token, get_money_flow
import requests
import pandas as pd
from jqdatasdk import auth,get_money_flow
from zvtm import zvt_config
from zvtm.api.kdata import generate_kdata_id
from zvtm.contract import IntervalLevel
from zvtm.contract.api import df_to_db
from zvtm.contract.recorder import FixedCycleDataRecorder
from zvtm.domain import StockMoneyFlow1, Stock
from zvtm.recorders.joinquant.common import to_jq_entity_id
from zvtm.recorders.joinquant.misc.jq_index_money_flow_recorder import JoinquantIndexMoneyFlowRecorder
from zvtm.utils import pd_is_not_null, to_time_str
from zvtm.utils.time_utils import TIME_FORMAT_DAY
import datetime

class EmStockMoneyFlowRecorder(FixedCycleDataRecorder):
    entity_provider = "em"
    entity_schema = Stock

    provider = "em"
    data_schema = StockMoneyFlow1

    def __init__(
        self,
        force_update=True,
        sleeping_time=10,
        exchanges=None,
        entity_id=None,
        entity_ids=None,
        code=None,
        codes=None,
        day_data=False,
        entity_filters=None,
        ignore_failed=True,
        real_time=False,
        fix_duplicate_way="ignore",
        start_timestamp=None,
        end_timestamp=None,
        level=IntervalLevel.LEVEL_1DAY,
        kdata_use_begin_time=False,
        one_day_trading_minutes=24 * 60,
        compute_index_money_flow=False,
    ) -> None:
        super().__init__(
            force_update,
            sleeping_time,
            exchanges,
            entity_id,
            entity_ids,
            code,
            codes,
            day_data,
            entity_filters,
            ignore_failed,
            real_time,
            fix_duplicate_way,
            start_timestamp,
            end_timestamp,
            level,
            kdata_use_begin_time,
            one_day_trading_minutes,
        )
        """
          东方财富网-沪深京 A 股-实时行情
          https://quote.eastmoney.com/center/gridlist.html#hs_a_board
          :return: 实时行情
          :rtype: pandas.DataFrame
          """
        url = "http://82.push2.eastmoney.com/api/qt/clist/get"
        params = {
            "pn": "1",
            "pz": "50000",
            "po": "1",
            "np": "1",
            "ut": "bd1d9ddb04089700cf9c27f6f7426281",
            "fltt": "2",
            "invt": "2",
            "fid": "f3",
            "fs": "m:0 t:6,m:0 t:80,m:1 t:2,m:1 t:23",
            "fields": "f2,f3,f8,f12,f13,f14,f62,f64,f69,f70,f75,f76,f81,f82,f87,f124",
            "_": "1623833739532",
        }
        # close    f2
        # change_pct    f3
        # turnover_rate    f8
        # f12: 股票代码    #
        # f13: 市场    #
        # f14: 股票名称
        # net_inflows    f62
        # net_inflow_rate
        # net_main_inflows    f64 + f70
        # net_main_inflow_rate    f69 + f75
        # net_huge_inflows    f64
        # net_huge_inflow_rate    f69
        # net_big_inflows    f70
        # net_big_inflow_rate    f75
        # net_medium_inflows    f76
        # net_medium_inflow_rate    f81
        # net_small_inflows    f82
        # net_small_inflow_rate    f87

        r = requests.get(url, params=params)
        data_json = r.json()
        if not data_json["data"]["diff"]:
            return pd.DataFrame()
        temp_df = pd.DataFrame(data_json["data"]["diff"])
        temp_df.columns = [
            "close",
            "change_pct",
            "turnover_rate",
            "code",
            "entity",
            "name",
            "net_inflows",
            "net_huge_inflows",
            "net_huge_inflow_rate",
            "net_big_inflows",
            "net_big_inflow_rate",
            "net_medium_inflows",
            "net_medium_inflow_rate",
            "net_small_inflows",
            "net_small_inflow_rate",
            "timestamp",
        ]
        # temp_df.reset_index(inplace=True)
        # temp_df["index"] = temp_df.index + 1
        # temp_df.rename(columns={"index": "序号"}, inplace=True)
        temp_df = temp_df[
            [
                "close",
                "change_pct",
                "turnover_rate",
                "code",
                "entity",
                "name",
                "net_inflows",
                "net_huge_inflows",
                "net_huge_inflow_rate",
                "net_big_inflows",
                "net_big_inflow_rate",
                "net_medium_inflows",
                "net_medium_inflow_rate",
                "net_small_inflows",
                "net_small_inflow_rate",
                "timestamp",
            ]
        ]
        temp_df["entity"] = temp_df["entity"].apply(lambda x: 'sz' if x == 0 else 'sh')
        dt = pd.to_datetime(temp_df["timestamp"], unit='s', errors="coerce")[0].replace(hour=0, minute=0, second=0)
        temp_df["timestamp"] = dt
        temp_df["close"] = pd.to_numeric(temp_df["close"], errors="coerce")
        temp_df["change_pct"] = pd.to_numeric(temp_df["change_pct"], errors="coerce")
        temp_df["turnover_rate"] = pd.to_numeric(temp_df["turnover_rate"], errors="coerce")
        temp_df["net_inflows"] = pd.to_numeric(temp_df["net_inflows"], errors="coerce")
        temp_df["net_huge_inflows"] = pd.to_numeric(temp_df["net_huge_inflows"], errors="coerce")
        temp_df["net_huge_inflow_rate"] = pd.to_numeric(temp_df["net_huge_inflow_rate"], errors="coerce")
        temp_df["net_big_inflows"] = pd.to_numeric(temp_df["net_big_inflows"], errors="coerce")
        temp_df["net_medium_inflows"] = pd.to_numeric(temp_df["net_medium_inflows"], errors="coerce")
        temp_df["net_medium_inflow_rate"] = pd.to_numeric(temp_df["net_medium_inflow_rate"], errors="coerce")
        temp_df["net_small_inflows"] = pd.to_numeric(temp_df["net_small_inflows"], errors="coerce")
        temp_df["net_small_inflow_rate"] = pd.to_numeric(temp_df["net_small_inflow_rate"], errors="coerce")
        self.df = temp_df

    def generate_domain_id(self, entity, original_data):
        return generate_kdata_id(entity_id=entity.id, timestamp=original_data["timestamp"], level=self.level)

    def on_finish(self):
        pass
        # 根据 个股资金流 计算 大盘资金流
        # if self.compute_index_money_flow:
        #     JoinquantIndexMoneyFlowRecorder().run()

    def record(self, entity, start, end, size, timestamps):
        temp = self.df
        df = temp[temp['code'] == entity.code]

        if pd_is_not_null(df):
            df_to_db(df=df, data_schema=self.data_schema, provider=self.provider, force_update=self.force_update)
        else:
            self.logger.info(f"no kdata for {entity.id}")

        return None


if __name__ == "__main__":
    EmStockMoneyFlowRecorder().run()
# the __all__ is generated
__all__ = ["EmStockMoneyFlowRecorder"]
