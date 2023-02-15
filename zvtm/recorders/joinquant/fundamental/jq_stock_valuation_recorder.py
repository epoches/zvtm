# -*- coding: utf-8 -*-

import pandas as pd
# from jqdatapy.api import get_fundamentals
from jqdatasdk import auth
from zvtm import zvt_config
from jqdatasdk import get_fundamentals,query,valuation,income
from pandas._libs.tslibs.timedeltas import Timedelta

from zvtm.contract.api import df_to_db
from zvtm.contract.recorder import TimeSeriesDataRecorder
from zvtm.domain import Stock, StockValuation, Etf,StockDetail
from zvtm.recorders.joinquant.common import to_jq_entity_id
from zvtm.utils.time_utils import now_pd_timestamp, to_time_str, to_pd_timestamp


class JqChinaStockValuationRecorder(TimeSeriesDataRecorder):
    entity_provider = "joinquant"
    entity_schema = Stock

    # 数据来自jq
    provider = 'joinquant'

    data_schema = StockValuation

    def init_entities(self):
        super().init_entities()
        # 过滤掉退市的
        self.entities = [entity for entity in self.entities if
                         (entity.end_date is None) or (entity.end_date > now_pd_timestamp())]
        auth(zvt_config['jq_username'], zvt_config['jq_password'])

    def record(self, entity, start, end, size, timestamps):
        start = max(start, to_pd_timestamp("2005-01-01"))
        end = min(now_pd_timestamp(), start + Timedelta(days=500))

        count: Timedelta = end - start

        # df = get_fundamentals_continuously(q, end_date=now_time_str(), count=count.days + 1, panel=False)
        # df = get_fundamentals(
        #     table="valuation", code=to_jq_entity_id(entity), date=to_time_str(end), count=min(count.days, 500)
        # )
        df = get_fundamentals(query(valuation, income).filter(valuation.code == (to_jq_entity_id(entity))), date=to_time_str(start))
        df["entity_id"] = entity.id
        df["timestamp"] = pd.to_datetime(df["day"])
        df["code"] = entity.code
        df["name"] = entity.name
        df["id"] = df["timestamp"].apply(lambda x: "{}_{}".format(entity.id, to_time_str(x)))
        df = df.rename(
            {"pe_ratio_lyr": "pe", "pe_ratio": "pe_ttm", "pb_ratio": "pb", "ps_ratio": "ps", "pcf_ratio": "pcf"},
            axis="columns",
        )

        df["market_cap"] = df["market_cap"] * 100000000
        df["circulating_market_cap"] = df["circulating_market_cap"] * 100000000
        df["capitalization"] = df["capitalization"] * 10000
        df["circulating_cap"] = df["circulating_cap"] * 10000
        df["turnover_ratio"] = df["turnover_ratio"] * 0.01
        df_to_db(df=df, data_schema=self.data_schema, provider=self.provider, force_update=self.force_update)

        return None


if __name__ == "__main__":
    # 上证50
    df = Etf.get_stocks(code="510050")
    stocks = df.stock_id.tolist()
    print(stocks)
    print(len(stocks))

    JqChinaStockValuationRecorder(entity_ids=["stock_sz_300999"], force_update=True).run()
# the __all__ is generated
__all__ = ["JqChinaStockValuationRecorder"]
