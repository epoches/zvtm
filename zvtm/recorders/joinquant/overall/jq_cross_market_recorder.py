from jqdatapy.api import run_query

from zvtm.contract.recorder import TimeSeriesDataRecorder
from zvtm.domain import Index, CrossMarketSummary
from zvtm.utils.time_utils import to_time_str
from zvtm.utils.utils import multiple_number


class CrossMarketSummaryRecorder(TimeSeriesDataRecorder):
    entity_provider = "joinquant"
    entity_schema = Index

    provider = "joinquant"
    data_schema = CrossMarketSummary

    def __init__(self, force_update=False, sleeping_time=5, real_time=False, fix_duplicate_way="add") -> None:

        # 聚宽编码
        # 市场通编码	市场通名称
        # 310001	沪股通
        # 310002	深股通
        # 310003	港股通（沪）
        # 310004	港股通（深）

        codes = ["310001", "310002", "310003", "310004"]
        super().__init__(
            force_update,
            sleeping_time,
            ["cn"],
            None,
            codes=codes,
            day_data=True,
            real_time=real_time,
            fix_duplicate_way=fix_duplicate_way,
        )

    def init_entities(self):
        super().init_entities()

    def record(self, entity, start, end, size, timestamps):
        df = run_query(table="finance.STK_ML_QUOTA", conditions=f"link_id#=#{entity.code}&day#>=#{to_time_str(start)}")
        print(df)

        json_results = []

        for item in df.to_dict(orient="records"):
            result = {
                "provider": self.provider,
                "timestamp": item["day"],
                "name": entity.name,
                "buy_amount": multiple_number(item["buy_amount"], 100000000),
                "buy_volume": item["buy_volume"],
                "sell_amount": multiple_number(item["sell_amount"], 100000000),
                "sell_volume": item["sell_volume"],
                "quota_daily": multiple_number(item["quota_daily"], 100000000),
                "quota_daily_balance": multiple_number(item["quota_daily_balance"], 100000000),
            }

            json_results.append(result)

        if len(json_results) < 100:
            self.one_shot = True

        return json_results

    def get_data_map(self):
        return None


if __name__ == "__main__":
    CrossMarketSummaryRecorder().run()
# the __all__ is generated
__all__ = ["CrossMarketSummaryRecorder"]
