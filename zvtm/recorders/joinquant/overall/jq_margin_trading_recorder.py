from jqdatapy.api import run_query

from zvtm.contract.recorder import TimeSeriesDataRecorder
from zvtm.domain import Index, MarginTradingSummary
from zvtm.utils.time_utils import to_time_str

# 聚宽编码
# XSHG-上海证券交易所
# XSHE-深圳证券交易所

code_map_jq = {
    '000001': 'XSHG',
    '399106': 'XSHE'
}


class MarginTradingSummaryRecorder(TimeSeriesDataRecorder):
    entity_provider = 'exchange'
    entity_schema = Index

    provider = 'joinquant'
    data_schema = MarginTradingSummary

    def __init__(self, force_update=False, sleeping_time=5, exchanges=None, entity_ids=None, day_data=False,
                 entity_filters=None, ignore_failed=True, real_time=False, fix_duplicate_way='add',
                 start_timestamp=None, end_timestamp=None) -> None:
        # 上海A股,深圳市场
        codes = ['000001', '399106']
        super().__init__(force_update, sleeping_time, exchanges, entity_ids, codes=codes, day_data=day_data,
                         entity_filters=entity_filters, ignore_failed=ignore_failed, real_time=real_time,
                         fix_duplicate_way=fix_duplicate_way, start_timestamp=start_timestamp,
                         end_timestamp=end_timestamp)

    def record(self, entity, start, end, size, timestamps):
        jq_code = code_map_jq.get(entity.code)

        df = run_query(table='finance.STK_MT_TOTAL',
                       conditions=f'exchange_code#=#{jq_code}&date#>=#{to_time_str(start)}', parse_dates=['date'])
        print(df)

        json_results = []

        for item in df.to_dict(orient='records'):
            result = {
                'provider': self.provider,
                'timestamp': item['date'],
                'name': entity.name,
                'margin_value': item['fin_value'],
                'margin_buy': item['fin_buy_value'],
                'short_value': item['sec_value'],
                'short_volume': item['sec_sell_volume'],
                'total_value': item['fin_sec_value']
            }

            json_results.append(result)

        if len(json_results) < 100:
            self.one_shot = True

        return json_results

    def get_data_map(self):
        return None


if __name__ == '__main__':
    MarginTradingSummaryRecorder().run()
# the __all__ is generated
__all__ = ['MarginTradingSummaryRecorder']