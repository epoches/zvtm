from jqdatapy.api import run_query

from zvtm.contract.recorder import TimeSeriesDataRecorder
from zvtm.domain import Index
from zvtm.domain import StockSummary
from zvtm.utils.time_utils import to_time_str
from zvtm.utils.utils import multiple_number

# 聚宽编码
# 322001	上海市场
# 322002	上海A股
# 322003	上海B股
# 322004	深圳市场	该市场交易所未公布成交量和成交笔数
# 322005	深市主板
# 322006	中小企业板
# 322007	创业板

code_map_jq = {
    '000001': '322002',
    '399106': '322004',
    '399001': '322005',
    '399005': '322006',
    '399006': '322007'
}


class StockSummaryRecorder(TimeSeriesDataRecorder):
    entity_provider = 'exchange'
    entity_schema = Index

    provider = 'joinquant'
    data_schema = StockSummary

    def __init__(self, force_update=False, sleeping_time=5, exchanges=None, entity_ids=None, day_data=False,
                 entity_filters=None, ignore_failed=True, real_time=False, fix_duplicate_way='add',
                 start_timestamp=None, end_timestamp=None) -> None:
        # 上海A股,深圳市场,深圳成指,中小板,创业板
        codes = ['000001', '399106', '399001', '399005', '399006']
        super().__init__(force_update, sleeping_time, exchanges, entity_ids, codes=codes, day_data=day_data,
                         entity_filters=entity_filters, ignore_failed=ignore_failed, real_time=real_time,
                         fix_duplicate_way=fix_duplicate_way, start_timestamp=start_timestamp,
                         end_timestamp=end_timestamp)

    def record(self, entity, start, end, size, timestamps):
        jq_code = code_map_jq.get(entity.code)

        df = run_query(table='finance.STK_EXCHANGE_TRADE_INFO',
                       conditions=f'exchange_code#=#{jq_code}&date#>=#{to_time_str(start)}', parse_dates=['date'])
        print(df)

        json_results = []

        for item in df.to_dict(orient='records'):
            result = {
                'provider': self.provider,
                'timestamp': item['date'],
                'name': entity.name,
                'pe': item['pe_average'],
                'total_value': multiple_number(item['total_market_cap'], 100000000),
                'total_tradable_vaule': multiple_number(item['circulating_market_cap'], 100000000),
                'volume': multiple_number(item['volume'], 10000),
                'turnover': multiple_number(item['money'], 100000000),
                'turnover_rate': item['turnover_ratio']
            }

            json_results.append(result)

        if len(json_results) < 100:
            self.one_shot = True

        return json_results

    def get_data_map(self):
        return None


if __name__ == '__main__':
    StockSummaryRecorder().run()
# the __all__ is generated
__all__ = ['StockSummaryRecorder']