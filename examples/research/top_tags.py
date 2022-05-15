# -*- coding: utf-8 -*-
from zvtm.api import get_top_performance_by_month
from zvtm.domain import Stock1dHfqKdata
from zvtm.utils import next_date, month_end_date, is_same_date

# 每月涨幅前30，市值90%分布在100亿以下
# 重复上榜的有1/4左右
# 连续两个月上榜的1/10左右
def top_tags(data_provider="em", start_timestamp="2010-01-01"):
    records = []
    for timestamp, df in get_top_performance_by_month(
        start_timestamp=start_timestamp, list_days=250, data_provider=data_provider
    ):
        for entity_id in df.index[:30]:
            query_timestamp = timestamp
            while True:
                kdata = Stock1dHfqKdata.query_data(
                    provider=data_provider,
                    entity_id=entity_id,
                    start_timestamp=query_timestamp,
                    order=Stock1dHfqKdata.timestamp.asc(),
                    limit=1,
                    return_type="domain",
                )
                if not kdata or kdata[0].turnover_rate == 0:
                    if is_same_date(query_timestamp, month_end_date(query_timestamp)):
                        break
                    query_timestamp = next_date(query_timestamp)
                    continue
                cap = kdata[0].turnover / kdata[0].turnover_rate
                break

            records.append(
                {"entity_id": entity_id, "timestamp": timestamp, "cap": cap, "score": df.loc[entity_id, "score"]}
            )

    return records


if __name__ == "__main__":
    print(top_tags())
