from zvtm.domain import Stock1mKdata,Stock1mHfqKdata
from jqdatasdk import auth,get_query_count,get_price
auth('13595667941', 'Yjbir=1977')
print(get_query_count())
spare = get_query_count()
print(spare['spare'])
Stock1mKdata.record_data(provider='joinquant', sleeping_time=0)
# Stock1mHfqKdata.record_data(provider='joinquant', sleeping_time=0)
spare = get_query_count()
print(spare['spare'])