from zvtm.domain.misc.stock_news import StockNews

# stock_sh_605116 stock_sz_002932
StockNews.record_data(provider='em',entity_id='stock_sh_601228')
df =StockNews.query_data(provider='em',entity_id='stock_sh_601228')
print(df[-10:])