# 查询数据 通过各种数据库进行查询，封装起来方便使用

from zvtm import init_log, zvt_config
from zvtm.utils.mysql_pool import MysqlPool
import datetime
import string

# 根据开始日期结束日期获取交易日数据 包含节假日
def get_trade_day(start_date,end_date=datetime.datetime.now().strftime('%Y-%m-%d')):
    #获取交易日数据
    db = 'tushare'
    mp = MysqlPool(zvt_config['mysql_host'], zvt_config['mysql_user'], zvt_config['mysql_password'], db,
                   int(zvt_config['mysql_port']), 'utf8')
    sql = "select timestamp from trade_day where timestamp > %s  and timestamp <= %s"
    df = mp.fetch_all(sql=sql, args=[start_date,end_date])
    return df

# 根据结束日期获取交易日数据 不用包含节假日，只查询交易日日数
def get_trades_day(end_date=datetime.datetime.now(),n=1):
    #获取交易日数据.strftime('%Y-%m-%d')
    start_date = (end_date + datetime.timedelta(-n)).strftime('%Y-%m-%d')
    end_date = end_date.strftime('%Y-%m-%d')
    db = 'tushare'
    mp = MysqlPool(zvt_config['mysql_host'], zvt_config['mysql_user'], zvt_config['mysql_password'], db,
                   int(zvt_config['mysql_port']), 'utf8')
    sql = "select timestamp from trade_day where timestamp > %s  and timestamp <= %s"
    df = mp.fetch_all(sql=sql, args=[start_date,end_date])
    if len(df)>0:
        return df['timestamp'].dt.strftime('%Y-%m-%d').tolist()
    else:
        return -1

def is_trade_day(dt=datetime.datetime.now()):
    start_date = (dt + datetime.timedelta(-1)).strftime('%Y-%m-%d')
    df = get_trade_day(start_date=start_date,end_date=dt)
    if len(df) == 1:
        if df['timestamp'].iloc[0].strftime('%Y-%m-%d') == dt.strftime('%Y-%m-%d'):
            return True
        else:
            return False
    else:
        return False

def get_trade_day_sql(sql,arg):
    #获取交易日数据
    db = 'tushare'
    mp = MysqlPool(zvt_config['mysql_host'], zvt_config['mysql_user'], zvt_config['mysql_password'], db,
                   int(zvt_config['mysql_port']), 'utf8')
    if sql == '' and arg == '':
        sql = "select timestamp from trade_day where timestamp > %s"
        arg = eval("'" + (datetime.datetime.now() + datetime.timedelta(days=-365)).strftime('%Y-%m-%d') + "'")
        df = mp.fetch_all(sql=sql, args=arg)
    else:
        df = mp.fetch_all(sql=sql, args=arg)
    return df

# 获取股票数据 待解决参数问题
def get_stock_list(sql,arg):
    db = 'exchange_stock_meta'
    mp = MysqlPool(zvt_config['mysql_host'], zvt_config['mysql_user'], zvt_config['mysql_password'], db,
                   int(zvt_config['mysql_port']), 'utf8')
    if sql == '' and arg == '':
        sql = 'select code,name from stock where code not in (%s,%s,%s) and name not like %s and name not like %s'
        stocks_list = mp.fetch_all(sql=sql, args=('301%','300%','388%','%ST%','%退%'))
        return stocks_list
    args = eval("('" + "','".join(arg) + "')")
    stocks_list = mp.fetch_all(sql=sql, args=args)
    return stocks_list

def get_security_info(code):
    db = 'exchange_stock_meta'
    mp = MysqlPool(zvt_config['mysql_host'], zvt_config['mysql_user'], zvt_config['mysql_password'], db,
                   int(zvt_config['mysql_port']), 'utf8')
    sql = 'select code,name from stock where code = %s'
    arg = code
    stocks_list = mp.fetch_all(sql=sql, args=arg)
    return stocks_list


# 获取股票和细节数据
def get_stock_detail_list(sql,arg):
    db = 'exchange_stock_meta'
    mp = MysqlPool(zvt_config['mysql_host'], zvt_config['mysql_user'], zvt_config['mysql_password'], db,
                   int(zvt_config['mysql_port']), 'utf8')
    if sql == '' and arg == '':
        sql = 'select a.code,a.name,b.list_date from stock a left join stock_detail b on a.code = b.code where b.list_date <= %s'
        arg = eval("'"+(datetime.datetime.now()+ datetime.timedelta(days=-365)).strftime('%Y-%m-%d')+"'")
        stocks_list = mp.fetch_all(sql=sql, args=arg)
        return stocks_list
    #args = eval("('" + "','".join(arg) + "')")
    stocks_list = mp.fetch_all(sql=sql, args=arg)
    return stocks_list

def get_stock_and_detail_list(sql,arg):
    # 非 301 300 388 ST 退 股票 且上市日期超过1080天
    db = 'exchange_stock_meta'
    mp = MysqlPool(zvt_config['mysql_host'], zvt_config['mysql_user'], zvt_config['mysql_password'], db,
                   int(zvt_config['mysql_port']), 'utf8')
    if sql == '' and arg == '':
        sql = 'select a.code,a.entity_id,a.exchange,a.entity_type,a.name,b.list_date from stock a left join stock_detail b on a.code = b.code where  a.code not in (%s,%s,%s) and a.name not like %s and a.name not like %s and b.list_date <= %s '
        arg = ['301%','300%','388%','%ST%','%退%',(datetime.datetime.now() + datetime.timedelta(days=-1080)).strftime('%Y-%m-%d')]
        stocks_list = mp.fetch_all(sql=sql, args=arg)
        return stocks_list
    #args = eval("('" + "','".join(arg) + "')")
    stocks_list = mp.fetch_all(sql=sql, args=arg)
    return stocks_list

def get_stock_roe_list(sql,arg):
    db = 'stock'
    mp = MysqlPool(zvt_config['mysql_host'], zvt_config['mysql_user'], zvt_config['mysql_password'], db,
                   int(zvt_config['mysql_port']), 'utf8')
    if sql == '' and arg == '':
        sql = 'select code,report_date,roe_ttm,peg from finance_ttm_roe where flag = 1'
        arg = []
        stocks_list = mp.fetch_all(sql=sql, args=arg)
        return stocks_list
    else:
        stocks_list = mp.fetch_all(sql=sql, args=arg)
        return stocks_list


def get_money_flow(sql,arg):
    db = 'em_money_flow1'
    mp = MysqlPool(zvt_config['mysql_host'], zvt_config['mysql_user'], zvt_config['mysql_password'], db,
                   int(zvt_config['mysql_port']), 'utf8')
    if sql=='' and arg=='':
        sql = 'select timestamp,code,change_pct,net_main_inflow_rate,net_huge_inflows,net_huge_inflow_rate,net_main_inflows,net_main_inflow_rate from stock_money_flow where timestamp = %s'
        arg = eval("'"+(datetime.datetime.now()+ datetime.timedelta(days=-1)).strftime('%Y-%m-%d')+"'")
        return mp.fetch_all(sql=sql, args=arg)
    args = eval("('" + "','".join(arg) + "')")
    df = mp.fetch_all(sql=sql, args=args)
    return df


def get_stock_1d_hfq_data(sql,arg):
    db = 'em_stock_1d_hfq_kdata'
    mp = MysqlPool(zvt_config['mysql_host'], zvt_config['mysql_user'], zvt_config['mysql_password'], db,
                   int(zvt_config['mysql_port']), 'utf8')
    if sql=='' and arg=='':
        sql = 'select timestamp,code,open,close,high,low,volume from stock_1d_hfq_kdata where timestamp>=%s'
        arg = eval("'"+(datetime.datetime.now()+ datetime.timedelta(days=-1)).strftime('%Y-%m-%d')+"'")
        return mp.fetch_all(sql=sql, args=arg)
    elif sql=='' and arg !='':
        sql = 'select timestamp,code,open,close,high,low,volume from stock_1d_hfq_kdata where timestamp>=%s'
        args = eval("('" + "','".join(arg) + "')")
        return mp.fetch_all(sql=sql, args=args)
    args = eval("('" + "','".join(arg) + "')")
    df = mp.fetch_all(sql=sql, args=args)
    return df

def get_stock_1wk_hfq_data(sql,arg):
    db = 'em_stock_1wk_hfq_kdata'
    mp = MysqlPool(zvt_config['mysql_host'], zvt_config['mysql_user'], zvt_config['mysql_password'], db,
                   int(zvt_config['mysql_port']), 'utf8')
    if sql=='' and arg=='':
        sql = 'select timestamp,code,open,close,high,low,volume from stock_1wk_hfq_kdata where timestamp>=%s'
        arg = eval("'"+(datetime.datetime.now()+ datetime.timedelta(days=-1)).strftime('%Y-%m-%d')+"'")
        return mp.fetch_all(sql=sql, args=arg)
    elif sql=='' and arg !='':
        sql = 'select timestamp,code,open,close,high,low,volume from stock_1wk_hfq_kdata where timestamp>=%s'
        args = eval("('" + "','".join(arg) + "')")
        return mp.fetch_all(sql=sql, args=args)
    args = eval("('" + "','".join(arg) + "')")
    df = mp.fetch_all(sql=sql, args=args)
    return df

def get_stock_1mon_hfq_data(sql,arg):
    db = 'em_stock_1mon_hfq_kdata'
    mp = MysqlPool(zvt_config['mysql_host'], zvt_config['mysql_user'], zvt_config['mysql_password'], db,
                   int(zvt_config['mysql_port']), 'utf8')
    if sql=='' and arg=='':
        sql = 'select timestamp,code,open,close,high,low,volume from stock_1mon_hfq_kdata where timestamp>=%s'
        arg = eval("'"+(datetime.datetime.now()+ datetime.timedelta(days=-1)).strftime('%Y-%m-%d')+"'")
        return mp.fetch_all(sql=sql, args=arg)
    elif sql=='' and arg !='':
        sql = 'select timestamp,code,open,close,high,low,volume from stock_1mon_hfq_kdata where timestamp>=%s'
        args = eval("('" + "','".join(arg) + "')")
        return mp.fetch_all(sql=sql, args=args)
    args = eval("('" + "','".join(arg) + "')")
    df = mp.fetch_all(sql=sql, args=args)
    return df

def get_finance_data(sql,arg):
    db = 'eastmoney_finance'
    mp = MysqlPool(zvt_config['mysql_host'], zvt_config['mysql_user'], zvt_config['mysql_password'], db,
                   int(zvt_config['mysql_port']), 'utf8')
    if sql=='' and arg=='':
        sql = 'select timestamp,code,net_profit_growth_yoy,report_period,op_income_growth_yoy,basic_eps,total_op_income,' \
              'net_profit,roe,rota,gross_profit_margin,net_margin from finance_factor where timestamp>=%s'
        arg = eval("'"+(datetime.datetime.now()+ datetime.timedelta(days=-1)).strftime('%Y-%m-%d')+"'")
        return mp.fetch_all(sql=sql, args=arg)
    elif sql=='' and arg !='':
        sql =  'select timestamp,code,net_profit_growth_yoy,report_period,op_income_growth_yoy,basic_eps,total_op_income,' \
              'net_profit,roe,rota,gross_profit_margin,net_margin from finance_factor where timestamp>=%s and net_profit_growth_yoy > %s' \
               ' and report_period = %s and op_income_growth_yoy > %s'
        args = eval("('" + "','".join(arg) + "')")
        return mp.fetch_all(sql=sql, args=args)
    args = eval("('" + "','".join(arg) + "')")
    df = mp.fetch_all(sql=sql, args=args)
    return df

def get_stock_valuation_data(sql,arg):
    db = 'em_valuation1'
    mp = MysqlPool(zvt_config['mysql_host'], zvt_config['mysql_user'], zvt_config['mysql_password'], db,
                   int(zvt_config['mysql_port']), 'utf8')
    if sql=='' and arg=='':
        sql = 'select timestamp,code,name,capitalization,circulating_cap,market_cap,circulating_market_cap,turnover_ratio,pe,pe_ttm,pb,ps,pcf from stock_valuation where timestamp >= %s'
        arg = eval("'"+(datetime.datetime.now()+ datetime.timedelta(days=-1)).strftime('%Y-%m-%d')+"'")
        return mp.fetch_all(sql=sql, args=arg)
    elif sql=='' and arg !='':
        sql =  'select timestamp,code,name,capitalization,circulating_cap,market_cap,circulating_market_cap,turnover_ratio,pe,pe_ttm,pb,ps,pcf from stock_valuation where timestamp>=%s and pe > %s and pe_ttm > %s and pb > %s '
        args = eval("('" + "','".join(arg) + "')")
        return mp.fetch_all(sql=sql, args=args)
    args = eval("('" + "','".join(arg) + "')")
    df = mp.fetch_all(sql=sql, args=args)
    return df

def get_index_1d_kdata(sql,arg):
    db = 'em_index_1d_kdata'
    mp = MysqlPool(zvt_config['mysql_host'], zvt_config['mysql_user'], zvt_config['mysql_password'], db,
                   int(zvt_config['mysql_port']), 'utf8')
    if sql=='' and arg=='':
        sql = 'select timestamp,provider,code,name,level,open,close,high,low,volume,turnover,change_pct,turnover_rate  from index_1d_kdata where code=\'000001\' and timestamp >= %s'
        arg = eval("'" + (datetime.datetime.now() + datetime.timedelta(days=-1)).strftime('%Y-%m-%d') + "'")
        return mp.fetch_all(sql=sql, args=arg)
    elif sql=='' and arg !='':
        sql =  'select timestamp,provider,code,name,level,open,close,high,low,volume,turnover,change_pct,turnover_rate  from index_1d_kdata where code=\'000001\' and timestamp >= %s and  volume > %s and turnover > %s '
        args = eval("('" + "','".join(arg) + "')")
        return mp.fetch_all(sql=sql, args=args)
    args = eval("('" + "','".join(arg) + "')")
    df = mp.fetch_all(sql=sql, args=args)
    return df

def get_stock_1d_hfq_data_in(sql,arg):
    db = 'em_stock_1d_hfq_kdata'
    mp = MysqlPool(zvt_config['mysql_host'], zvt_config['mysql_user'], zvt_config['mysql_password'], db,
                   int(zvt_config['mysql_port']), 'utf8')
    df = mp.fetch_all(sql=sql, args=arg)
    return df

def get_stock_1wk_hfq_data_in(sql,arg):
    db = 'em_stock_1wk_hfq_kdata'
    mp = MysqlPool(zvt_config['mysql_host'], zvt_config['mysql_user'], zvt_config['mysql_password'], db,
                   int(zvt_config['mysql_port']), 'utf8')
    df = mp.fetch_all(sql=sql, args=arg)
    return df

def get_stock_1mon_hfq_data_in(sql,arg):
    db = 'em_stock_1mon_hfq_kdata'
    mp = MysqlPool(zvt_config['mysql_host'], zvt_config['mysql_user'], zvt_config['mysql_password'], db,
                   int(zvt_config['mysql_port']), 'utf8')
    df = mp.fetch_all(sql=sql, args=arg)
    return df

def get_stock_5m_hfq_data(sql,arg):
    db = 'em_stock_5m_hfq_kdata'
    mp = MysqlPool(zvt_config['mysql_host'], zvt_config['mysql_user'], zvt_config['mysql_password'], db,
                   int(zvt_config['mysql_port']), 'utf8')
    df = mp.fetch_all(sql=sql, args=arg)
    return df

def get_stock_1m_hfq_data(sql,arg):
    db = 'em_stock_1m_hfq_kdata'
    mp = MysqlPool(zvt_config['mysql_host'], zvt_config['mysql_user'], zvt_config['mysql_password'], db,
                   int(zvt_config['mysql_port']), 'utf8')
    df = mp.fetch_all(sql=sql, args=arg)
    return df

def get_stock_1h_hfq_data(sql,arg):
    db = 'em_stock_1h_hfq_kdata'
    mp = MysqlPool(zvt_config['mysql_host'], zvt_config['mysql_user'], zvt_config['mysql_password'], db,
                   int(zvt_config['mysql_port']), 'utf8')
    df = mp.fetch_all(sql=sql, args=arg)
    return df

def get_index_5m_kdata(sql,arg):
    db = 'em_index_5m_kdata'
    mp = MysqlPool(zvt_config['mysql_host'], zvt_config['mysql_user'], zvt_config['mysql_password'], db,
                   int(zvt_config['mysql_port']), 'utf8')
    df = mp.fetch_all(sql=sql, args=arg)
    return df

def get_index_1m_kdata(sql,arg):
    db = 'em_index_1m_kdata'
    mp = MysqlPool(zvt_config['mysql_host'], zvt_config['mysql_user'], zvt_config['mysql_password'], db,
                   int(zvt_config['mysql_port']), 'utf8')
    df = mp.fetch_all(sql=sql, args=arg)
    return df

def get_index_1h_kdata(sql,arg):
    db = 'em_index_1h_kdata'
    mp = MysqlPool(zvt_config['mysql_host'], zvt_config['mysql_user'], zvt_config['mysql_password'], db,
                   int(zvt_config['mysql_port']), 'utf8')
    df = mp.fetch_all(sql=sql, args=arg)
    return df

def get_index_1h_kdata_in(sql,arg):
    db = 'em_index_1h_kdata'
    mp = MysqlPool(zvt_config['mysql_host'], zvt_config['mysql_user'], zvt_config['mysql_password'], db,
                   int(zvt_config['mysql_port']), 'utf8')
    df = mp.fetch_all(sql=sql, args=arg)
    return df

def get_data(db,sql,arg):
    mp = MysqlPool(zvt_config['mysql_host'], zvt_config['mysql_user'], zvt_config['mysql_password'], db,
                   int(zvt_config['mysql_port']), 'utf8')
    df = mp.fetch_all(sql=sql, args=arg)
    return df

def update_data(db,sql,arg):
    mp = MysqlPool(zvt_config['mysql_host'], zvt_config['mysql_user'], zvt_config['mysql_password'], db,
                   int(zvt_config['mysql_port']), 'utf8')
    result = mp.update_one(sql=sql, args=arg)
    return result

def update_many(db,sql,arg):
    mp = MysqlPool(zvt_config['mysql_host'], zvt_config['mysql_user'], zvt_config['mysql_password'], db,
                   int(zvt_config['mysql_port']), 'utf8')
    result = mp.updatemany(sql=sql, args=arg)
    return result

if __name__ == '__main__':
    #df = get_stock_list(sql='',arg='')
    #sql = 'select a.code,a.name,b.list_date from stock a left join stock_detail b on a.code = b.code where b.list_date <= %s and a.code in %s'
    #from dateutil.relativedelta import relativedelta
    #last_year = datetime.datetime.today() - relativedelta(months=36)  # 获取前三年的日期
    #arg = [last_year.strftime('%Y-%m-%d'),['000001','002932']]
    # db = 'em_stock_meta'
    # mp = MysqlPool(zvt_config['mysql_host'], zvt_config['mysql_user'], zvt_config['mysql_password'], db,
    #                int(zvt_config['mysql_port']), 'utf8')
    # stocks_list = mp.fetch_all(sql=sql, args=arg)
    #df = get_stock_and_detail_list(sql=sql,arg=arg)
    # df = get_security_info('000001')
    # print(df)
    #sql = 'select timestamp,code,change_pct,net_main_inflow_rate from stock_money_flow where timestamp = %s and change_pct <%s'
    #arg = ['2022-03-28','9']
    #df = get_money_flow(sql=sql, arg=arg)
    #df = get_money_flow(sql='', arg='')
    #arg = ['2022-03-28']
    #df = get_stock_1d_hfq_data(sql='',arg=arg)
    #df = get_stock_1wk_hfq_data(sql='', arg='')
    #df = get_stock_1mon_hfq_data(sql='', arg='')
    #arg = ['2022-03-26','0.6','year','0.4']
    #df = get_finance_data(sql='', arg=arg)
    #arg = ['2022-03-26', '0', '0', '0']
    #df = get_stock_valuation_data(sql='', arg=arg)
    # arg = ['2022-03-31', '0', '0']
    # df = get_index_1d_kdata(sql='', arg=arg)
    # print(df)
    db = 'em_stock_1m_hfq_kdata'
    sql = "select timestamp,code,open,close,high,low,volume from stock_1m_hfq_kdata where timestamp>= %s and timestamp <=%s and code = %s"
    arg = ['2022-01-01','2022-03-01','601398']
    df = get_data(db=db,sql=sql, arg=arg)
    print(df)
    #获取日线数据 code in
    # sql = "select timestamp,code,open,close,high,low,volume from stock_1d_hfq_kdata where timestamp>= %s and timestamp <=%s and code in %s"
    # arg = ["2021-08-01","2022-04-15",['000012','000408','000429','000510','000520','000532','000568','000596','000603','000615','000636','000661','000663','000683','000723','000725','000731','000733','000786','000818','000822','000830','000848','000893','000915','000923','000949','002001','002002','002003','002008','002022','002023','002027','002028','002050','002064','002080','002088','002098','002109','002128','002130','002138','002145','002158','002179','002185','002222','002240','002245','002254','002258','002266','002270','002274','002293','002382','002402','002407','002415','002430','002436','002452','002460','002469','002497','002517','002532','002568','002572','002581','002597','002601','002636','002677','002690','002709','002732','002759','002763','002774','002778','002791','002801','002810','002815','002818','002829','002832','002833','002859','002860','002876','002884','002900','002913','002915','002919','002925','002949','300012','300014','300033','300037','300095','300121','300124','300146','300158','300171','300196','300244','300259','300280','300286','300305','300316','300327','300347','300350','300373','300390','300401','300408','300409','300415','300416','300418','300441','300453','300458','300460','300463','300481','300494','300502','300503','300518','300522','300529','300573','300587','300596','300601','300610','300613','300623','300628','300630','300639','300643','300653','300676','300685','300687','300705','300723','300724','300725','300737','300747','300760','300770','600018','600063','600075','600089','600111','600113','600123','600141','600171','600176','600183','600273','600285','600295','600328','600338','600389','600395','600426','600438','600452','600460','600499','600535','600563','600566','600570','600571','600586','600596','600618','600702','600707','600746','600817','600836','600845','600867','600884','600885','600887','600976','600985','600988','600997','601001','601100','601101','601216','601225','601515','601636','601678','601865','601882','601888','601969','603005','603008','603010','603025','603026','603055','603067','603077','603088','603100','603181','603197','603198','603203','603260','603277','603297','603369','603380','603386','603393','603444','603486','603501','603517','603566','603568','603589','603599','603606','603626','603658','603678','603707','603722','603726','603738','603799','603801','603806','603823','603833','603848','603855','603877','603882','603899','603908','603938','603985','603986','603989']]
    # df = get_stock_1d_hfq_data_in(sql=sql, arg=arg)
    # print(df)
# the __all__ is generated
__all__ = ['get_data','get_trade_day','get_trade_day_sql','get_stock_list','get_security_info','get_stock_detail_list','get_stock_and_detail_list','get_money_flow','get_stock_1d_hfq_data','get_stock_1wk_hfq_data','get_stock_1mon_hfq_data','get_finance_data','get_stock_valuation_data','get_index_1d_kdata','get_stock_1d_hfq_data_in','get_stock_1wk_hfq_data_in','get_stock_1mon_hfq_data_in','get_stock_5m_hfq_data','get_index_5m_kdata','get_stock_1h_hfq_data','get_index_1h_kdata','get_index_1h_kdata_in','get_stock_1m_hfq_data','get_index_1m_kdata']