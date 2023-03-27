# 对 finance 没有填的日期进行填充

import pandas as pd
import requests
from tqdm import tqdm

def stock_yjbb_em(date: str = "20200331") -> pd.DataFrame:
    """
    东方财富-数据中心-年报季报-业绩快报-业绩报表
    https://data.eastmoney.com/bbsj/202003/yjbb.html
    :param date: "20200331", "20200630", "20200930", "20201231"; 从 20100331 开始
    :type date: str
    :return: 业绩报表
    :rtype: pandas.DataFrame
    'filter': f"(REPORTDATE='{'-'.join([date[:4], date[4:6], date[6:]])}')",
    """
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        'pageSize': '500',
        'pageNumber': '1',
        'reportName': 'RPT_LICO_FN_CPD',
        'columns': 'SECURITY_CODE,REPORTDATE,NOTICE_DATE',
        'filter': f"(REPORTDATE='{'-'.join([date[:4], date[4:6], date[6:]])}')",
    }
    r = requests.get(url, params=params)
    data_json = r.json()
    page_num = data_json["result"]["pages"]
    big_df = pd.DataFrame()
    for page in tqdm(range(1, page_num + 1), leave=False):
        params.update({
            'pageNumber': page,
        })
        r = requests.get(url, params=params)
        data_json = r.json()
        temp_df = pd.DataFrame(data_json["result"]["data"])
        big_df = pd.concat([big_df, temp_df], ignore_index=True)

    big_df.reset_index(inplace=True)
    big_df["index"] = range(1, len(big_df) + 1)
    big_df.columns = [
        "序号",
        "code",
        "report_date",
        "notice_date",
    ]
    big_df = big_df[
        [
            "序号",
            "code",
            "report_date",
            "notice_date",
        ]
    ]
    big_df['report_date'] = pd.to_datetime(big_df['report_date']).dt.date
    big_df['notice_date'] = pd.to_datetime(big_df['notice_date']).dt.date
    return big_df

# 获取没有finance 没有timestamp的数据
from schedule.utils.query_data import get_data,update_data
#,codes='001225' and code in %s
def get_finance(report_date='2021-12-31'):
    db = 'eastmoney_finance'
    sql = "select timestamp,report_date,code from finance_factor where  \
                   report_date = %s and timestamp = report_date"
    arg = [report_date]
    df = get_data(db=db, sql=sql, arg=arg)
    return df

def update_finance(code,timestamp,report_date):
    db = 'eastmoney_finance'
    sql = 'UPDATE finance_factor set timestamp=%s where code = %s and report_date = %s'
    arg = [timestamp,code,report_date]
    update_data(db=db, sql=sql, arg=arg)

# codes = ['001225']
# report_dates = ['1994-12-31','1995-12-31','1996-12-31','1997-06-30','1997-12-31','1998-06-30','1998-12-31','1999-06-30','1999-12-31','2000-06-30','2000-12-31','2001-06-30','2001-12-31','2002-03-31','2002-06-30','2002-09-30','2002-12-31','2003-03-31','2003-06-30','2003-09-30','2003-12-31','2004-03-31','2004-06-30','2004-09-30','2004-12-31','2005-03-31','2005-06-30','2005-09-30','2005-12-31','2006-03-31','2006-06-30','2006-09-30','2006-12-31','2007-03-31','2007-06-30','2007-09-30','2007-12-31','2008-03-31','2008-06-30','2008-09-30','2008-12-31','2009-03-31','2009-06-30','2009-09-30','2009-12-31','2010-03-31','2010-06-30','2010-09-30','2010-12-31','2011-03-31','2011-06-30','2011-09-30','2011-12-31','2012-03-31','2012-06-30','2012-09-30','2012-12-31','2013-03-31','2013-06-30','2013-09-30','2013-12-31','2014-03-31','2014-06-30','2014-09-30','2014-12-31','2015-03-31','2015-06-30','2015-09-30','2015-12-31','2016-03-31','2016-06-30','2016-09-30','2016-12-31','2017-03-31','2017-06-30','2017-09-30','2017-12-31','2018-03-31','2018-06-30','2018-09-30','2018-12-31','2019-03-31','2019-06-30','2019-09-30','2019-12-31','2020-03-31','2020-06-30','2020-09-30','2020-12-31','2021-03-31','2021-06-30','2021-09-30','2021-12-31','2022-03-31','2022-06-30','2022-09-30','2022-12-31','2001-09-30']
report_dates = ['2022-12-31']

for report_date in report_dates:
    df = get_finance(report_date)

    report_date1 = report_date.replace('-','')

    df1 = stock_yjbb_em(report_date1)

    #填充获取的数据并写入
    for i in range(len(df)):
        df_code = df1[df1['code'] == df.loc[i,'code']]
        if len(df_code) == 1:
            update_finance(df_code['code'].iloc[0],df_code['notice_date'].iloc[0],report_date)
