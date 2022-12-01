'''
Created on 2020年1月30日

@author: JM
'''
import pandas as pd
import tushare as ts
from sqlalchemy import create_engine 
import pymysql
pymysql.install_as_MySQLdb()
import os
import sys

engine_ts = create_engine('mysql://root:Csu123456789!@127.0.0.1:3306/tushare?charset=utf8&use_unicode=1')

def read_data():
    sql = """SELECT * FROM stock_basic"""
    df = pd.read_sql_query(sql, engine_ts)
    return df


def write_data(df):
    res = df.to_sql('stock_basic', engine_ts, index=False, if_exists='append', chunksize=5000)
    print(res)

import time
import datetime
now = datetime.datetime.now()
today = now.strftime('%y%m%d')
start_date = (datetime.datetime.now() - datetime.timedelta(days = 3))
start_date = start_date.strftime('%y%m%d')
print(today) 
print(start_date) 

if __name__ == '__main__':
    pro = ts.pro_api('fc5ee2aabec9f7148bcdebac4049b1791777700c0b060a34d0ade298')
    ts.set_token('fc5ee2aabec9f7148bcdebac4049b1791777700c0b060a34d0ade298')
    stock_basic = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')

    write_data(stock_basic)
    code_count = len(stock_basic)
    for i in range(code_count) :
        code = stock_basic.loc[i, 'ts_code']
        print(i,code)
        df_hfq = ts.pro_bar(ts_code=code, adj='hfq', start_date=start_date, end_date=today)
        df_fina = pro.query('fina_indicator_vip', ts_code=code, start_date=start_date, end_date=today)
        df_daily_basic = pro.daily_basic(ts_code=code, start_date=start_date, end_date=today)
        if (df_hfq is not None and df_fina is not None and df_daily_basic is not None):
            df_hfq.to_sql('df_hfq', engine_ts, index=False, if_exists='append')
            df_fina.to_sql('df_fina', engine_ts, index=False, if_exists='append')
            df_daily_basic.to_sql('df_daily_basic', engine_ts, index=False, if_exists='append')

