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
import threading
import sys

thread_count = 4
note =  open('D:data\config.txt',mode='r')
lines = note.readlines()
get_day_count = int(lines[0].strip('\n'))
print(get_day_count)
engine_ts = create_engine('mysql://root:Csu123456789!@127.0.0.1:3306/tushare?charset=utf8&use_unicode=1')

def read_data():
    sql = """SELECT * FROM stock_basic"""
    df = pd.read_sql_query(sql, engine_ts)
    return df

def write_data(df):
    code_count = df.to_sql('stock_basic', engine_ts, index=False, if_exists='append', chunksize=5000)
    print('stock_basic count:',code_count)

import time
import datetime
now = datetime.datetime.now()
today = now.strftime('%y%m%d')
today = '20' + today
start_date = (datetime.datetime.now() - datetime.timedelta(days = get_day_count))
start_date = start_date.strftime('%y%m%d')
start_date = '20' + start_date
print('today:',today) 
print('start_date:',start_date) 
pro = ts.pro_api('fc5ee2aabec9f7148bcdebac4049b1791777700c0b060a34d0ade298')
ts.set_token('fc5ee2aabec9f7148bcdebac4049b1791777700c0b060a34d0ade298')
stock_basic = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')

write_data(stock_basic)
code_count = len(stock_basic)

def get_tushare(start_index:int,end_index:int) -> None:
    print(start_index,end_index)
    i = start_index
    while i <= end_index:
        code = stock_basic.loc[i, 'ts_code']
        print(i,code)
        df_hfq = ts.pro_bar(ts_code=code, adj='hfq', start_date=start_date, end_date=today)
        df_fina = pro.query('fina_indicator_vip', ts_code=code, start_date=start_date, end_date=today)
        df_daily_basic = pro.daily_basic(ts_code=code, start_date=start_date, end_date=today)
        i = i + 1
        if (df_hfq is not None and df_fina is not None and df_daily_basic is not None):
            count = df_hfq.to_sql('df_hfq', engine_ts, index=False, if_exists='append')
            print('df_hfq count:',count)
            count = df_fina.to_sql('df_fina', engine_ts, index=False, if_exists='append')
            # print('df_fina count:',count)
            count = df_daily_basic.to_sql('df_daily_basic', engine_ts, index=False, if_exists='append')
            # print('df_hfq count:',count)

for i in range(thread_count):
    start_index = int(code_count/thread_count * i)
    end_index =  int(code_count/thread_count * (i + 1))
    if (i + 1 == thread_count):
        end_index = code_count
    t = threading.Thread(target=get_tushare,args=(start_index,end_index))
    t.start()



