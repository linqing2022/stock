# -*- coding:utf-8 -*-
# 导入tushare
import tushare as ts
import pandas as pd
import os
import sys
import threading

thread_count = 10
code_count =0
lock = threading.Lock()

def mkdir(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)
import datetime
now = datetime.datetime.now()
today = now.strftime('%y%m%d')
print(today) 
# 初始化pro接口
pro = ts.pro_api('fc5ee2aabec9f7148bcdebac4049b1791777700c0b060a34d0ade298')
ts.set_token('fc5ee2aabec9f7148bcdebac4049b1791777700c0b060a34d0ade298')
stock_basic = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
code_count = len(stock_basic)
print(code_count)
mkdir('d:\\data\\tushare\\hfq\\')
mkdir('d:\\data\\tushare\\fina\\')
mkdir('d:\\data\\tushare\\hfq_fina\\')
mkdir('d:\\data\\tushare\\daily_basic')

if (os.path.exists('D:\\data\\tushare\\code.txt')):
    os.remove('D:\\data\\tushare\\code.txt')
note =  open('D:\\data\\tushare\\code.txt',mode='a')

def get_tushare(start_index:int,end_index:int) -> None:
    print(start_index,end_index)
    i = start_index
    while i <= end_index:
        code = stock_basic.loc[i, 'ts_code']
        print(i,code)
        df_hfq = ts.pro_bar(ts_code=code, adj='hfq', start_date='19900101', end_date=today)
        df_fina = pro.query('fina_indicator_vip', ts_code=code, start_date='19900101', end_date=today)
        df_daily_basic = pro.daily_basic(ts_code=code, start_date='19900101', end_date=today)
        i = i + 1
        if (df_hfq is not None and df_fina is not None and df_daily_basic is not None):
            df_hfq = df_hfq.sort_values('trade_date')
            df_hfq.to_csv('d:\\data\\tushare\\hfq\\' + code + '.csv',index=False)
            df_fina = df_fina.sort_values('end_date')
            df_fina.to_csv('d:\\data\\tushare\\fina\\' + code + '.csv',index=False)
            df_daily_basic = df_daily_basic.sort_values('trade_date')
            df_daily_basic.to_csv('d:\\data\\tushare\\daily_basic\\' + code + '.csv',index=False)
            lock.acquire()
            note.write(code+'\n')
            lock.release()

for i in range(thread_count):
    start_index = int(code_count/thread_count * i)
    end_index =  int(code_count/thread_count * (i + 1))
    if (i + 1 == thread_count):
        end_index = code_count
    t = threading.Thread(target=get_tushare,args=(start_index,end_index))
    t.start()
    
if(threading.active_count() == 0):
    note.close()

