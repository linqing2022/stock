# -*- coding:utf-8 -*-
# 导入tushare
import tushare as ts
import pandas as pd
import os
import sys

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
data = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
print(len(data))
mkdir('D:\\data\\tushare\\hfq\\')
mkdir('D:\\data\\tushare\\fina\\')


mkdir('D:\\data\\tushare\\hfq_fina\\')
mkdir('D:\\data\\tushare\\daily_basic')

if (os.path.exists('D:\\data\\code.txt')):
    os.remove('D:\\data\\code.txt')
note =  open('D:\\data\\code.txt',mode='a')

for i in range(len(data)) :
    code = data.loc[i, 'ts_code']
    print(i,code)
    df_hfq = ts.pro_bar(ts_code=code, adj='hfq', start_date='19900101', end_date=today)
    df_fina = pro.query('fina_indicator_vip', ts_code=code, start_date='19900101', end_date=today)
    df_daily_basic = pro.daily_basic(ts_code=code, start_date='19900101', end_date=today)
    if (df_hfq is not None and df_fina is not None and df_daily_basic is not None):
        df_hfq = df_hfq.sort_values('trade_date')
        df_hfq.to_csv('D:\\data\\tushare\\hfq\\' + code + '.csv',index=False)
        df_fina = df_fina.sort_values('end_date')
        df_fina.to_csv('D:\\data\\tushare\\fina\\' + code + '.csv',index=False)
        df_daily_basic = df_daily_basic.sort_values('trade_date')
        df_daily_basic.to_csv('D:\\data\\tushare\\daily_basic\\' + code + '.csv',index=False)
        note.write(code+'\n')
note.close()

