# -*- coding:utf-8 -*-
# 导入tushare
import tushare as ts
import pandas as pd
import os
import sys
import datetime
# from sqlalchemy import create_engine 
# import pymysql
# pymysql.install_as_MySQLdb()
# engine_ts = create_engine('mysql://root:Csu123456789!@127.0.0.1:3306/tushare?charset=utf8&use_unicode=1')

dir_path = 'd:\\data\\tushare\\hfq_fina\\'
all_data_path = 'd:\\data\\tushare\\all.csv'
file_cnt = 0
if (os.path.exists(all_data_path)):
    os.remove(all_data_path)

for inputfile in os.listdir(dir_path):
# header=None表示原始文件数据没有列索引，这样的话read_csv会自动加上列索引
    df = pd.read_csv(dir_path + inputfile,usecols=["ts_code_x", "trade_date","open","pe_dong","dt_netprofit_yoy"]) 
    df['code_index'] = file_cnt
    df.to_csv(all_data_path, mode='a', index=False)
    file_cnt = file_cnt + 1
    
print(file_cnt)
print(datetime.datetime.now()) 
df = pd.read_csv(all_data_path,error_bad_lines=False,low_memory=False) 
print(datetime.datetime.now()) 
df = df.drop(df[df['ts_code_x']=='ts_code_x'].index)
print(datetime.datetime.now()) 
df = df.sort_values('trade_date')
print(datetime.datetime.now()) 
df.to_csv(all_data_path,index=False)
print(datetime.datetime.now())
# df.to_sql('all', engine_ts, index=False, if_exists='append')
# print(datetime.datetime.now()) 
