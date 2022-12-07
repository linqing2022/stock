import tushare as ts
import pandas as pd
index_path = 'd:\\data\\tushare\\ths_index\\'

import datetime
now = datetime.datetime.now()
today = now.strftime('%y%m%d')
today = '20' + today
print(today) 

pro = ts.pro_api('fc5ee2aabec9f7148bcdebac4049b1791777700c0b060a34d0ade298')
ts.set_token('fc5ee2aabec9f7148bcdebac4049b1791777700c0b060a34d0ade298')
df = pro.ths_index()
df.to_csv(index_path + 'ths_index.csv')

for i in range(len(df)):
    code = df.loc[i,'ts_code']
    name = df.loc[i,'name']
    name = name.replace('/'," ")
    print(i,code,name)
    df_daily = pro.ths_daily(ts_code=code, end_date=today)
    df_daily.to_csv(index_path + code + '_' + name + '.csv')