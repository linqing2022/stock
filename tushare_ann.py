import tushare as ts
import pandas as pd
import pymysql
pymysql.install_as_MySQLdb()
from sqlalchemy import create_engine 
import datetime
pro = ts.pro_api('fc5ee2aabec9f7148bcdebac4049b1791777700c0b060a34d0ade298')
ts.set_token('fc5ee2aabec9f7148bcdebac4049b1791777700c0b060a34d0ade298')
stock_basic = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
code_count = len(stock_basic)
print(code_count)
engine_ts = create_engine('mysql://root:Csu123456789!@127.0.0.1:3306/tushare?charset=utf8&use_unicode=1')
for i in range(code_count):
    ts_code = stock_basic.loc[i,'ts_code']
    end_date = datetime.datetime.now().strftime("%Y%m%d")
    for j in range(200):
        start_date = (datetime.datetime.strptime(end_date,'%Y%m%d') - datetime.timedelta(days = 50))
        start_date = start_date.strftime('%Y%m%d')
        print(start_date,end_date,ts_code)
        df = pro.anns(ts_code=ts_code, start_date = start_date, end_date=end_date)
        df.to_sql('ann', engine_ts, index=False, if_exists='append', chunksize=5000)
        end_date = start_date

    
