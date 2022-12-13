import tushare as ts
import pandas as pd
import pymysql
pymysql.install_as_MySQLdb()
from sqlalchemy import create_engine 
import datetime
end_date = datetime.datetime.now().strftime("%Y-%m-%d")
pro = ts.pro_api('fc5ee2aabec9f7148bcdebac4049b1791777700c0b060a34d0ade298')
ts.set_token('fc5ee2aabec9f7148bcdebac4049b1791777700c0b060a34d0ade298')
engine_ts = create_engine('mysql://root:Csu123456789!@127.0.0.1:3306/tushare?charset=utf8&use_unicode=1')
list =['sina','wallstreetcn','10jqka','eastmoney','yuncaijing']
for i in range(2500):
    start_date = (datetime.datetime.strptime(end_date,'%Y-%m-%d') - datetime.timedelta(days = 1))
    start_date = start_date.strftime('%Y-%m-%d')
    print(start_date,end_date)

    for src in list:
        df = pro.news(src=src, start_date = start_date, end_date=end_date)
        df.to_sql(src, engine_ts, index=False, if_exists='append', chunksize=5000)
        df.to_csv('d:\\data\\tushare\\' + src + '.csv',mode='a',header=False,index=False)
    end_date = start_date
    
