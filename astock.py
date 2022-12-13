import tushare as ts
import pandas as pd
import pymysql
pymysql.install_as_MySQLdb()
from sqlalchemy import create_engine 
pro = ts.pro_api('fc5ee2aabec9f7148bcdebac4049b1791777700c0b060a34d0ade298')
ts.set_token('fc5ee2aabec9f7148bcdebac4049b1791777700c0b060a34d0ade298')
engine_ts = create_engine('mysql://root:Csu123456789!@127.0.0.1:3306/tushare?charset=utf8&use_unicode=1')

src = 'val'
df = pd.read_csv('d:\\val.csv',sep='\t')
df = df.drop(df.loc[df.verbA0A1.isna()].index)
df = df.drop(df.loc[df.verbA0A1=='[]'].index)
print(df)    
df.to_sql(src, engine_ts, index=False, if_exists='append', chunksize=5000)