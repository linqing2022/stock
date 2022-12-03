import tushare as ts
from sqlalchemy import create_engine 
import time
import datetime
engine_ts = create_engine('mysql://root:Csu123456789!@127.0.0.1:3306/tushare?charset=utf8&use_unicode=1')
pro = ts.pro_api('fc5ee2aabec9f7148bcdebac4049b1791777700c0b060a34d0ade298')
ts.set_token('fc5ee2aabec9f7148bcdebac4049b1791777700c0b060a34d0ade298')
start_date = '19900101'
end_date = '20221201'
stock_basic = pro.stock_basic(exchange='', list_status='L')
i = 0 
while (i < len(stock_basic)):
    code = stock_basic.loc[i, 'ts_code']
    print(i, code)
    now = datetime.datetime.now()
    print(now)
    try :
        i += 1
        df_hfq_min = ts.pro_bar(ts_code=code, adj='hfq',  freq= '1min', start_date=start_date, end_date=end_date)
        if (df_hfq_min is not None):
            df_hfq_min.to_csv('D://data/tushare/minute' + code + '.csv')
            # df_hfq_min.to_sql('df_hfq_min', engine_ts, index=False, if_exists='append')
    except Exception as e:
        i -= 1   
        time.sleep(36000)

    