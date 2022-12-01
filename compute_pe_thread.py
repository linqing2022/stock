# -*- coding:utf-8 -*-
# 导入tushare
import tushare as ts
import pandas as pd
import os
import sys
import threading

note = open('D:\\data\\tushare\\code.txt','r')
lines = note.readlines()
code_count = len(lines)
thread_count = 1

def compute(start_index:int,end_index:int) -> None:
    print(start_index,end_index)
    i = start_index
    while i < end_index:
        code = lines[i].strip('\n')
        print(i,code)
        i = i + 1

        path_hfq = 'd:\\data\\tushare\\hfq\\' + code + '.csv'
        if (os.path.exists(path_hfq)):
            df_hfq =pd.read_csv(path_hfq)
        else:
            continue

        path_fina = 'd:\\data\\tushare\\fina\\' + code + '.csv'
        if (os.path.exists(path_fina)):
            df_fina =pd.read_csv(path_fina)
        else:
            continue

        path_daily_basic = 'd:\\data\\tushare\\daily_basic\\' + code + '.csv'
        if (os.path.exists(path_daily_basic)):
            df_daily_basic =pd.read_csv(path_daily_basic)
        else:
            continue
        df_hfq =pd.read_csv('d:\\data\\tushare\\hfq\\' + code + '.csv')
        df_fina = pd.read_csv('d:\\data\\tushare\\fina\\' + code + '.csv')
        df_daily_basic = pd.read_csv('d:\\data\\tushare\\daily_basic\\' + code + '.csv')

        df_hfq = pd.merge(df_hfq,df_daily_basic,on="trade_date")
        count_hfq = len(df_hfq) - 1
        count_fina = len(df_fina) -1
        if (count_hfq == -1 or count_fina == -1) :
            continue;
        #print(count_hfq,count_fina)
        j= k = 0
        df_hfq['profit_dedt'] = ''
        df_hfq['pe_dong'] = ''
        while j <= count_hfq:
            if ((k == count_fina) or (df_hfq.loc[j,'trade_date'] > df_fina.loc[k,'end_date'] and df_hfq.loc[j,'trade_date'] <= df_fina.loc[k + 1,'end_date'])):
                    end_date = str(df_fina.loc[k, 'end_date'])
                    profit_dedt_k = df_fina.loc[k,'profit_dedt']
                    if ('0331' in end_date):
                        df_hfq.loc[j,'profit_dedt'] = profit_dedt_k * 4 
                    if ('0630' in end_date):
                        df_hfq.loc[j,'profit_dedt'] = profit_dedt_k * 2 
                    if ('0930' in end_date):
                        df_hfq.loc[j,'profit_dedt'] = profit_dedt_k * 4 / 3 
                    if ('1231' in end_date):
                        df_hfq.loc[j,'profit_dedt'] = profit_dedt_k
                    df_hfq.loc[j,'pe_dong'] =  df_hfq.loc[j, 'total_mv'] * 10000 / df_hfq.loc[j,'profit_dedt']
                    df_hfq.loc[j,'dt_netprofit_yoy'] = df_fina.loc[k,'dt_netprofit_yoy'] 
                    j = j + 1
            elif (df_hfq.loc[j,'trade_date'] <= df_fina.loc[k,'end_date']):
                    j = j + 1
            elif (df_hfq.loc[j,'trade_date'] > df_fina.loc[k + 1,'end_date']):
                    k = k + 1

        df_hfq.to_csv('d:\\data\\tushare\\hfq_fina\\' + code + '.csv', index =False)

for i in range(thread_count):
    start_index = int(code_count/thread_count * i)
    end_index =  int(code_count/thread_count * (i + 1))
    if (i + 1 == thread_count):
        end_index = code_count
    t = threading.Thread(target=compute,args=(start_index,end_index))
    t.start()