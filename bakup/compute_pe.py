# -*- coding:utf-8 -*-
# 导入tushare
import tushare as ts
import pandas as pd
import os
import sys

note = open('D:\\data\\code.txt','r')
code_count = note.readlines()

for i in range(len(code_count)):
    code = code_count[i].strip('\n')
    print(i,code)
    df_hfq =pd.read_csv('D:\\data\\tushare\\hfq\\' + code + '.csv',usecols=["ts_code", "trade_date"])
    df_fina = pd.read_csv('D:\\data\\tushare\\fina\\' + code + '.csv',usecols=["end_date","profit_dedt"])
    df_daily_basic = pd.read_csv('D:\\data\\tushare\\daily_basic\\' + code + '.csv',usecols=["trade_date","total_mv"])

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
                if ('0331' in end_date):
                    df_hfq.loc[j,'profit_dedt'] = df_fina.loc[k,'profit_dedt'] * 4 
                if ('0630' in end_date):
                    df_hfq.loc[j,'profit_dedt'] = df_fina.loc[k,'profit_dedt'] * 2 
                if ('0930' in end_date):
                    df_hfq.loc[j,'profit_dedt'] = df_fina.loc[k,'profit_dedt'] * 4 / 3 
                if ('1231' in end_date):
                    df_hfq.loc[j,'profit_dedt'] = df_fina.loc[k,'profit_dedt']
                df_hfq.loc[j,'pe_dong'] =  df_hfq.loc[j, 'total_mv'] * 10000 / df_hfq.loc[j,'profit_dedt']
                # print(sys._getframe().f_lineno,j,k,df_hfq.loc[j,'trade_date'],df_fina.loc[k,'end_date'],df_fina.loc[k,'profit_dedt'],df_hfq.loc[j,'profit_dedt'])
                j = j + 1
        elif (df_hfq.loc[j,'trade_date'] <= df_fina.loc[k,'end_date']):
                j = j + 1
        elif (df_hfq.loc[j,'trade_date'] > df_fina.loc[k + 1,'end_date']):
                k = k + 1

    df_hfq.to_csv('.\\tushare\\hfq_fina\\' + code + '.csv', index =False)
