# -*- coding:utf-8 -*-
# 导入tushare
import pandas as pd
import os
import numpy
import math

max_range = 1000
down_percent = 0.5
sel_up_percent = 3

sel_down_percent = 0.7
profit = 0
balance = 1000000
Shares = 0
sales_value = 0
win_cnt = 0
loss_cnt = 0
buy_price = 0 
sel_price = 0
buy_index = -1
dir_path = 'd:\\data\\tushare\\hfq_fina\\'
all_data_path = 'd:\\data\\tushare\\all.csv'
file_cnt = 5000
pe_dong = 200
peg = 0.5

df = pd.read_csv(all_data_path,error_bad_lines=False,low_memory=False) 
# for inputfile in os.listdir(dir_path):
#     file_cnt = file_cnt + 1
print(len(df))
max_open = numpy.zeros(file_cnt)
code_cnt = numpy.zeros(file_cnt)
open_price = numpy.zeros(file_cnt)

for i in range(len(df)) :
        line = df.loc[i, 'code_index']
        if (math.isnan(line)) :
                continue
        index = int(line)
        code_cnt[index] += 1 
        if ( code_cnt[index] % max_range == 0) :
                max_open[index] = 0
        open_price[index] = float(df.loc[i,'open'])

        # print(i,index,open_price[index])
        if ( open_price[index] > max_open[index]):
                max_open[index] = open_price[index]
                pe_dong_i = float(df.loc[i,'pe_dong'])
                dt_netprofit_yoy_i = float(df.loc[i,'dt_netprofit_yoy'])
                peg_i = pe_dong_i / dt_netprofit_yoy_i
        if (open_price[index] / max_open[index] < down_percent and 
                buy_price == 0 and 
                pe_dong_i <= pe_dong and
                pe_dong_i > 0 and
                peg < 0.5 ):
                
                buy_price = open_price[index]
                Shares = int(balance / buy_price)
                Shares_value = buy_price * Shares
                balance = balance - Shares_value
                code = df.loc[i,'ts_code_x']
                date = df.loc[i,'trade_date']
                buy_index = index
        
                print("code      date     max_open buy_price pe_dong Shares Shares_value balance")
                print("%-9s %-8d %-8d %-8d %-7d %-6d %-12d %-7d" % (code, date, max_open[index], buy_price, df.loc[i,'pe_dong'], Shares, Shares_value, balance))
        if (buy_price > 0 and buy_index == index):
                if (open_price[index] > buy_price * sel_up_percent or open_price[index] < buy_price * sel_down_percent) :
                        sel_price = open_price[index]
                        Shares_value = sel_price * Shares
                        balance = balance + Shares_value
                        profit = balance - 1000000
                        if (sel_price < buy_price):
                                print("loss%8d" % ((sel_price - buy_price) * Shares))
                                loss_cnt = loss_cnt + 1
                        else :
                                win_cnt = win_cnt + 1

                        buy_price = 0
                        code = df.loc[i,'ts_code_x']
                        date = df.loc[i,'trade_date']

                        print("win_cnt loss_cnt      code    date sel_price profit Shares_value balance")
                        print("%-7d %-7d %-9s %-8d %-8d %-8d %-8d %-8d" % (win_cnt, loss_cnt, code, date, sel_price, profit, Shares_value, balance))
