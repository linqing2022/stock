# -*- coding:utf-8 -*-
# 导入tushare
import pandas as pd
import os

max_range = 250
max_open  = 0
down_percent = 0.3
buy_price = 0 
sel_up_percent = 2
sel_down_percent = 0.8
sel_price = 0
profit = 0
balance = 1000000
Shares = 0
sales_value = 0
win_cnt = 0
loss_cnt = 0

dir_path = 'd:\\data\\tushare\\hfq_fina\\'
for root, dirs, files in os.walk(dir_path):
    for fname in files:
        df = pd.read_csv(dir_path + str(fname))
        for i in range(len(df)) :
            if ( i % max_range == 0) :
                max_open = 0
            open = df.loc[i,'open']
            if ( open > max_open):
                max_open = open
            if (open / max_open < down_percent and buy_price == 0):
                buy_price = open
                Shares = int(balance / buy_price)
                Shares_value = buy_price * Shares
                balance = balance - Shares_value
                print("code date max_open buy_price Shares Shares_value balance")
                print(df.loc[i,'ts_code'], df.loc[i,'trade_date'],max_open,buy_price,Shares,Shares_value,balance)
            if (buy_price > 0):
                if (open > buy_price * sel_up_percent or open < buy_price * sel_down_percent) :
                    sel_price = open
                    Shares_value = sel_price * Shares
                    balance = balance + Shares_value
                    profit = balance - 1000000
                    if (sel_price < buy_price):
                        print("loss", (sel_price - buy_price) * Shares)
                        loss_cnt = loss_cnt + 1
                    else :
                        win_cnt = win_cnt + 1
                    buy_price = 0
                    print("win_cnt loss_cnt code date sel_price profit Shares_value balance")
                    print(win_cnt, loss_cnt, df.loc[i,'ts_code'], df.loc[i,'trade_date'],sel_price,profit,Shares_value,balance)
