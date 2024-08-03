# -*- coding: utf-8 -*-
import baostock as bs
import datetime as dt
import pandas as pd

date = "2024-08-01"
file_path = "../k_day_1.csv"

bs.login()
stock_df = bs.query_all_stock(date).get_data()
df = pd.DataFrame(stock_df)
count = len(df)
sh_stock = [''] * count
sz_stock = [''] * count
all_stock = [''] * count

def baostock_lst():
    k = m = n = 0
    for i in range(count):
        stock_code = df.iloc[i]['code']
        if ('sh.60' in stock_code):
            sh_stock[m] = stock_code[-6:]
            all_stock[k] = stock_code
            m += 1
            k += 1
        if ('sz.00' in stock_code[:-4]) or ('sz.30' in stock_code[:-4]) or ('sz.002' in stock_code[:-4]):
            sz_stock[n] = stock_code[-6:]
            all_stock[k] = stock_code
            n += 1
            k += 1
    print(sh_stock[1000])
    print(sz_stock[1000])
    print(all_stock[1000])
    return

def baostock_k_day():
    for i in range(count):
        rs = bs.query_history_k_data_plus(all_stock[i],
                                     "date,code,open,high,low,close,volume",
                                     start_date='2023-01-01',
                                     end_date= date,
                                     frequency="d", adjustflag="2")

        df = rs.get_data()
        if not df.empty:
            df.to_csv(file_path, mode='a', index=False)
    print(f"Data saved to {file_path}")

if __name__ == "__main__":
    baostock_lst()
    baostock_k_day()

