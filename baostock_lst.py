# -*- coding: utf-8 -*-
import baostock as bs
import pandas as pd
import csv
import os
from pytdx.hq import TdxHq_API
import numpy as np
from datetime import date, timedelta

root_path = os.getcwd()
print(root_path)
file_path = root_path + "/k_day.csv"
limit_up_file_path = root_path + "/limit_up.csv"

bs.login()
stock_df = bs.query_all_stock(date).get_data()
df = pd.DataFrame(stock_df)
count = len(df)
sh_stock = [''] * count
sz_stock = [''] * count
all_stock = [''] * count

api = TdxHq_API()

# 获取今天的日期
today = date.today()
# 将日期格式化为字符串
baostock_today = today.strftime("%Y-%m-%d")

money = 0
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
                                     end_date= baostock_today,
                                     frequency="d", adjustflag="2")

        df = rs.get_data()
        if not df.empty:
            df.to_csv(file_path, mode='a', index=False)
    print(f"Data saved to {file_path}")

def data_clear():
    df = pd.read_csv(file_path)
    count_csv = len(df)
    del_id = 0
    while del_id < count_csv:
            stock_code = df.iloc[del_id]['code']
            stock_date = df.iloc[del_id]['date']
            if ('sh' in stock_code) and ('sh.60' not in stock_code):
                df.loc[del_id, 'code'] = np.nan
            elif ('code' in stock_code):
                df.loc[del_id, 'code'] = np.nan
            del_id +=1
    df = df.dropna()
    df.to_csv(file_path, index=False)

def insert_col():
    df = pd.read_csv(file_path)
    df['limit_up_count'] = 0
    df.to_csv(file_path, index=False)

def count_limit_up():
    df = pd.read_csv(file_path)
    count_csv = len(df)
    i = 1
    old_code = df.iloc[0]['code']
    old_date = df.iloc[0]['date']
    old_close = float(df.iloc[0]['close'])

    while i < count_csv:
        new_code = df.iloc[i]['code']
        new_date = df.iloc[i]['date']
        new_close = float(df.iloc[i]['close'])

        if (new_code != old_code) :
            old_code = df.iloc[i]['code']
            old_date = df.iloc[i]['date']
            old_close = float(df.iloc[i]['close'])
        change_percent = (new_close - old_close) / old_close * 100
        if ('sz.30' in new_code):
            precent = 20
        else :
            precent = 10
        if change_percent >= precent:
            limit_up_count = int(df.iloc[i]['limit_up_count'])
            limit_up_count += 1
            df.at[i, 'limit_up_count'] = limit_up_count
        else :
            limit_up_count = 0
        i += 1
        old_code = new_code
        old_date = new_date
        old_close = new_close
    df.to_csv(limit_up_file_path, index=False)

def save_limit_up():
    df = pd.read_csv(limit_up_file_path)
    # 会生成一个与原DataFrame形状相同的布尔型DataFrame，其中所有非0值被替换为True，0
    # 值被替换为False。.all(axis=1)
    # 则沿着行方向检查，如果某一行全部为True（即不包含0值），则保留该行。
    df = df[df['limit_up_count'] != 0]
    df.to_csv(limit_up_file_path, index=False)

def sort_by_date_limit_up_count():
    df = pd.read_csv(limit_up_file_path)
    df = df.sort_values(by=['date', 'limit_up_count'], ascending=[True, False])
    df.to_csv(limit_up_file_path, index=False)

def download_detail_day(pytdx_today):
    df = pd.read_csv(limit_up_file_path)
    detail_file_path = root_path + "/" + pytdx_today + "/detail.csv"
    if not os.path.exists(detail_file_path):
        os.makedirs(detail_file_path)
    with api.connect('119.147.212.81', 7709):
        for i in range(len(df)) :
            code = df.iloc[i]['code']
            if ('sz' in new_code):
                market = 0
            else:
                market = 1
            for j in range(5):
                df = api.get_history_transaction_data(market, code, j * 1000, 1000, pytdx_today)
                df['date'] = pytdx_today
                df.to_csv(detail_file_path, mode='a', index=False)

def download_detail():
    today = date.today()
    delta = today - date(20230101)
    pytdx_today = today.strftime("%Y%m%d")
    for i in range(delta):
        download_detail_day(pytdx_today)
        today = today - timedelta(days=1)

def spilt_k_day_by_code():
    df = pd.read_csv(file_path)
    count = len(df)

    new_df = pd.DataFrame()
    new_df['date'] = 'date'
    new_df['open'] = 'open'
    new_df['close'] = 'close'
    new_df['high'] = 'high'
    new_df['low'] = 'low'
    new_df.iloc[0]['date'] = df.iloc[0]['date']
    new_df.iloc[0]['open'] = df.iloc[0]['open']
    new_df.iloc[0]['close'] = df.iloc[0]['close']
    new_df.iloc[0]['high'] = df.iloc[0]['high']
    new_df.iloc[0]['low'] = df.iloc[0]['low']

    old_code = df.iloc[0]['code']
    j = i = 0
    while i < count:
        new_code = df.iloc[i+1]['code']
        new_df.iloc[j]['date'] = df.iloc[i]['date']
        new_df.iloc[j]['open'] = df.iloc[i]['open']
        new_df.iloc[j]['close'] = df.iloc[i]['close']
        new_df.iloc[j]['high'] = df.iloc[i]['high']
        new_df.iloc[j]['low'] = df.iloc[i]['low']

        if (old_code != new_code):
            new_path = file_path + "/" + old_code + "k_day.csv"
            new_df.to_csv(new_path)
            old_code = new_code
            j = 0
        i+=1
        j+=1

def deal_day(day,start_row):
    limit_up_df = pd.read_csv(limit_up_file_path)
    count = len(limit_up_df)
    old_date = limit_up_df.iloc[start_row]['date']
    old_close = limit_up_df.iloc[start_row]['close']
    code = limit_up_df.iloc[start_row]['code']
    new_path = file_path + "/" + code + "k_day.csv"
    # print("buy price:" {})
    i = 1
    start_day = day + timedelta(days=1)
    while start_row < count:
        new_date = limit_up_df.iloc[i]['date']
        if (old_date != new_date) :
            return i;
        start_row += 1


def deal():
    start_day = date(20230101)
    end_day = date.today()
    delta = end_day - start_day
    start_row = 0
    for i in range(delta):
        start_row = deal_day(start_day,start_row)
        start_day = start_day + timedelta(days=1)

if __name__ == "__main__":
    # baostock_lst()
    # baostock_k_day()
    # data_clear()
    # insert_col()
    # count_limit_up()
    save_limit_up()
    # sort_by_date_limit_up_count()
    # download_detail()
    # spilt_k_day_by_code()
    # deal()



