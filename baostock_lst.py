# -*- coding: utf-8 -*-
import shutil

import baostock as bs
import pandas as pd
import os
from pytdx.hq import TdxHq_API
import numpy as np
from datetime import date, timedelta
import math
from pytdx.params import TDXParams
from datetime import datetime

download_start_time = '2015-01-01'
deal_start_time = '2015-01-01'
root_path = os.getcwd()
print(root_path)
list_path = root_path + "\\list.csv"
file_path = root_path + "\\k_day.csv"
limit_up_file_path = root_path + "\\limit_up.csv"
trading_day_path = root_path + "\\trading_day.csv"
deal_path = root_path + "\\deal.csv"
white_path  = root_path + "\\white.csv"
# 登录 Baostock 系统
bs.login()

STOCKNUM = 10000
sh_stock = [''] * STOCKNUM
sz_stock = [''] * STOCKNUM
all_stock = [''] * STOCKNUM
all_count = 0

api = TdxHq_API()
# 获取今天的日期
today = date.today() - timedelta(days=1)

# 将日期格式化为字符串
baostock_today = today.strftime("%Y-%m-%d")

head_col = [
    ['code', 'name', 'buy_date', 'buy_price','sel_date', 'sel_price', 'total_money', 'up_down'],
]
deal_df = pd.DataFrame(columns=head_col[0])
if os.path.exists(deal_path):
    os.remove(deal_path)
deal_df.to_csv(deal_path)
deal_col = 0

def baostock_all_lst():
    # 获取所有 A 股股票信息
    rs = bs.query_all_stock(day=baostock_today)
    # 显示结果集
    all_stock_list = []
    while (rs.error_code == '0') & rs.next():
        # 获取一条记录，将记录合并在一起
        all_stock_list.append(rs.get_row_data())

    if not all_stock_list:
        return
    # 结果转化为 DataFrame 格式
    all_stock_df = pd.DataFrame(all_stock_list, columns=rs.fields)
    all_stock_df.to_csv(list_path)

def baostock_lst():
    k = m = n = 0
    df = pd.read_csv(list_path)
    all_count = len(df)

    for i in range(all_count):
        stock_code = df.iloc[i]['code']
        if ('sh.60' in stock_code) or ('sh.68' in stock_code):
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
    if os.path.exists(file_path):
        os.remove(file_path)
    df = pd.read_csv(list_path)
    all_count = len(df)
    for i in range(all_count):
        rs = bs.query_history_k_data_plus(all_stock[i],
                                     "date,code,open,high,low,close,volume",
                                     start_date=download_start_time,
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
    df['limit_up_turnover_count'] = 0
    df['white'] = 0
    df.to_csv(file_path, index=False)

def is_limit_up(old_price, new_price, precent):
        old_price = math.floor(float(old_price) * 100) / 100
        old_price = f"{old_price:.2f}"
        price_trim = float(old_price) * float(precent)
        price_trim = math.floor(price_trim * 100) / 100
        price_trim = f"{price_trim:.2f}"
        new_price = f"{new_price:.2f}"
        if float(price_trim) <= float(new_price):
            return 1
        else :
            return 0

def count_limit_up():
    df = pd.read_csv(file_path)
    count_csv = len(df)
    i = 1
    old_code = df.iloc[0]['code']
    old_date = df.iloc[0]['date']
    old_close = float(df.iloc[0]['close'])
    limit_up_count = 0
    limit_up_turnover_count = 0
    while i < count_csv:
        new_code = df.iloc[i]['code']
        new_date = df.iloc[i]['date']
        new_close = float(df.iloc[i]['close'])
        new_open = float(df.iloc[i]['open'])
        if (new_code != old_code) :
            old_code = df.iloc[i]['code']
            old_date = df.iloc[i]['date']
            old_close = float(df.iloc[i]['close'])
        if ('sz.30' in new_code) or ('sh.68' in new_code):
            precent = 1.2
        else :
            precent = 1.1
        old_close = math.floor(float(old_close) * 100) / 100
        old_close = f"{old_close:.2f}"
        price_trim = float(old_close) * float(precent)
        price_trim = math.floor(price_trim * 100) / 100
        price_trim = f"{price_trim:.2f}"
        new_close = f"{new_close:.2f}"
        new_open = f"{new_open:.2f}"
        if float(price_trim) <= float(new_close):
            limit_up_count += 1
            if float(price_trim) > float(new_open):
                limit_up_turnover_count += 1
            df.at[i, 'limit_up_count'] = limit_up_count
            df.at[i, 'limit_up_turnover_count'] = limit_up_turnover_count
           
        else :
            limit_up_count = 0
            limit_up_turnover_count = 0
        old_code = new_code
        old_date = new_date
        old_close = float(df.iloc[i]['close'])
        i += 1
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
    df = df.sort_values(by=['date', 'limit_up_count', 'limit_up_turnover_count'], ascending=[True, False, False])
    df.to_csv(limit_up_file_path, index=False)

def limit_up_add_name():
    df = pd.read_csv(limit_up_file_path)
    list_df = pd.read_csv(list_path)
    df['code_name'] = ''
    for i in range(len(df)):
        j = list_df.loc[list_df['code'] == df.iloc[i]['code']].index.tolist()
        if j[0] >= len(df):
            break
        df.at[i, 'code_name'] = list_df.loc[j[0], 'code_name']
    df.to_csv(limit_up_file_path)

def white_list():
    df = pd.read_csv(limit_up_file_path)
    old_date = df.loc[0]['date']
    j = 0
    highest = df.loc[0]['limit_up_count']
    df.at[0, 'white'] = 1
    i = 1
    while i < len(df):
        new_date = df.loc[i]['date']
        if (new_date != old_date) :
            old_date = new_date
            j += 1
            df.at[i, 'white'] = 1
            highest = df.loc[i]['limit_up_count']
            continue
        if (highest == df.loc[i]['limit_up_count']):
            df.at[i ,'white'] = 1
        i += 1
    df = df[df['white'] != 0]
    df.to_csv(limit_up_file_path, index=False)
    
def download_detail_day(pytdx_today,code):
    detail_file_path = root_path + "\\detail\\" + pytdx_today
    if not os.path.exists(detail_file_path):
        os.makedirs(detail_file_path)
        new_path = 1
    else :
        new_path = 0

    detail_file_path = detail_file_path + "\\detail.csv"
    pytdx_today = int(pytdx_today)
    if ('sz' in code):
        market = 0
    else:
        market = 1
    j = 5
    code = code[-6:]
    while j >= 0:
        detail_df = api.to_df(api.get_history_transaction_data(market, code, j * 1000, 1000, pytdx_today))
        if len(detail_df) > 1:
            detail_df['date'] = pytdx_today
            detail_df['code'] = code
            if (new_path == 1) :
                detail_df.to_csv(detail_file_path, mode='a', index=False)
                new_path = 0
            else :
                detail_df.to_csv(detail_file_path, mode='a', index=False, header=False)
        j-=1

def trade_dates():
    start_date = download_start_time
    end_date = baostock_today
    rs = bs.query_trade_dates(start_date=start_date, end_date=end_date)
    # 解析结果
    data_list = []
    while (rs.error_code == '0') & rs.next():
        data_list.append(rs.get_row_data())

    result = pd.DataFrame(data_list, columns=rs.fields)
    result = result[result['is_trading_day'] != '0']
    result.reset_index()
    result.to_csv(trading_day_path)

def get_directory_names(path):
    # 获取指定路径下的所有文件和目录
    entries = os.listdir(path)
    # 过滤出只有目录的名称
    directories = [entry for entry in entries if os.path.isdir(os.path.join(path, entry))]
    return directories

def download_detail():
    trade_dates()
    detail_file_path = root_path + "\\detail\\"
    directories = get_directory_names(detail_file_path)
    chars_to_remove = "-"
    # 使用列表推导式过滤掉不需要的字符
    filtered_chars = [char for char in download_start_time if char not in chars_to_remove]
    # 将列表转换回字符串
    remove_start_date = ''.join(filtered_chars)
    for i in range(len(directories)):
        if(directories[i] >= remove_start_date):
            tmp_path = detail_file_path + directories[i]
            shutil.rmtree(tmp_path)

    result = pd.read_csv(trading_day_path)
    df = pd.read_csv(limit_up_file_path)
    i = 0
    for i in range(len(df)):
        pytdx_today = df.iloc[i]['date']
        if (pytdx_today >= download_start_time):
            break

    with api.connect('119.147.212.81', 7709):
        while i < len(df):
            code = df.iloc[i]['code']
            pytdx_today = df.iloc[i]['date']
            j = result.loc[result['calendar_date'] == pytdx_today].index.tolist()
            pytdx_today = result.loc[j[0], 'calendar_date']
            pytdx_today = datetime.strptime(pytdx_today, "%Y-%m-%d")
            pytdx_today = pytdx_today.strftime("%Y%m%d")
            download_detail_day(pytdx_today,code)
            i+=1

def spilt_k_day_by_code():
    split_file_path = root_path + "\\spilt\\"
    if not os.path.exists(split_file_path):
        os.makedirs(split_file_path)
    df = pd.read_csv(file_path)
    count = len(df)
    old_code = df.iloc[0]['code']
    i = 1
    j = 0
    while i < count:
        new_code = df.iloc[i]['code']
        new_df= df.iloc[i].copy()
        if (old_code != new_code):
            new_df = df.iloc[j:i-1].copy()
            new_path = split_file_path + old_code + "_k_day.csv"
            new_df.to_csv(new_path)
            old_code = new_code
            j = i
        i+=1

total_money = 10000

# 昨天最高板的几个股票；选择今天开盘不涨停又最快盘中涨停的买入；
# 今天收盘不涨停，直接卖出；今天收盘涨停，继续持有，直到某天开盘跌或盘中不涨后卖出
def deal_code_1(code,name,date,old_close,start_row):
    global total_money
    global deal_col 
    split_file_path = root_path + "\\spilt\\"
    k_day_path = split_file_path + code + "_k_day.csv"
    k_day_df = pd.read_csv(k_day_path)
    for i in range(len(k_day_df)):
        now_date = k_day_df.iloc[i]['date']
        if (date == now_date):
            break
    if i == len(k_day_df) -1:
        return date
    if ('sz.30' in code) or ('sh.68' in code):
            precent = 1.2
    else :
            precent = 1.1
    open = k_day_df.iloc[i+1]['open']
    buy_date = k_day_df.iloc[i+1]['date']
    limit_flag = is_limit_up(old_close,open,precent)
    high = k_day_df.iloc[i+1]['high']
    high_limit_flag = is_limit_up(old_close, high, precent)
    if limit_flag == 0 and high_limit_flag == 1:
        old_money = total_money
        buy_price = open
        j = 0
        while i+j+2 < len(k_day_df):    
            old_close = k_day_df.iloc[i+j]['close']
            buy_close = k_day_df.iloc[i+j+1]['close']
            sel = k_day_df.iloc[i + j]['close']
            sel_date = k_day_df.iloc[i+j+1]['date']
            limit_flag = is_limit_up(old_close,buy_close,precent)
            if limit_flag == 0:
                if j > 0 :
                    sel_price = sel
                else :
                    sel_price = k_day_df.iloc[i+j+2]['open']
                    sel_date = k_day_df.iloc[i+j+2]['date']
                total_money *= sel_price / buy_price
                total_money = int(total_money)
                up_down = ((total_money - old_money) / old_money) * 100
                up_down = int(up_down)
                str = f"{code},{name},{buy_date},{buy_price},{sel_date},{sel_price},{total_money},{up_down}%"
                print(str)
                deal_df.at[deal_col,'code'] = code
                deal_df.at[deal_col,'name'] = name
                deal_df.at[deal_col,'buy_date'] = buy_date
                deal_df.at[deal_col,'buy_price'] = buy_price
                deal_df.at[deal_col,'sel_date'] = sel_date
                deal_df.at[deal_col,'sel_price'] = sel_price
                deal_df.at[deal_col,'total_money'] = total_money
                deal_df.at[deal_col,'up_down'] = up_down
                deal_col += 1
                return sel_date  
            j +=1
    return buy_date

def deal_day(start_row):
    limit_up_df = pd.read_csv(limit_up_file_path)
    count = len(limit_up_df)
    old_date = limit_up_df.iloc[start_row]['date']
    old_close = limit_up_df.iloc[start_row]['close']
    code = limit_up_df.iloc[start_row]['code']
    name = limit_up_df.iloc[start_row]['code_name']
    deal_date = deal_code_1(code,name,old_date,old_close,start_row)

    while start_row < count:
        new_date = limit_up_df.iloc[start_row]['date']
        if (deal_date <= new_date) :
            return start_row
        start_row += 1
    return start_row

def stock_deal():
    year  = int(deal_start_time[0:4])
    month = int(deal_start_time[5:7])
    day = int(deal_start_time[8:10])
    start_day = date(year,month,day)
    end_day = date.today()
    delta = end_day - start_day
    limit_up_df = pd.read_csv(limit_up_file_path)
    start_row = 0
    count = len(limit_up_df)

    for i in range(count):
        now_date = limit_up_df.iloc[i]['date']
        if (deal_start_time <= now_date):
            start_row = i
            break

    for i in range(delta.days):
        if start_row >= count:
            return
        start_row = deal_day(start_row)
    deal_df.to_csv(deal_path,index=False)

def prepare_k_day():
    baostock_all_lst()
    baostock_lst()
    baostock_k_day()
    data_clear()
    spilt_k_day_by_code()

def deal_limit_up():
    insert_col()
    count_limit_up()
    save_limit_up()
    sort_by_date_limit_up_count()
    limit_up_add_name()
    white_list()

if __name__ == "__main__":
    #准备股票原始k线数据
    # prepare_k_day()

    #计算涨停股票
    # deal_limit_up()

    #下载涨停股票的下一日交易明细
    download_detail()

    #交易策略
    stock_deal()



