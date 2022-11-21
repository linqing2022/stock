import pandas as pd
code = '601636.SH'
# dir_path = 'd:\\data\\tushare\\hfq_fina\\002466.SZ.csv'
dir_path = 'd:\\data\\tushare\\hfq_fina\\' + code + '.csv'
corr_path = 'd:\\data\\tushare\\corr\\' + code + '.csv'
df = pd.read_csv(dir_path,error_bad_lines=False,low_memory=False) 
df2 = df.corr(method='pearson')
df2.to_csv(corr_path)
#df2 = df.open.corr(df.ps_ttm,method='pearson')
#print(df2)
