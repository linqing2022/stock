import pandas as pd
df = pd.DataFrame({'A': [6, 87,  2], 'B': [26, 5, 30], 'C': [69, 89, 2]})
print(df.corr(method='pearson')) # 默认是pearson，可以不写直接用df2.corr()
