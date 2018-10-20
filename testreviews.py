import pandas as pd

df = pd.read_csv('ffreviews.csv')
print(df['date'].unique())
