import pandas as pd

# Define two dataframes
df1 = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6], 'C':[10, 11, 12]})
df2 = pd.DataFrame({'A': None, 'D': [10, 11, 12]})


df2.update(df1)


# concaaatenate the two dataframes
df2 = pd.merge(df1, df2)
print(df2)
