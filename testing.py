#import pandas as pd
#netflow_df = pd.read_csv("netflow.csv", nrows=100)
#print(list(netflow_df.columns.values))

i = 0
ma = 4

with open("netflow.csv") as myfile:
    head = [next(myfile) for x in range(10)]

print(head)