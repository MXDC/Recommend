#!-coding=utf-8

import pandas as pd
import numpy as np
import sys
import os

listme=[]
for i in [5,4,3,2]:
	if i < 3 :
		listme.append([i, 'a'])
	else :
			listme.append([i, 'b'])

for i in [15,24,33,72]:
	if i < 3 :
		listme.append([i, 'a'])
	else :
			listme.append([i, 'b'])
			
print 'before list\n', listme
listme.sort(reverse=True)

print 'after list\n', listme

sys.exit(1)


file2 = open('des.txt', 'w+')
file2.write('abc')
file2.close()
os._exit(0)
print 'sys exit'
sys.exit(1)

df = pd.DataFrame({'A' : ['one', 'one', 'two', 'three'] * 3,
'B' : ['A', 'B', 'C'] * 4,'C' : ['foo', 'foo', 'foo', 'bar', 'bar', 'bar'] * 2,
'D' : np.random.randn(12),'E' : np.random.randn(12)})

df_val = pd.DataFrame( {'A':['one','two','three'], 'val':[1,2,3]} )

print 'df is\n', df
print '\ndf-val is\n', df_val


pivot_df = pd.pivot_table(df, values='D', index=['A', 'B'], columns=['C'])
print '\n\n pivoted\n', pivot_df

print '\n\n type\n', type(pivot_df)


print (df[ df['B'] == 'B' ]).loc[:, ['A','B']]

print df.groupby('A')['one']