# authors = Kanonpy
# coding=UTF-8
import pandas as pd
import numpy as np
import os
from scipy.optimize import linprog
 
distance = pd.read_excel('Distance.xlsx')
 
df = pd.read_excel('chuli.xls')
 
def commuteCalcu(pop,dist,name):
    #保证pop和dist行列数值相等
    intdistcolumns = {d:int(float(d)) for d in dist.columns}
    intdistindex = {d:int(float(d)) for d in dist.index}
    unicodepopcolumns = {d:unicode(d) for d in pop.columns}
    unicodepopindex = {d:unicode(d) for d in pop.index}
    for d in dist.columns:
        if d not in pop.columns:
            dist = dist.drop(d,axis=1)
            #print 'the col %s in distance was del'%(str(d))
       
    for i in dist.index:
        if i not in pop.index:
            dist = dist.drop(i,axis=0)
            #print 'the col %s in distance was del '%(str(i))
       
    for d in pop.columns:
        if d not in dist.columns:
            pop = pop.drop(d,axis=1)
            #print 'the col %s in distance was del '%(str(i))
       
    for i in pop.index:
        if i not in dist.index:
            pop = pop.drop(i,axis=0)
            #print 'the col %s in distance was del '%(str(i))
 
    if not os.path.exists(u'%s'%(name)):
        os.mkdir(u'%s'%(name))
        print u'creat %s_%s file'%(col,i)
    dist.to_excel(u'%s/Population.xlsx'%(name))
    pop.to_excel(u'%s/Distance.xlsx'%(name))
    matrix = np.array(pop)*np.array(dist)
    total_commute = matrix.sum()
    commute = total_commute/np.array(pop).sum()
    print u'%s 总通勤距离为 %s'%(name,unicode(total_commute))
    print u'%s 通勤距离(ARC)为 %s'%(name,unicode(commute))
    print u'%s 人口总数为 %s'%(name,unicode(np.array(pop).sum()))
 
 
for col in [u'性别', u'户籍', u'职业', u'收入']:
        for i in df.groupby(col).size().index:
            species = df[df[col]==i]
            pt = pd.pivot_table(data=species,values=col,rows=u'工作地或学校地址',
                                cols=u'居住小区',aggfunc=np.size,fill_value=0)
            if sum(pt.shape) > 10:
                commuteCalcu(pt,distance,col+u'中的'+unicode(i))
            else:
                print '%s_%s is too small'%(col,i)