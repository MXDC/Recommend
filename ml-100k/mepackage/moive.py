# coding=utf-8
__author__='changgy'
__date__  ='20160205'

import sys
import math
import numpy as np
import pandas as pd
import datetime

''' 
u.data 表示100k条评分记录，每一列的数值含义是：
user_id | item_id | rating | timestamp

u.user表示用户的信息，每一列的数值含义是：
user_id | age | gender | occupation | zip code

u.item文件表示电影的相关信息，每一列的数值含义是
item_id| movie title | release date | video release date |IMDb URL | unknown | Action | Adventure | Animation 
| Children's | Comedy | Crime | Documentary | Drama | Fantasy |Film-Noir | Horror | Musical | Mystery | Romance
| Sci-Fi |Thriller | War | Western |
'''

 
global g_DF_UDATA
global g_DF_USER
global g_DF_MOVIE_ITEM


def readSrcData(fileDir,sep, columns):
	loop = True
	chunkSize = 100000	
	chunks = []
	#reader = pd.read_csv('u.data', sep='\t', iterator=True)
	
	reader = pd.read_table(fileDir,sep, header=None,iterator=True)
	i = 0
	
	while loop:
		try:
			chunk = reader.get_chunk(chunkSize)
			chunks.append(chunk)
		except StopIteration:
			loop = False
			#print 'iteration is stoppd'
		
	df = pd.concat(chunks,ignore_index=True)
	df.columns=columns
	return df

class CcalcCostTime():
	#d1,d2

	def __init__(self):
		self.d1 = datetime.datetime.now()
		
	def __del__(self):
		d2 = datetime.datetime.now()
		print 'cost is ',(d2-self.d1).seconds ,'s ',(d2-self.d1).microseconds / 1000 ,'ms'
	
	
def getUserDataFramebyGender(user_all, gender):
	# dataframe to series
	series_user = user_all[ user_all[gender] > 0 ][gender]
	
	# series to dataFrame
	return pd.DataFrame(series_user)

def readData():
	global g_DF_UDATA
	global g_DF_USER
	global g_DF_MOVIE_ITEM
	
  # 读取udata文件	
	g_DF_UDATA = readSrcData('u.data','\t',['user_id','item_id','rating','timestamp'] )
	
	# 读取u.user文件
	g_DF_USER= readSrcData('u.user','|', ['user_id','age','gender','occupation','zip code'] )
	
	# 读取u.item文件
	g_DF_MOVIE_ITEM= readSrcData('u.item','|', ['item_id','title','release','video_release','url','unknown',' Action',' Adventure',' Animation','Children',' Comedy',' Crime',' Documentary',' Drama',' Fantasy','Film-Noir',' Horror',' Musical',' Mystery',' Romance','Sci-Fi','Thriller',' War',' Western'] )

def pivot_work():
	coster = CcalcCostTime()
	
	# 建立透视表，提取女性用户
	g_DF_USER_all = pd.pivot_table(g_DF_USER, index=g_DF_USER.index, values='user_id', columns='gender',fill_value=0)
	g_DF_USER_wm = getUserDataFramebyGender(g_DF_USER_all , 'F')	
	merger = pd.merge(g_DF_UDATA, g_DF_USER_wm, left_on='user_id', right_on='F', how='left')	
	va = np.vstack(merger[ merger['F'] > 0 ] ['rating'])
	#print '\n, merge len', len(va), '\navg',va.mean(), '\nvar', va.var(), '\nstd', va.std()

		
	# 提取男性用户
	g_DF_USER_m = getUserDataFramebyGender(g_DF_USER_all , 'M')
	merger = pd.merge(g_DF_UDATA, g_DF_USER_m, left_on='user_id', right_on='M', how='left')	
	va2 = np.vstack(merger[ merger['M'] > 0 ] ['rating'])
	#print '\n, merge len', len(va2), '\navg',va2.mean(), '\nvar', va2.var(), '\nstd', va2.std()

	
	# 读取u.item件
	#df_item = readSrcData('u.item','|')
	#print 'item\n', df_item.head(5)
	
	''' 计算男女对电影评分的标准差'''
	F_StandardDiff =  va.std() #1.0
	M_StandardDiff =  va2.std() #2.0
	ser_result = pd.Series({'F':F_StandardDiff, 'M':M_StandardDiff})
	ser_result.name='rating'
	print '\n\ngender\n',ser_result

def merge_work():
	coster = CcalcCostTime()
	
  # merge udata and user table
	merger = pd.merge(g_DF_UDATA, g_DF_USER, on='user_id')	
	#print merger[merger.item_id == 196].head(20)q
	
	# group by  gender, then std who is a dataframe
	by_rating = merger.groupby('gender') ['rating'].std() 	
	print '\nend \n', by_rating
	
	#gd = merger.groupby('gender')
	#print gd.get_group('F').head(6)

#
#	计算平均分
#	输入是list, item是['movitemid','rating']
#
def calcAvg(movies):
	avg = 0
	for item in movies:
		avg += item[1]
	return (avg * 1.0 / len(movies) )	

#
#  基于jaccard，计算相似度, J=|A∩B|/|A∪B|
#
def calcSimilarByJaccard(usermovies, nearmovies):
	user_avg = calcAvg( usermovies )	
	near_avg = calcAvg( nearmovies )
	
	u1_u2 = 0 # 交集个数
	for u1 in usermovies:
		for u2 in nearmovies:
			if u1[0] == u2[0] and u1[1] > user_avg and u2[1] > near_avg:
				u1_u2 += 1
	
	u12 = len( usermovies )	+ len( nearmovies ) - u1_u2	
	return (u1_u2 * 1.0 / u12)
				

#
#   使用 |A&B|/sqrt(|A || B |)计算余弦距离
#
def calcCosDistSpe(usermovies, nearmovies):
	user_avg = calcAvg( usermovies )	
	near_avg = calcAvg( nearmovies )
	
	u1_u2 = 0 # 交集个数
	for u1 in usermovies:
		for u2 in nearmovies:
			if u1[0] == u2[0] and u1[1] > user_avg and u2[1] > near_avg:
				u1_u2 += 1
					
	u1u2=len(usermovies)*len(nearmovies)*1.0
	return u1_u2/math.sqrt(u1u2)

#
#   计算余弦距离
#
#
def calcCosDist(user1,user2):
    sum_x=0.0
    sum_y=0.0
    sum_xy=0.0
    for key1 in user1:
        for key2 in user2:
            if key1[0]==key2[0] :
                sum_xy+=key1[1]*key2[1]
                sum_y+=key2[1]*key2[1]
                sum_x+=key1[1]*key1[1]
    
    if sum_xy == 0.0 :
        return 0
    sx_sy=math.sqrt(sum_x*sum_y) 
    return sum_xy/sx_sy


#
#
#   相似余弦距离
#
#
#
def calcSimlaryCosDist(user1,user2):
    sum_x=0.0
    sum_y=0.0
    sum_xy=0.0
    avg_x=0.0
    avg_y=0.0
    for key in user1:
        avg_x+=key[1]
    avg_x=avg_x/len(user1)
    
    for key in user2:
        avg_y+=key[1]
    avg_y=avg_y/len(user2)
    
    for key1 in user1:
        for key2 in user2:
            if key1[0]==key2[0] :
                sum_xy+=(key1[1]-avg_x)*(key2[1]-avg_y)
                sum_y+=(key2[1]-avg_y)*(key2[1]-avg_y)
        sum_x+=(key1[1]-avg_x)*(key1[1]-avg_x)
    
    if sum_xy == 0.0 :
        return 0
    sx_sy=math.sqrt(sum_x*sum_y) 
    return sum_xy/sx_sy
    

#		计算最近的K个邻居
def calcNears(merger, userid, k = 6):
	#coster = CcalcCostTime()
	nears = []
	nears_list = []		
	user2items =  merger[merger.user_id == userid]['item_id'].values
	
	#1. 获取所有邻居
	for item in user2items:
		item2users =  merger[merger.item_id == item]['user_id'].values	
			
		for user in item2users:
			if user != userid and user not in nears:
				nears.append(user)
				
	#2. 计算每个邻居的邻近度
	usermovies = (merger[merger.user_id == userid]).loc[ :,['item_id','rating'] ].values
	for near in nears:
		# near to movies what he have seen
		nearmovies = (merger[merger.user_id == near]).loc[ :,['item_id','rating'] ].values
		
		dist = calcCosDistSpe(usermovies, nearmovies) #calcSimilarByJaccard(usermovies, nearmovies)
		nears_list.append([dist, near])
	
	#3. 相近度排序
	nears_list.sort(reverse=True)
	return nears_list[:k]
	
# 基于交集，测试
def testPrint(nears, merger,userid):
	print 'user ', userid
	print (merger[merger.user_id == userid ]) ['item_id'].values
	alist = (merger[merger.user_id == userid ]) ['item_id'].values
			
	for item in nears:
		print 'near ', item[1]
		print (merger[merger.user_id == item[1] ]) ['item_id'].values
		b = ( (merger[merger.user_id == item[1] ]) ['item_id'].values )
		alist = list( set(alist) & set(b) )
	print 'intersection:\n', alist
		
#
#   使用UserFC进行推荐
#   输入：user 和 data评分表的合并表, 邻居的数量, 希望推荐的数量
#   输出：推荐的电影ID,输入用户的电影列表,电影对应用户的反序表，邻居列表
#
def recommendByUserFC(userid, k=3, wantedNum=5):
	coster = CcalcCostTime()

	#1. 建立user id到看过的电影映射; 电影id 到user的映射；本次使用df
	merger = pd.merge(g_DF_UDATA, g_DF_USER, on='user_id')
				
	#2. 计算最近的K个邻居
	nears = calcNears(merger, userid, k)
	
	#testPrint(nears, merger,userid)
	
	#3. 对邻居的所有看过的电影，基于邻近情况，计算推荐度
	movieitems_dist={}
	for item in nears:
		nearmovies = (merger[merger.user_id == item[1] ]) ['item_id'].values
		for movie in nearmovies:		
			if movieitems_dist.has_key(movie):
				movieitems_dist[movie] += item[0]
			else:
				movieitems_dist[movie] = item[0]

  #4. 基于推荐度，进行排序
	SeriesMovies = pd.Series(movieitems_dist).sort_values()
	print '\n', SeriesMovies.tail(wantedNum)
	
	#5. 输出
	recommned_moiveID_df = pd.DataFrame(SeriesMovies.tail(wantedNum).keys(), columns=['item_id'] )
	recomm_merger = pd.merge(g_DF_MOVIE_ITEM, recommned_moiveID_df, on='item_id')
	print '\n\n', userid,'\'s recom list:\n', recomm_merger.loc[:, ['item_id','title','release'] ]
	
	
def test():
	coster = CcalcCostTime()
	
	#1. read data
	readData()
	
	#2. 协调推荐	
	for item in xrange(1):
		recommendByUserFC(item+1, 3, 10)
		
if __name__ == '__main__':
	
	test()
	
	#pivot_work()
	#merge_work()
	
