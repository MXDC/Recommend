#!/usr/bin/env python
#coding=utf-8

#author=‘changgy'
#date='20160302'

from threading import Thread
from mepackage.moive import *


	
def func(userid):
	recommendByUserFC(userid)
	

# 协调过滤推荐worker
class FCWorker(Thread):
	def __init__(self, function, userid):
		Thread.__init__(self)
		self.setDaemon(True)   # 父进程退出后，线程自动退出
 
		self.fuc = function
		self.userid = userid
	
	def run(self):
		self.fuc(self.userid)
	

if  __name__ == '__main__':
	coster = CcalcCostTime()
	print '------start------\n'

  #1. read data
	readData()
	
	#2 theard works
	worklist=[]
	for item in xrange(2): # from 1 start
		worker = FCWorker(func, item+1)
		worker.start()
		worklist.append(worker)
		
	for worker in worklist:
		worker.join()
	
	print '\n------end------\n'