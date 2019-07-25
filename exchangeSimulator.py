# -*- coding: utf-8 -*-
"""
Created on Thu Jun 20 10:12:21 2019

@author: hongsong chou
"""

import threading
import os
import time
from common.SingleStockExecution import SingleStockExecution
from multiprocessing import Queue

class ExchangeSimulator:
    
    def __init__(self, marketData_2_exchSim_q, platform_2_exchSim_order_q, exchSim_2_platform_execution_q):
        print("[%d]<<<<< call ExchSim.init" % (os.getpid(),))
        self.quotes = {}
        self.execID = 0
        self.limit_order_q = Queue()
        self.stock_ticker = '2330'
        self.futures_ticker = 'ABC'
        t_md = threading.Thread(name='exchsim.on_md', target=self.consume_md, args=(marketData_2_exchSim_q, exchSim_2_platform_execution_q))
        t_md.start()
        
        t_order = threading.Thread(name='exchsim.on_order', target=self.consume_order, args=(platform_2_exchSim_order_q, exchSim_2_platform_execution_q, ))
        t_order.start()

    def consume_md(self, marketData_2_exchSim_q, exchSim_2_platform_execution_q):
        while True:
            res = marketData_2_exchSim_q.get()
            if res.ticker in self.quotes.keys():
                self.quotes[res.ticker]['ask'] = res.askPrice1
                self.quotes[res.ticker]['bid'] = res.bidPrice1
            else:
                self.quotes[res.ticker] = {'ask':res.askPrice1, 'bid':res.bidPrice1}
                
            qsize = self.limit_order_q.qsize()
            for i in range(qsize):
                limit_order = self.limit_order_q.get()
                self.produce_execution(limit_order, exchSim_2_platform_execution_q)
            print('[%d]ExchSim.consume_md' % (os.getpid()))
            print(res.outputAsDataFrame())
    
    def consume_order(self, platform_2_exchSim_order_q, exchSim_2_platform_execution_q):
        while True:
            res = platform_2_exchSim_order_q.get()
            print('[%d]ExchSim.on_order' % (os.getpid()))
            print(res.outputAsArray())
            self.produce_execution(res, exchSim_2_platform_execution_q)
    
    def execute(self, order, price):
        execution = SingleStockExecution(order.ticker, order.date, time.asctime(time.localtime(time.time())))
        execution.execID = self.execID
        self.execID += 1
        execution.orderID = order.orderID
        execution.direction = order.direction
        execution.price = price
        execution.size = order.size
        return execution
        
    def produce_execution(self, order, exchSim_2_platform_execution_q):
        tkr = order.ticker
        if tkr not in self.quotes.keys():
            self.limit_order_q.put(order)
            return
        if order.type == 'MO':
            if order.direction == 1:
                execution = self.execute(order, self.quotes[tkr]['ask'])
            elif order.direction == -1:
                execution = self.execute(order, self.quotes[tkr]['bid'])
        elif order.type == 'LO':
            if order.direction == 1 and order.price >= self.quotes[tkr]['ask']:
                execution = self.execute(order, self.quotes[tkr]['ask'])
            elif order.direction == -1 and order.price <= self.quotes[tkr]['bid']:
                execution = self.execute(order, self.quotes[tkr]['ask'])
            elif (order.direction == 1 and order.price < self.quotes[tkr]['ask']) or\
                    (order.direction == -1 and order.price > self.quotes[tkr]['bid']):
                self.limit_order_q.put(order)
                return
        exchSim_2_platform_execution_q.put(execution)
        print('[%d]ExchSim.produce_execution' % (os.getpid()))
        print(execution.outputAsArray())
