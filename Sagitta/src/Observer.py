'''
Created on 03 ago 2016

@author: claudio
'''

class Subscriber:
    def __init__(self,name):
        self.name=name
    def update(self,message):
        if self.name=="Terminal":
            print message,

class Publisher:
    def __init__(self):
        self.subscribers=[]
    def register(self,subscriber):
        self.subscribers.append(subscriber)
    def remove_subscriber(self,subscriber):
        self.subscribers.remove(subscriber)
    def dispatch(self,message):
        for s in self.subscribers:
            s.update(message) 