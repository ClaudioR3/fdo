'''
Created on 03 ago 2016

@author: claudio
'''

class Subscriber:
    '''
    This class works like a messages receiver. To receive the message, the class need subscribe to the 
    list of Publisher class.
    '''
    def __init__(self,name,event='terminal'):
        self.name=name
        self.event=event
    def update(self,message):
        '''
        Update status of Subscriber object. In default update the function prints message. 
        @param message: string.
        '''
        print message,

class Publisher:
    '''
    This class works like a messages sender. The class sends the messages to all Subscribers objects of own list.
    '''
    def __init__(self):
        self.subscribers=[]
    def register(self,subscriber):
        '''
        Add Subscriber object in own subscribers list.
        @param subscriber: Subscriber object.
        '''
        self.subscribers.append(subscriber)
    def remove_subscriber(self,subscriber):
        '''
        Remove Subscriber object in own subscriber list.
        @param subscriber: Subscriber object.
        '''
        self.subscribers.remove(subscriber)
    def dispatch(self,message,event='terminal'):
        '''
        Send the message to all Subscriber object only if Subscriber object has event attribute equals to event value
        @param message: string.
        @param event: string, event of subscriber
        '''
        for s in self.subscribers:
            if s.event==event:
                s.update(message) 