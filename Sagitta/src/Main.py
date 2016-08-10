'''
Created on 04 lug 2016

@author: claudio
'''


from OperationFactory import ReflectionOperationFactory
from Observer import Subscriber

#initialization
s=Subscriber("Terminal")
# create and perform the operation
op=ReflectionOperationFactory().find_op([s])
op.run()