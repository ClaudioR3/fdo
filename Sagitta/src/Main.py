'''
Created on 04 lug 2016

@author: claudio
'''


from OperationFactory import ReflectionOperationFactory
from Query import Query
from Observer import Subscriber

#initialization
q=Query()
s=Subscriber("Terminal")
# create and perform the operation
op=ReflectionOperationFactory().find_op([s])
op.run(q)