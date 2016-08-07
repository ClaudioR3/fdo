'''
Created on 04 lug 2016

@author: claudio
'''


from OperationFactory import *

#initialization
q=Query()
s=Subscriber("Terminal")
# create and perform the operation
op=OperationFactory().find_op([s])
print op.run(q)