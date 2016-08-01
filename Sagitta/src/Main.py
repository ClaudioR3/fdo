'''
Created on 04 lug 2016

@author: claudio
'''


from OperationFactory import *

#initialization
q=Query()
# create and perform the operation
op=OperationFactory().find_op()
print op.run(q)