'''
Created on Jul 29, 2016

@author: claudio
'''
from Operations import NotOperation
from Document import Document
import sys
class ReflectionOperationFactory:
    '''
    This class is based on Factory Method with the 'find_op' reflection function.
    '''
    def __init__(self):
        pass
    
    def find_op(self,subscribers):
        '''
        The function takes the second argoment of sys.argv (nameArg) and creates nameArgOperation istance if exists.
        Otherwise, return 'Operation not found'.
        @param subscriber: list, list of Subscriber objects to set all Subscriber objetcs in subscriber attribute in 
        nameArgOperation class, and then save sys.argv in log Document.
        @return object: subclass Operation, return the subclass of Operation with name 'nameArgOperation'
        '''
        try:
            op=sys.argv[1]
            op_name="Operations."+str.upper(op[0])+str.lower(op[1:])+"Operation"
            the_class = self.my_import(op_name)
            objecT=the_class()
            #set other argoments of sys.argv in args attribute of Operation object
            objecT.set_args(sys.argv[2:])
            objecT.subscribers=subscribers
            self.save_operation(sys.argv)
            return objecT
        except :
            #case: operation not found
            n_op=NotOperation()
            n_op.subscribers=subscribers
            return n_op
            
    def save_operation(self,operation):
        '''
        Save the operation in Document object with 'log.txt' as name attribute. It saves last executed operations.
        @param operation: list,sys.argv 
        '''
        log=Document("log.txt")
        operations=log.get_params()
        prev=""
        for arg in operation:
            prev+=arg+" "
        for key in operations:
            tmp=operations[key]
            operations[key]=prev
            prev=tmp
        operations[len(operations)+1]=prev
        log.update(operations)
        
    def my_import(self,name):
        '''
        Define the class in 'name'.
        @param name: string, the path of 'name' class. For example Operazions.HelpOperation
        '''
        components = name.split('.')
        mod = __import__(components[0])
        for comp in components[1:]:
            mod = getattr(mod, comp)
        return mod