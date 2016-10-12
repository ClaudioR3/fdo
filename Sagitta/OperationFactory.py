'''
Created on Jul 29, 2016

@author: claudio
'''
from Operations import NotOperation
from Document import Document
import sys
from scipy.constants.codata import precision
class ReflectionOperationFactory:
    def __init__(self):
        self.args=self.search_args()
    
    def find_op(self,subscribers):
        try:
            op=self.args[1]
            op_name="Operations."+str.upper(op[0])+str.lower(op[1:])+"Operation"
            the_class = self.my_import(op_name)
            objecT=the_class()
            objecT.set_args(self.args[2:])
            objecT.subscribers=subscribers
            self.save_operation(self.args)
            return objecT
        except :
            n_op=NotOperation()
            n_op.subscribers=subscribers
            return n_op
            
    def save_operation(self,operation):
        '''
        save the operation in history.txt. It saves last 10 operations
        '''
        log=Document("log.txt")
        operations=log.get_params()
        prec=""
        for arg in operation:
            prec+=arg+" "
        for key in operations:
            tmp=operations[key]
            operations[key]=prec
            prec=tmp
        operations[len(operations)+1]=prec
        log.update(operations)
        
    def my_import(self,name):
        components = name.split('.')
        mod = __import__(components[0])
        for comp in components[1:]:
            mod = getattr(mod, comp)
        return mod

    def search_args(self):
        risultato=[]
        for arg in sys.argv:
            risultato.append(arg)
        return risultato