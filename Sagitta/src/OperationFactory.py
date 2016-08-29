'''
Created on Jul 29, 2016

@author: claudio
'''
from Operations import NotOperation
import sys
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
                return objecT
            except:
                n_op=NotOperation()
                n_op.subscribers=subscribers
                return n_op
        
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