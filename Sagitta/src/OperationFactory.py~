'''
Created on Jul 29, 2016

@author: claudio
'''
from Operations import *
import sys
class ReflectionOperationFactory:
    def __init__(self):
        pass
    
    def find_op(self,subscribers):
            args=self.definisci_args()
            try:
                op=args[1]
                op_name="Operations."+str.upper(op[0])+str.lower(op[1:])+"Operation"
                the_class = self.my_import(op_name)
                objecT=the_class()
                objecT.set_args(args[2:])
                for subscriber in subscribers:
                    objecT.register(subscriber)
                return objecT
            except():
                return NotOperation()
        
    def my_import(self,name):
        components = name.split('.')
        mod = __import__(components[0])
        for comp in components[1:]:
            mod = getattr(mod, comp)
        return mod

    def definisci_args(self):
        risultato=[]
        for arg in sys.argv:
            risultato.append(arg)
        return risultato