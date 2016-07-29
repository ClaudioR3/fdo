'''
Created on Jul 29, 2016

@author: claudio
'''
import sys
from Operations import *
class OperationFactory:
    def __init__(self):
        pass
    
    def find_op(self):
        op=Operation()
        #list of argoments by python
        args=self.definisci_args()
        if len(args)>1:
            #first argoment must be the operation to do
            operazione=args[1]  
        else :
            operazione=""

        if operazione=="":
            #case operation null  
            op=NotOperation()
        elif operazione=="find": 
            op=FindOperation(self.riempiArgs(args[2:]))
        elif operazione=="describe":
            op=DescribeOperation()
        elif operazione=="config":
            op=ConfigOperation(self.riempiArgs(args[2:]))
        elif operazione=="getconfig":
            op=GetconfigOperation()
        elif operazione=="delconfig":
            op=DelconfigOperation()
        elif operazione=="wget":
            op=WgetOperation()
        elif operazione=="help":
            op=HelpOperation()
        else:
            op=NotOperation()
        return op

    def riempiArgs(self,lista):
        args={}
        i=0
        key=""
        for arg in sys.argv:
            if i>1:
                if arg[0]=='-':
                    key=arg[1:]
                else :
                    if key!="":
                        args[key]=arg
                        key=""
            i+=1
        return args

    def definisci_args(self):
        risultato=[]
        for arg in sys.argv:
            risultato.append(arg)
        return risultato

class ReflectionOperationFactory:
    def __init__(self):
        pass
    
    def find_op(self):
        
            args=self.definisci_args()
            if len(args)>1:
                #first argoment must be the operation to do
                op=args[1]  
            else :
                return Notoperation()
            op_name="Operations."+str.upper(op[0])+str.lower(op[1:])+"Operation"
            the_class = self.my_import(op_name)
            the_class.set_args(self.riempiArgs(args[2:]))
            return the_class
        
    def my_import(self,name):
        components = name.split('.')
        mod = __import__(components[0])
        for comp in components[1:]:
            mod = getattr(mod, comp)
        return mod
    
    def riempiArgs(self,lista):
        args={}
        i=0
        key=""
        for arg in sys.argv:
            if i>1:
                if arg[0]=='-':
                    key=arg[1:]
            else :
                if key!="":
                    args[key]=arg
                    key=""
            i+=1
        return args

    def definisci_args(self):
        risultato=[]
        for arg in sys.argv:
            risultato.append(arg)
        return risultato