'''
Created on Jul 29, 2016

@author: claudio
'''
import sys
from Query import Query

class OperationFactory:
    def __init__(self):
        pass
    
    def help(self):     # @return: list of operations 
        lista=["help","find","describe","config","getconfig","delconfig","wget"]
        s= "List of operation: \n"
        for elem in lista:
            s+= "'"+elem+"' "
        return s
    
    def find_op(self):
        #create Query istance
        q=Query()
        #list of argoments by python
        args=self.definisci_args()
        if len(args)>1:
            #first argoment must be the operation to do
            operazione=args[1]  
        else :
            operazione=""

        if operazione=="":
            #case operation null  
            print self.not_operation()
        elif operazione=="find": 
            try :
                #do query with all (argoments - operation) 
                print q.do_query(self.riempiArgs(args[2:])) 
            except ():
                print q.find_conn_probls()
        elif operazione=="describe":
            #print all camp name of database     
            for i in q.do_describe():
                print i
        elif operazione=="config":
            #configure connection data
            q.config_dblink(self.riempiArgs(args[2:]))
        elif operazione=="getconfig":
            #print connection data
            print q.get_config_toString()
        elif operazione=="delconfig":
            #delete connection data
            q.del_config()
        elif operazione=="wget":
            #print wget + url of last query
            try :
                if len(args)>2:
                    #case nestled 'find'
                    if args[2]=="find":
                        q.do_query(self.riempiArgs(args[3:]))
                        print q.wget()
                    else :
                        print self.not_operation()
                else:
                    print q.wget()
            except:
                print q.find_conn_probls()
        elif operazione=="help":
            print self.help()
        else:
            print self.not_operation()

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

    def not_operation(self):
        return "Not Defined Operation, 'help' for list of operations"

