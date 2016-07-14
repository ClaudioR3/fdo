'''
Created on 04 lug 2016

@author: claudio
'''
from Query import Query
import sys

def riempiArgs(lista):
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

def definisci_args():
    risultato=[]
    for arg in sys.argv:
        risultato.append(arg)
    return risultato

def not_operation():
    return "Not Defined Operation, 'help' for list of operations"

def help():
    lista=["help","find","describe","config","getconfig","delconfig","wget"]
    s= "List of operation: \n"
    for elem in lista:
        s+= "'"+elem+"' "
    return s

q=Query()
args=definisci_args()
if len(args)>1:
    operazione=args[1]
else :
    operazione=""

if operazione=="":
    print not_operation()
elif operazione=="find": 
    try :
        print q.do_query(riempiArgs(args[2:]))
    except ():
        print q.find_conn_probls()
elif operazione=="describe":
    for i in q.do_describe():
        print i
elif operazione=="config":
    q.config_dblink(riempiArgs(args[2:]))
elif operazione=="getconfig":
    print q.get_config_toString()
elif operazione=="delconfig":
    q.del_config()
elif operazione=="wget":
    try :
        if len(args)>2:
            if args[2]=="find":
                q.do_query(riempiArgs(args[3:]))
                print q.wget()
            else :
                print not_operation()
        else:
            print q.wget()
    except:
        print q.find_conn_probls()
elif operazione=="help":
    print help()
else:
    print not_operation()