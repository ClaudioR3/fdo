'''
Created on Jul 29, 2016

@author: claudio
'''
from Query import Query
from Observer import *
import sys
class Operation:
    def __init__(self, args=[]):
        self.args=args
        self.subscribers=[]
        
    def run(self,q=Query()):
        raise "Metodo Astratto"
    
    def set_args(self,args):
        self.args=args
    
    def set_subscribers(self,subscribers):
        self.subscribers=subscribers
        
    def args_to_map(self,lista):
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
    
class FindOperation(Operation):
    def __init__(self,args=[]):
        Operation.__init__(self, args)
        
    def run(self,q=Query()):
        try :
            #do query with a map of args -> args_map={camp of database: value of camp}
            return q.tupla_toString(q.do_query(self.args_to_map(self.args))) 
        except:
            return q.find_conn_probls()

class DescribeOperation(Operation):
    def __init__(self,args=[]):
        Operation.__init__(self, args)
        
    def run(self,q=Query()):
        #return all camp name of database
        try: 
            s=""    
            for e in q.do_describe(): s+= e+"\n"
            return s
        except:
            return q.find_conn_probls()
    
class ConfigOperation(Operation):
    def __init__(self,args=[]):
        Operation.__init__(self, args)
        
    def run(self,q=Query()):
        #configure connection data
        q.config_dblink(self.args_to_map(self.args))
        return ""
        
class GetconfigOperation(Operation):
    def __init__(self,args=[]):
        Operation.__init__(self, args)
        
    def run(self,q=Query()):
        #print connection data
        return q.get_config_toString()
    
class DelconfigOperation(Operation):
    def __init__(self,args="none"):
        Operation.__init__(self, args)
        
    def run(self,q=Query()):
        #delete connection data
        q.del_config()
        return ""
    
class WgetOperation(Operation):
    def __init__(self,args=[]):
        Operation.__init__(self, args)
    
    def run (self,q=Query()):
        #print wget + url of last query
        try :
            if len(self.args)>0:
                #case nestled operation
                if self.args[0]=="find":
                    f_op=FindOperation()
                    f_op.set_args(self.args[1:])
                    f_op.run(q)
                    return self.wget(q)
                elif self.args[0]=="selectrow":
                    sr_op=SelectrowOperation()
                    sr_op.set_args(self.args[1:])
                    sr_op.run(q)
                    return self.wget(q)
                else :
                    return "Element after 'wget' is unknown"
            else:
                return self.wget(q)
        except:
            return q.find_conn_probls()
        
    def wget(self,q):
        risultato=""
        url=q.get_url()
        try:
            index_fname=q.get_index("fname")
        except:
            return "No 'fname' camp in database"
        try : 
            for a in q.send_query(q.get_last_query()):
                risultato+="wget "+url+a[index_fname]
                risultato+="\n"
            return risultato
        except():
            return "Last Query not founded or Wrong Config Data"
        
class SelectrowOperation(Operation):
    def __init__(self,args=[]):
        Operation.__init__(self, args)
        
    def run(self,q=Query()):
        try:
            if len(self.args)==0: return "At least one row"
            if self.is_int_and_uniq(self.args)==True: 
                return len(q.send_query(self.build_new_query(q)))
        except ValueError as te:
            return "%s is not int"%te[0]
        except SyntaxError as se:
            return "%s is not uniq"%se[0]
        except NameError:
            return "Last Query not founded"
        except ():
            return "Problems"
        
    def is_int_and_uniq(self,args):
        #args must be a list
        for elem in args:
            #check if elem is a number (no int->raise ValueError)
            int(elem)   
            #check if elem is not uniq
            if args.count(elem)>1:raise SyntaxError(elem)
        return True
            
    
    def build_new_query(self,q):
        new_query=q.get_last_query()
        if new_query=="":raise NameError
        new_query_div=new_query.split(" ")
        if len(new_query_div)==4:
            new_query+=" where"
        else:
            new_query+=" and"
        new_query+="("
        for a in self.args:
            new_query_div=new_query.split(" ")
            if a!=self.args[0]:
                new_query+=" ||"
            dataset_name=q.get_row_dataset(a)
            if dataset_name=="":
                raise a
            new_query+=" dataset = '%s'"%dataset_name
        new_query+=")"
        return new_query
    
class DownloadOperation(Operation):
    def __init__(self,args=[]):
        Operation.__init__(self, args)
        
    def run(self,q=Query()):
        #download any files in path
        path=q.get_path()
        if path=="":
            path=os.path.dirname(os.path.abspath(__file__))+"/Download/"
        nc_dl=NC_download()
        for s in self.subscribers:
            nc_dl.register(s)
        for i in q.send_query(q.get_last_query()):
            url=q.get_url()+i[int(q.get_index("fname"))]
            try:
                nc_dl.download(url,path)
            except:
                raise "Downloading is failed"
        return "Done...Downloading is complete"
        
class HelpOperation(Operation):
    def __init__(self,args=[]):
        Operation.__init__(self, args)
        self.lista=["help","find","describe","config","getconfig","delconfig","wget","selectrow","download"]
        
    def run(self,q=Query()):
        # @return: list of operations 
        s= "List of operation: \n"
        for elem in self.lista: s+= "'"+elem+"' "
        return s
    
class NotOperation(Operation):
    def __init__(self,args=[]):
        Operation.__init__(self, args)
    
    def run(self,q=Query()):
        return "Not Defined Operations, 'help' for list of operations"