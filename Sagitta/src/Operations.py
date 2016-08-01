'''
Created on Jul 29, 2016

@author: claudio
'''
from Query import Query
class Operation:
    def __init__(self, args=[]):
        self.args=args
        
    def run(self,q=Query()):
        raise "Metodo Astratto"
    
    def set_args(self,args):
        self.args=args
    
class FindOperation(Operation):
    def __init__(self,args=[]):
        Operation.__init__(self, args)
        
    def run(self,q=Query()):
        try :
            #do query with all (argoments) 
            return q.tupla_toString(q.do_query(self.args)) 
        except ():
            #return q.find_conn_probls()
            return "Problems"

class DescribeOperation(Operation):
    def __init__(self,args=[]):
        Operation.__init__(self, args)
        
    def run(self,q=Query()):
        #return all camp name of database 
        s=""    
        for e in q.do_describe(): s+= e+"\n"
        return s
    
class ConfigOperation(Operation):
    def __init__(self,args=[]):
        Operation.__init__(self, args)
        
    def run(self,q=Query()):
        #configure connection data
        q.config_dblink(self.args)
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
            if len(self.args)>2:
                #case nestled 'find'
                if self.args[2]=="find":
                    q.do_query(self.riempiArgs(self.args[3:]))
                    return q.wget()
                else :
                    return self.not_operation()
            else:
                return q.wget()
        except:
            #return q.find_conn_probls()
            return "Problems"
        
class SelectrowOperation(Operation):
    def __init__(self,args=[]):
        Operation.__init__(self, args)
        
    def run(self,q=Query()):
        try:
            return len(q.select_row(q.get_row_dataset(self.args[0])))
        except (),e:
            return e
        
class HelpOperation(Operation):
    def __init__(self,args=[]):
        Operation.__init__(self, args)
        self.lista=["help","find","describe","config","getconfig","delconfig","wget"]
        
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