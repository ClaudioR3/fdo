'''
Created on 01 lug 2016

@author: claudio
'''
from DBlink import DBlink
from Document import *

class Query:
    '''
    classdocs
    '''
    

    def __init__(self):
        '''
        Constructor
        '''
        self.dblink=DBlink()
        self.lastquery=Document("lastquery.txt")
        
        
    def confronta(self,n1,n2):
        n1=str.lower(n1)
        n2=str.lower(n2)
        return n1==n2
        
    def is_defined(self,name,describe):
        nameDefined=""
        for e in describe:
            if self.confronta(name,e):
                nameDefined=name
        if nameDefined=="":
            nameDefined=self.is_alias(name)
        return nameDefined
    
    def is_alias(self,name):
        return ""
        
    def build_where(self,args):
        s=""
        describe=self.do_describe()
        for name in args.keys():
            nameDefined=self.is_defined(name,describe)
            if nameDefined!="":
                if s!="":
                    s+=" and "
                s+= " "+nameDefined+" = '"+args[name]+"' "
        if s!="":
            s=" where "+s
        return s
    
    def save_query(self,q):
        a={"query":q}
        self.lastquery.write(a)
        
    def do_query(self,args):
        try :
            query="select * from "+self.dblink.get_table()+" "+self.build_where(args)
            tupla=self.dblink.send_query(query)
            self.save_query(query)
            return "%d risultati"%len(tupla)
        except:
            return "Problem 0: Wrong Config"
    
    def do_describe(self):
        try:
            describe=self.dblink.send_query("describe MEDCORDEX")
            l=[]
            for elem in describe:
                l.append(elem[0])
            return l
        except:
            return ["Problem 0: Wrong Config"]
    
    def get_config_toString(self):
        return self.dblink.get_config_toString()
    
    def config_dblink(self,params):
        self.dblink.config(params)
        
    def del_config(self):
        self.dblink.del_config()
        
    def wget(self):
        risultato=""
        try : 
            for a in self.dblink.send_query(self.lastquery.get_query()):
                risultato+="wget "
                for i in range(0,2):
                    risultato+= str(a[i])+" "
                risultato+="\n"
            return risultato
        except(),e:
            print e
            return "Last Query not founded or Wrong Config"