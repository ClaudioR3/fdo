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
            return self.find_conn_probls()
    
    def do_describe(self):
        try:
            describe=self.dblink.send_query("describe MEDCORDEX")
            l=[]
            for elem in describe:
                l.append(elem[0])
            return l
        except:
            return [self.find_conn_probls()]
    
    def get_config_toString(self):
        return self.dblink.get_config_toString()
    
    def config_dblink(self,params):
        self.dblink.config(params)
        
    def del_config(self):
        self.dblink.del_config()
        
    def get_index_fname(self):
        index=0
        for a in self.do_describe():
            if a=="fname":
                print index
                return index
            index+=1
        return -1
        
    def wget(self):
        risultato=""
        url=self.dblink.get_url()
        index_fname=self.get_index_fname()
        try : 
            for a in self.dblink.send_query(self.lastquery.get_query()):
                risultato+="wget "+url+a[index_fname]
                risultato+="\n"
            return risultato
        except:
            return "Last Query not founded or Wrong Config"
        
    def find_conn_probls(self):
        return self.dblink.find_conn_probls()