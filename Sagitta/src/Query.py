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
        
    def save_datasets(self,datasets):
        i=1
        lq={}
        lq["query"]=self.lastquery.get_query()
        for d in datasets:
            lq[i]=d
            i+=1
        self.lastquery.write(lq)
        
    def get_datasets(self,tupla):
        #return {dataset name:[n file , n size]}
        datasets={}
        index_dataset=self.get_index("dataset")
        index_size=self.get_index("size")
        for elem in tupla:
            if datasets.has_key(elem[index_dataset]):
                datasets[elem[index_dataset]][0]+=1
                datasets[elem[index_dataset]][1]+=elem[index_size]
            else:
                datasets[elem[index_dataset]]=[1,elem[index_size]]
        self.save_datasets(datasets)
        return datasets
            
        
    def tupla_toString(self,tupla):
        datasets=self.get_datasets(tupla)
        s=""
        i=0
        tot_files=0
        tot_size=0
        s+="row %69s %13s %14s\n"%("FILE NAME","N FILES","N MBs")
        for d in datasets:
            i+=1
            tot_files+=datasets[d][0]
            tot_size+=datasets[d][1]
            s+= "%02d %70s : %5d files %10.2f MB \n"%(i,d,datasets[d][0],datasets[d][1])
        return "Finded "+str(tot_files)+" files in " +str(i)+" Datasets, total size "+str(tot_size)+"MB\n\n"+s
        
    def do_query(self,args):
        try :
            query="select * from "+self.dblink.get_table()+" "+self.build_where(args)
            tupla=self.dblink.send_query(query)
            self.save_query(query)
            return tupla
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
        
    def get_row_dataset(self,number):
        #return dataset name in row==number in lastquery.txt 
        if self.lastquery.get_params().has_key(number):
            return self.lastquery.get_params()[number]
        else:
            return ""
        
    def select_row(self,num):
        print self.lastquery.get_query()+" and dataset = '"+self.get_row_dataset(num)+"'"
        return self.dblink.send_query(self.lastquery.get_query()+" and dataset = '"+self.get_row_dataset(num))+"'"
        #To DO safe?
        
    def get_index(self,name):
        index=0
        for a in self.do_describe():
            if a==name:
                return index
            index+=1
        return -1
        
    def wget(self):
        risultato=""
        url=self.dblink.get_url()
        index_fname=self.get_index("fname")
        try : 
            for a in self.dblink.send_query(self.lastquery.get_query()):
                risultato+="wget "+url+a[index_fname]
                risultato+="\n"
            return risultato
        except:
            return "Last Query not founded or Wrong Config"
        
    def find_conn_probls(self):
        return self.dblink.find_conn_probls()