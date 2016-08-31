'''
Created on 01 lug 2016

@author: claudio
'''
from DBlink import DBlink
from Document import Document,KL_Document

class Query:
    '''
    the aim of this class is to build the query with different info by alias.txt,lastquery.txt and argoments 
and let DBLink class to send the query to the database
    '''
    def __init__(self):
        '''
        Constructor
        '''
        self.dblink=DBlink()
        self.lastquery=Document("lastquery.txt")
        self.alias=KL_Document("alias.txt")
        
        
    def compare(self,n1,n2):
        n1=str.lower(n1)
        n2=str.lower(n2)
        return n1==n2
        
    def is_defined(self,name,describe):
        nameDefined=""
        for camp in describe:
            if self.compare(name,camp):
                nameDefined=name
        if nameDefined=="":
            nameDefined=self.is_alias(name)
        return nameDefined
    
    def is_alias(self,name):
        alias_map=self.alias.get_params()
        for k in alias_map:
            for v in alias_map[k]:
                if v==name:
                    return k
        return ""
        
    def build_where(self,args):
        #build where of query
        s=""
        describe=self.do_describe()
        for name in args.keys():
            #nameDefined is the name of camp build by describe or alias
            nameDefined=self.is_defined(name,describe)
            if nameDefined!="":
                if s!="":
                    s+=" and"
                s+= " "+nameDefined+" = '"+args[name]+"'"
            else:
                raise Exception("error field name: '"+name+"'")
        if s!="":
            s=" where"+s
        return s
    
    def save_query(self,q):
        #save query in lastquery.txt
        a={"query":q}
        self.lastquery.write(a)
        
    def save_datasets(self,datasets):
        #save in lastquery.txt {row:dataset name} (no delete query)
        i=1
        lq={}
        lq["query"]=self.lastquery.get_parameter("query")
        for d in datasets:
            lq[i]=d
            i+=1
        self.lastquery.write(lq)
        
    def get_datasets(self,tupla):
        #return {dataset name:[n file , n size(MB)]}
        datasets={}
        index_dataset=self.get_index("dataset")
        index_size=self.get_index("size")
        for elem in tupla:
            if datasets.has_key(elem[index_dataset]):
                datasets[elem[index_dataset]][0]+=1
                datasets[elem[index_dataset]][1]+=elem[index_size]
            else:
                datasets[elem[index_dataset]]=[1,elem[index_size]]
        return datasets
    
    def tupla_toString(self,tupla):
        #return N Datasets, N files, N size(MB)
        datasets=self.get_datasets(tupla)
        self.save_datasets(datasets)
        s=""
        i=0
        tot_files=0
        tot_size=0
        if len(datasets)!=0:
            s+="row %69s %13s %14s\n"%("DATASET NAME","N FILES","N MBs")
            for d in datasets:
                i+=1
                tot_files+=datasets[d][0]
                tot_size+=datasets[d][1]
                s+= "%02d %70s : %5d files %10.2f MB \n"%(i,d,datasets[d][0],datasets[d][1])
        return "\nFound "+str(tot_files)+" files in "+str(i)+" Datasets, total size "+str(tot_size)+"MB\n\n"+s
        
    def do_query(self,args):
        query="select * from "+self.dblink.get_table()+self.build_where(args)
        tupla=self.dblink.send_query(query)
        self.save_query(query)
        return tupla
    
    def do_describe(self):
        describe=self.dblink.send_query("describe MEDCORDEX")
        l=[]
        for elem in describe:
            l.append(elem[0])
        return l
    
    def get_config_toString(self):
        return self.dblink.get_config_toString()
    
    def config_dblink(self,params):
        self.dblink.set_config(params)
        
    def del_config(self):
        self.dblink.del_config()
    
    def get_last_query(self):
        return self.lastquery.get_parameter("query")
        
    def send_query(self,query):
        tupla=self.dblink.send_query(query)
        self.save_query(query)
        datasets=self.get_datasets(tupla)
        self.save_datasets(datasets)
        return tupla
    
    def get_row_dataset(self,num):
        return self.lastquery.get_parameter(num)
        
    def get_index(self,name):
        index=0
        for a in self.do_describe():
            if a==name:
                return index
            index+=1
        return -1
        
    def get_url(self):
        return self.dblink.get_url()
    
    def get_path(self):
        return self.dblink.get_doc().get_parameter("path")
        
    def find_conn_probls(self):
        return self.dblink.find_conn_probls()
