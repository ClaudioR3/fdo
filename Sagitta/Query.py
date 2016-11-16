'''
Created on 01 lug 2016

@author: claudio
'''
import os
from DBlink import DBlink
from Document import Document,KL_Document

class Query:
    '''
    The aim of this class is create and manage the query using different info in alias.txt,lastquery.txt and other argoments and 
    to send the query to the istance of DBlink class. 
    '''
    def __init__(self):
        '''
        In initialization, the class save a reference of DBlink class and two references of Document class, one for
        info of last query and one to alias. 
        '''
        self.dblink=DBlink()
        #the lastquery document must be into personal path (into HOME for Linux and MacOS, into Documents for Windows)
        self.lastquery=Document("lastquery.txt")
        #the alias document must be into class' path
        path=os.path.dirname(os.path.abspath(__file__))
        self.alias=KL_Document("alias.txt",path)
        #default table for mecordex user
        self.default_table='MEDCORDEX'
        
    def is_alias(self,name):
        '''
        This function defines the field's name into 'where' of query to send it to the database.
        @param name: string, name of field
        @return string, definitive name choosen between the same name and the alias params
        '''
        alias=name
        for field in self.alias.get_params().keys():
            if name in self.alias.get_params()[field]:
                alias=field
        return alias
        
    def build_where(self,args):
        '''
        Build the 'where' field of query. 
        @param args: dictionary {field:value}
        @return s: string. If args={field:value, ...} return s=where field='value' and ... 
        '''
        string=""
        for field in args.keys():
            if string!="": string+=" and"
            string+= " "+self.is_alias(field)+" = '"+args[field]+"'"
        if string!="": string=" where"+string
        return string
    
    def save_query(self,query):
        '''
        Save query in lastquery.txt
        @param query: string like "select fields from table where field=value and .."
        '''
        self.lastquery.write({"query":query})
        
    def save_datasets(self,datasets):
        #save in lastquery.txt {row:dataset name} (no delete query)
        i=1
        lq={}
        lq["query"]=self.lastquery.get_parameter("query")
        for d in datasets:
            lq[i]=d
            i+=1
        self.lastquery.update(lq)
        
    def triple2table(self,triple):
        #return N Datasets, N files, N size(MB)
        #triple=((dataset1,size1,fname1),(dataset2,size2,fname2),....)
        numByte={}
        numFile={}
        if len(triple)>0:
            s="row %69s %13s %14s\n"%("DATASET NAME","N FILES","N MBs")
        for dataset,size,fname in triple:
            if dataset in numByte:
                numByte[dataset]+=size
                numFile[dataset]+=1
            else:
                numByte[dataset]=size
                numFile[dataset]=1
        #save dataset in lastquery.txt
        self.save_datasets(numByte.keys())
        i=0
        for dataset in numByte:
            i+=1
            s+= "%02d %70s : %5d files %10.2f MB \n"%(i,dataset,numFile[dataset],numByte[dataset])
        return "\nFound "+str(sum(numFile.values()))+" files in "+str(len(numByte))+" Datasets, total size "+str(sum(numByte.values()))+"MB\n\n"+s
        
    def do_query(self,args={},select="*",save=True):
        '''
        This builds the query, sends the query, save the query in lastquery.txt and returns the result(tupla).
        This function saves only query, doesn't save dataset
        @param args: dictionary {filed:value}
        @param select: string "field1,field2,..." or "*"(all), need to select the fields in query
        @param save: boolean, need to save the query
        @return tuple: ((result1),(result2),...,(resultN))
        '''
        table=self.dblink.get_table()
        if table=='': table=self.default_table
        query="select "+select+" from "+table+self.build_where(args)
        tuple=self.dblink.send_query(query)
        if save: self.save_query(query)
        return tuple
    
    def do_describe(self):
        #searches all fields in the database 
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
        return tupla
    
    def get_row_dataset(self,num):
        return self.lastquery.get_parameter(num)
    
    def get_select(self):
        #return a string of all select filed ('*' or 'field1,field2,...fieldN')
        return self.lastquery.get_parameter("query").split(" ")[1] 
        
    def get_index(self,name):
        #return index of tupla where the name == a field name
        select=self.get_select()
        if select =='*':
            #case 1: select *
            index=0
            for a in self.do_describe():
                if a==name: return index
                index+=1
            raise Exception(name+" are not in table")
        else:
            #case 2: select ...,name,...
            index=0
            for field in select.split(','):
                if name==field: return index
                index+=1
        #case 3: select ...,other,...
        raise Exception("Index not found")
    def get_url(self):
        return self.dblink.get_url()
    
    def get_path(self):
        return self.dblink.get_doc().get_parameter("path")