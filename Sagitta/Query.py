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
            if name in self.alias.get_params()[field]: alias=field
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
        '''
        Build a table of string type
        @param triple=((dataset1,size1,fname1),(dataset2,size2,fname2),....)
        @return table: string in format --- row N Datasets, N files, N size(MB)
        '''
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
        
    def do_query(self,args={},select="*",save=True,query=""):
        '''
        This function builds the query if parameter query is void, sends the query,
        save the query in lastquery.txt and returns the result(tupla).
        This function saves only query, doesn't save dataset
        @param args: dictionary {filed:value}
        @param select: string "field1,field2,..." or "*"(all), need to select the fields in query
        @param save: boolean, need to save the query
        @param query: string, query to send at DBlink class. If query is a void string then make string query with other parameters.
        @return tuple: ((result1),(result2),...,(resultN))
        '''
        if query=="":
            table=self.dblink.get_table()
            if table=='': table=self.default_table
            query="select "+select+" from "+table+self.build_where(args)
        tuple=self.dblink.send_query(query)
        if save: self.save_query(query)
        return tuple
    
    def do_describe(self):
        ''' 
        Send a describe query to know all fields in the database
        @return describe: list, all fields of database's table
        '''
        table=self.dblink.get_table()
        if table=='': table=self.default_table
        describe=self.dblink.send_query("describe "+table)
        return [ elem[0] for elem in describe ] 
    
    def get_last_query(self):
        '''
        @return last query
        '''
        return self.lastquery.get_parameter("query")
    
    def get_row_dataset(self,num):
        '''
        @param num: int, number of row
        @return dataset: string, dataset with row == num
        '''
        return self.lastquery.get_parameter(num)
    
    def get_select(self):
        '''
        @return setelct: string of all select filed ('*' or 'field1,field2,...fieldN')
        '''
        return self.lastquery.get_parameter("query").split(" ")[1] 
        
    def get_index(self,name):
        '''
        Scan fields in query's select and return the index of fields' name
        @param name: string, field's name
        @return index: int , index of tupla where the name == a field name
        '''
        select=self.get_select()
        if select =='*':
            #case 1: select *
            fields=self.do_describe()
            for i in range(0,len(fields)):
                if fields[i]==name: return fields[i]
            raise Exception(name+" are not in table")
        else:
            #case 2: select ...,name,...
            fileds=select.split(',')
            for i in range(0,len(fields)):
                if fields[i]==name: return fields[i]
        #case 3: select ...,other,...
        raise Exception("Index not found")
        
    def get_url(self):
        '''
        Repete the same function at DBlink istance.
        @return url: string
        '''
        return self.dblink.get_url()
    
    def get_path(self):
        '''
        Repete the same function at DBlink istance.
        @return path: string
        '''
        return self.dblink.get_doc().get_parameter("path")
    
    def get_config_toString(self):
        '''
        Repete the same function at DBlink istance.
        @return config: string
        '''
        return self.dblink.get_config_toString()
    
    def config_dblink(self,params):
        '''
        Repete the same function at DBlink istance.
        @param params, dictionary
        '''
        self.dblink.set_config(params)
        
    def del_config(self):
        '''
        Repete the same function at DBlink istance.
        '''
        self.dblink.del_config()