'''
Created on Jul 29, 2016

@author: claudio
'''
import urllib2 
import os
from Query import Query
from Observer import Publisher
from netCDF4 import Dataset

class Operation(Publisher):
    def __init__(self, args=[]):
        Publisher.__init__(self)
        self.bold="\33[1m"
        self.reset="\33[0;0m"
        self.args=args
        self.default_path=os.path.dirname(os.path.abspath(__file__))+"/Download/"
    
    def run(self,q=Query()):
        raise "Operation without the core"
    
    def set_args(self,args):
        self.args=args
        
    def args_to_map(self,l):
        map_of_args={}
        key=""
        for arg in l:
            if arg[0]=='-':
                key=arg[1:]
            else :
                if key!="":
                    map_of_args[key]=arg
                    key=""
        return map_of_args
    
    def description(self):
        self.dispatch("Operation without description or not founded")
    
class FindOperation(Operation):
    def __init__(self,args=[]):
        Operation.__init__(self, args)
        
    def run(self,q=Query()):
        try :
            opt=0
            if self.args.count("--oneline")==1:
                self.args.remove("--oneline")
                opt=1
            elif self.args.count("--d")==1:
                self.args.remove("--d")
                opt=2
                
            #do query with a map of args -> args_map={camp of database: value of camp}
            table=q.tupla_toString(q.do_query(self.args_to_map(self.args)))
            if opt==1:
                #dispatch only the first line (line[0] is empty)
                self.dispatch(table.split('\n')[1])
            elif opt==2:
                #dispatch only dataset of table
                for elem in table.split('\n')[4:]:
                    self.dispatch(elem.split(':')[0]+"\n")
            else:
                #dispatch table in the format of tupla_toString()
                self.dispatch(table) 
        except Exception as e:
            self.dispatch(e)
            #self.dispatch(q.find_conn_probls())
            
    def description(self):
        message="\n"+self.bold+"NAME"+self.reset+"\n\t sagitta find -[field] [value]\n"
        message+="\n"+self.bold+"DESCRIPTION"+self.reset+"\n\t This operation executes a query in database where fields are equals\n\t values and shows a table with number of row, all datasets names with\n\t relative numbers of files and sizes.\n\t If you want know all fields of your database try 'sagitta describe'.\n\t Or do 'sagitta help describe' to know other feature.\n"
        message+="\n"+self.bold+"EXAMPLE"+self.reset+"\n\t sagitta find :\n\t\t Without fields and values, this returns all what the database\n\t\t has.\n\n\t sagitta find -field1 value1 -field2 value2 ...:\n\t\t Query where 'field1'=='value1' and 'field2'=='value2'...\n"
        message+="\n"+self.bold+"OPTIONS"+self.reset+"\n\t --oneline\n\t\t Show only the essential of research of 'find' (Number found\n\t\t files and total of sizes)\n\n\t --d\n\t\t Shows the datases without informations"
        self.dispatch(message)

class DescribeOperation(Operation):
    def __init__(self,args=[]):
        Operation.__init__(self, args)
        
    def run(self,q=Query()):
        #if without args, it tries to dispatch all fields, otherwise searches all possible values of args.
        if len(self.args)==0: self.do_describe(q)
        else:
            #tupla=q.do_query({},0)
            for field in self.args:
                self.get_describe(q,field)
                
    def get_describe(self,q=Query(),field=""):
        try:
            all_values=q.do_query(select=field)
            self.dispatch("All values of "+field+":\n")
            for x in set(all_values):
                self.dispatch(x[0])
        except Exception as e:
            self.dispatch(e)
            
    
    def do_describe(self,q=Query()):
        try: 
            s=""    
            for e in q.do_describe(): s+= e+"\n"
            self.dispatch(s)
        except Exception as e:
            self.dispatch(e)
            #self.dispatch(q.find_conn_probls())
    
    def description(self):
        message="\n"+self.bold+"NAME"+self.reset+"\n\t sagitta describe\n"
        message+="\n"+self.bold+"DESCRIPTION"+self.reset+"\n\t Describe operation of Sagitta shows all fields of database.\n"
        message+="\n"+self.bold+"EXAMPLE"+self.reset+"\n\t sagitta describe :\n\t\t show fields\n\n\t sagitta describe [field] :\n\t\t show all possible value of 'field'.\n"
        self.dispatch(message)
    
class ConfigOperation(Operation):
    def __init__(self,args=[]):
        Operation.__init__(self, args)
        
    def run(self,q=Query()):
        #configure connection data
        q.config_dblink(self.args_to_map(self.args))
        
    def description(self):
        message="\n"+self.bold+"NAME"+self.reset+"\n\t sagitta config [option] [value]\n"
        message+="\n"+self.bold+"DESCRIPTION"+self.reset+"\n\t Configuration operation of Sagitta allows to set the own connection\n\t to the database and other things. It will save all configuration data\n\t in config.txt.\n"
        message+="\n"+self.bold+"EXAMPLES"+self.reset+"\n\t sagitta config :\n\t\t do nothing\n\n\t sagitta config [option] [value]:\n\t\t set option with value\n\t sagitta config [option1] [value1] [option2] [value2] ...\n\t\t you can add more option and value in the same operation.\n"
        message+="\n"+self.bold+"OPTIONS"+self.reset+"\n\t -user <value>\n\t\t set user name with 'value' to access at the database\n\n\t-passwd <value>\n\t\tset password with 'value' to access at the database\n\n\t-host <value>\n\t\tset host with 'value' to access at the database, if you\n\t\tdon't set the host, it means like 'localhost'\n\n\t-db"
        self.dispatch(message)
        
class GetconfigOperation(Operation):
    def __init__(self,args=[]):
        Operation.__init__(self, args)
        
    def run(self,q=Query()):
        #print connection data
        self.dispatch(q.get_config_toString())
    
class DelconfigOperation(Operation):
    def __init__(self,args="none"):
        Operation.__init__(self, args)
        
    def run(self,q=Query()):
        #delete connection data
        q.del_config()
        self.dispatch("")
    
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
                    self.dispatch(self.wget(q))
                elif self.args[0]=="selectrow":
                    sr_op=SelectrowOperation()
                    sr_op.set_args(self.args[1:])
                    sr_op.run(q)
                    self.dispatch(self.wget(q))
                else :
                    self.dispatch("Element after 'wget' is unknown")
            else:
                self.dispatch(self.wget(q))
        except Exception as e:
            self.dispatch(e)
            #return q.find_conn_probls()
        
    def wget(self,q):
        s=""
        url=q.get_url()
        try:
            index_fname=q.get_index("fname")
        except:
            self.dispatch("No 'fname' camp in database")
        try : 
            for a in q.send_query(q.get_last_query()):
                s+="\nwget "+url+a[index_fname]
            return s
        except ():
            self.dispatch("Last Query not founded or Wrong Config Data")
        
class SelectrowOperation(Operation):
    def __init__(self,args=[]):
        Operation.__init__(self, args)
        
    def run(self,q=Query()):
        try:
            if len(self.args)==0: self.dispatch("At least one row")
            if self.is_int_and_uniq(self.args)==True: 
                self.dispatch( q.tupla_toString(q.send_query(self.build_new_query(q))))
        except ValueError as te:
            self.dispatch( "%s is not int"%te[0])
        except SyntaxError as se:
            self.dispatch("%s is not uniq"%se[0])
        except NameError:
            self.dispatch("Last Query not founded")
        except Exception as e:
            self.dispatch(e)
        
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
                raise Exception('RowError: '+a)
            new_query+=" dataset = '%s'"%dataset_name
        new_query+=")"
        return new_query
    
class DownloadOperation(Operation):
    def __init__(self,args=[]):
        Operation.__init__(self, args)
        
    def run(self,q=Query()):
        #download any files in the path
        #initialization
        name_index=q.get_index("fname")
        size_index=q.get_index("size")
        dataset_index=q.get_index("dataset")
        url=q.get_url()
        query=q.send_query(q.get_last_query())
        
        #check path in config.txt and, if not exists, set default path
        path=q.get_path()
        if path=="": path=self.default_path
        
        for i in query:
            #second part of initialization
            #for example: 'https://example.com/file.ext'
            tmp_url=url+i[name_index]
            #for example: '/Download/Dataset_name/'
            tmp_path=path+i[dataset_index]
            try:
                #dispatch number of file/tot files  (progressive status of download)
                self.dispatch("\n"+str(query.index(i)+1)+"/"+str(len(query)))
                #resume or start download
                self.download_with_resume(tmp_url,tmp_path,i[size_index])
            except Exception as e:
                self.dispatch(str(e)+"\n")
        self.dispatch( "\nDone...Downloading is complete...\n")
        
    def build_req(self,url="",headers={}):
        req=urllib2.Request(url)    #build Request
        #set headers
        for k in headers:
            req.add_header(key=k, val=headers[k])
        return req
    
    def compare(self,n1,n2):
        if n1==n2:
            return True
        else:
            #approximation
            return "%.2f"%(n1/1024.0/1024)=="%.2f"%(n2/1024.0/1024)
        
    def download_with_resume(self,url="",path="",file_size=0):
        #download with resume only for http
        # |file_size|=MB 
        file_name=url.split('/')[-1]
        opener=urllib2.build_opener()
        #create the path if not exists
        if not os.path.exists(path):
            os.makedirs(path)
        #check if file exists and how much 
        start_sz=os.path.getsize(os.path.join(path,file_name)) if os.path.exists(os.path.join(path,file_name)) else 0
        #find the file size if file_size==0
        if file_size==0:
            try:
                netfile=urllib2.build_opener().open(url)
                file_size=netfile.info()['Content-Length']
                netfile.close()		
            except:
                raise Exception("Not Found Size")
        #if already download
        #compare with approximation
        if self.compare(float(start_sz),float(file_size*1024*1024)): 
            raise Exception("The file was already download")
        if start_sz>file_size:
            raise Exception("The file was already download, but it has more size than real file size")
        if url.split(':')[0]!="ftp":
            #case http, with http the program can do resume
            #show if there is 'resume' or 'start' download
            if start_sz!=0:self.dispatch("Resume of %s: (%.2f/%.2f MB)"%(file_name,start_sz/1024.0/1024,file_size)) 
            else:  self.dispatch("Downloading %s "%file_name)
            #initialization download
            headers={'Range':"bytes=%d-"%start_sz}   #header for resume
            try:
                opener=urllib2.build_opener()       #build opener
                req=self.build_req(url,headers)     #build Request with headers
                netfile=opener.open(req)
            except :
                #if python hasn't permissions
                headers['User-Agent']="Magic Browser"
                opener=urllib2.build_opener()       #build opener
                req=self.build_req(url,headers)     #build Request with headers
                netfile=opener.open(req)
            #open file with 'append' mode
            f=open(os.path.join(path,file_name),'ab')
            #start download
            filesize_dl=start_sz  
        else:
            #case ftp, the program can not do resume (now)
            netfile=urllib2.urlopen(url=url)
            #open file with 'write' mode
            f=open(os.path.join(path,file_name),'wb')
            #start download
            filesize_dl=0                 
        while True:               
            b_buffer=netfile.read(8*1024)
            if not b_buffer:
                #with this last dispatch, we fix the 'not 100%' bug when download is complete
                status=r"%10.2f/%10.2f MB [%3.1f%%]"%(filesize_dl/1024.0/1024,file_size,file_size*100.0/file_size)
                status=status+chr(8)*(len(status)+1)
                self.dispatch(status,)
                break
            filesize_dl+=len(b_buffer)
            f.write(b_buffer)
            status=r"%10.2f/%10.2f MB  [%3.1f%%]"%(filesize_dl/1024.0/1024,file_size,filesize_dl*100.0/file_size*1024*1024)
            status=status+chr(8)*(len(status)+1)
            self.dispatch(status,)
        netfile.close()
        f.close()

    
class OpenOperation(Operation):
    def __init__(self,args=[]):
        Operation.__init__(self, args)
    
    def run(self,q=Query()):
        if len(self.args)==0 or (len(self.args)==1 and self.args[0]=="--oneline"):
            self.dispatch("At least one argoment: 'path/filename.nc' or '--all'")
            return 
        try:
            oneline_opt=False
            if "--oneline" in self.args:
                oneline_opt=True
                self.args.remove("--oneline")
            if "--all" in self.args:
                self.args.remove("--all")
                if len(self.args)==0:
                    self.args.append(self.default_path)
                for p in self.args:
                    #check files.nc
                    files=self.check_files_nc(p)
                    for f in files:
                        self.open(p,f,oneline_opt)
            else:
                for a in self.args:
                    path=""
                    file_name=""
                    l=a.split('/')
                    file_name=l.pop()
                    if len(l)!=0:
                        for e in l:
                            path+=e+"/"
                        self.open(path,file_name,oneline_opt)
        except Exception as exc:
            self.dispatch("\n")
            self.dispatch(exc)

    def check_files_nc(self,path):
        l=[]
        for f in os.listdir(path):
            if f.endswith(".nc"):
                l.append(f)
        return l
            
    def open(self,path="",file_name="",oneline_opt=False):
        nc=Dataset(os.path.join(path,file_name),'r')
        if oneline_opt:
            #case one line option
            d=""
            for dim in nc.dimensions.keys(): d+=dim+" "
            v=""
            for var in nc.variables.keys(): v+=var+" "
            #try to print all info of file.nc in only one line
            string=file_name+"\t D("+str(len(nc.dimensions.keys()))+"): "+d+"\t V("+str(len(nc.variables.keys()))+"): "+v+"\n"
        else:
            #open file in path and return all dimensions and all variables
            string="\nFILENAME: "+file_name+"\n\t Dimensions: "
            for d in nc.dimensions.keys(): string+=d+" "
            string+="\n\t Variables: "
            for v in nc.variables.keys(): string+=v+" "
            #string+="\n\t Attrib: "
            #for a in nc.attr: string+=a+" "
        self.dispatch(string)
        
class HelpOperation(Operation):
    def __init__(self,args=[]):
        Operation.__init__(self, args)
        self.lista=["help","find","describe","config","getconfig","delconfig","wget","selectrow","download","open"]
        
    def run(self,q=Query()):
        if len(self.args)==0: self.show_operations()
        else: self.show_descriptions()
            
    def show_descriptions(self):
        #for help operation
        from OperationFactory import ReflectionOperationFactory
        #call description function of operations in args
        op_fact=ReflectionOperationFactory()
        for arg in self.args:
            op_fact.args=["help",arg]
            op=op_fact.find_op(self.subscribers)
            op.description()
        
    def show_operations(self):
        # @return: list of operations 
        s= "List of operation: \n"
        for elem in self.lista: s+= "'"+elem+"' "
        self.dispatch(s)
    
class NotOperation(Operation):
    def __init__(self,args=[]):
        Operation.__init__(self, args)
    
    def run(self,q=Query()):
        self.dispatch("Not Defined Operations, 'help' for list of operations")