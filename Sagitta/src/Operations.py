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
        self.args=args
        self.default_path=os.path.dirname(os.path.abspath(__file__))+"/Download/"
    
    def run(self,q=Query()):
        raise "Metodo Astratto"
    
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

class DescribeOperation(Operation):
    def __init__(self,args=[]):
        Operation.__init__(self, args)
        
    def run(self,q=Query()):
        #return all camp name of database
        try: 
            s=""    
            for e in q.do_describe(): s+= e+"\n"
            self.dispatch(s)
        except:
            self.dispatch(q.find_conn_probls())
    
class ConfigOperation(Operation):
    def __init__(self,args=[]):
        Operation.__init__(self, args)
        
    def run(self,q=Query()):
        #configure connection data
        q.config_dblink(self.args_to_map(self.args))
        
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
        except:
            return q.find_conn_probls()
        
    def wget(self,q):
        s=""
        url=q.get_url()
        try:
            index_fname=q.get_index("fname")
        except:
            self.dispatch("No 'fname' camp in database")
        try : 
            for a in q.send_query(q.get_last_query()):
                s+="wget "+url+a[index_fname]
                s+="\n"
            self.dispatch(s)
        except():
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
        except ():
            self.dispatch("Problems")
        
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
        #inizialization
        name_index=q.get_index("fname")
        size_index=q.get_index("size")
        files_list=[]
        all_opt=False
        if len(self.args)>0:
            if self.args[0]=="--all":
                all_opt=True
                self.args.remove("--all")
        path=q.get_path()
        if path=="":
            path=self.default_path
        url=q.get_url()
        for i in q.send_query(q.get_last_query()):
            files_list.append(path+i[name_index])
            tmp_url=url+i[name_index]
            try:
                if not all_opt:
                    self.dispatch("Downloading #TODO",)
                self.download(url=tmp_url,path=path,filesize=i[size_index]*1024*1024,show_status=all_opt)
            except Exception as e:
                self.dispatch(e)
        self.dispatch( "\nDone...Downloading is complete\n")
        #check if 'open' is in argoments of 'download'
        self.check_open_operation(q,files_list)
            
            
    def download(self,url="",path="",filesize=-1,show_status=True):
        #size(filesize)=MB
        file_name=url.split('/')[-1]
        try:
            u=urllib2.urlopen(url)
        except:
            try:
                #if python hasn't permissions
                req=urllib2.Request(url,headers={'User-Agent':"Magic Browser"})
                u=urllib2.urlopen(req)
            except:
                return
        #check path
        if not os.path.exists(path):
            os.makedirs(path)
        #new file=file_name in path
        f=open(os.path.join(path,file_name),'wb')
        if filesize==-1:
            try:
                meta=u.info()
                #When the method is HTTP
                filesize=int(meta.getheaders("Content-Length")[0])
            except:
                self.dispatch("problems on size of file \n")
                #filesize = casual number (never mind)
                filesize=1
        if show_status:
            self.dispatch("\nDownloading: %s"%file_name)
            filesize_dl=0
            block_sz=8192
            while True:
                b_buffer=u.read(block_sz)
                if not b_buffer:
                    #with this last dispatch, we fix the 'not 100%' bug when download is complete
                    status=r" %10.2f/%10.2f MB  [%3.1f%%]"%(filesize_dl/1024.0/1024,filesize/1024/1024,filesize*100.0/filesize)
                    status=status+chr(8)*(len(status)+1)
                    self.dispatch(status,)
                    break
                filesize_dl+=len(b_buffer)
                f.write(b_buffer)
                status=r" %10.2f/%10.2f MB  [%3.1f%%]"%(filesize_dl/1024.0/1024,filesize/1024/1024,filesize_dl*100.0/filesize)
                status=status+chr(8)*(len(status)+1)
                self.dispatch(status,)
            f.close()
        else:
            r=u.read()
            f.write(r)
            f.close()
    
    def check_open_operation(self,q,files_list):
        #create and run 'open' operation
        if "open" in self.args:
            self.args.remove("open")
            files_list.extend(self.args)
            open_op=OpenOperation()
            open_op.set_args(files_list)
            open_op.subscribers=self.subscribers
            open_op.run(q)
    
class OpenOperation(Operation):
    def __init__(self,args=[]):
        Operation.__init__(self, args)
    
    def run(self,q=Query()):
        if len(self.args)==0 or (len(self.args)==1 and self.args[0]=="--oneline"):
            self.dispatch("At least one argoment: 'path/filename.nc' or '--all'")
            break
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
        # @return: list of operations 
        s= "List of operation: \n"
        for elem in self.lista: s+= "'"+elem+"' "
        self.dispatch(s)
    
class NotOperation(Operation):
    def __init__(self,args=[]):
        Operation.__init__(self, args)
    
    def run(self,q=Query()):
        self.dispatch("Not Defined Operations, 'help' for list of operations")