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
    
    def run(self,q=Query()):
        raise "Metodo Astratto"
    
    def set_args(self,args):
        self.args=args
        
    def args_to_map(self,l):
        args={}
        i=0
        key=""
        for arg in l:
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
            self.dispatch(q.tupla_toString(q.do_query(self.args_to_map(self.args)))) 
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
        self.dispatch("")
        
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
                self.dispatch( len(q.send_query(self.build_new_query(q))))
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
    
class DownloadOperation(Operation,Publisher):
    def __init__(self,args=[]):
        Operation.__init__(self, args)
        
    def run(self,q=Query()):
        #download any files in path
        path=q.get_path()
        if path=="":
            #this path
            path=os.path.dirname(os.path.abspath(__file__))+"/Download/"
        url=q.get_url()
        name_index=q.get_index("fname")
        size_index=q.get_index("size")
        #full_size=self.calc_full_size(last_query,size_index)
        #cont_size=0
        for i in q.send_query(q.get_last_query()):
            tmp_url=url+i[name_index]
            #cont_size+=i[size_index]
            try:
                #status=r"Status Download: %10.2f/%10.2f MB  [%3.1f%%]"%(cont_size,full_size,cont_size*100.0/full_size)
                #status=status+chr(8)*(len(status)+1)
                #self.dispatch(status)
                self.download(url=tmp_url,path=path,filesize=i[size_index]*1024*1024)
            except :
                self.dispatch("Downloading is failed\n")
                break
        return "Done...Downloading is complete"
    
    def calc_full_size(self,tupla,size_index=0):
        tot=0
        for i in tupla:
            tot+=i[size_index]
        return tot
    
    def download(self,url="",path="",filesize=-1):
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
        self.dispatch("Downloading: %s "%(file_name,filesize/1024/1024))
        filesize_dl=0
        block_sz=8192
        while True:
            b_buffer=u.read(block_sz)
            if not b_buffer:
                break
            filesize_dl+=len(b_buffer)
            f.write(b_buffer)
            status=r" %10.2f/%10.2f MB  [%3.1f%%]"%(file_name,filesize_dl/1024.0/1024,filesize/1024/1024,filesize_dl*100.0/filesize)
            status=status+chr(8)*(len(status)+1)
            self.dispatch(status,)
        f.close()
    
class OpenOperation(Operation,Publisher):
    def __init__(self,args=[]):
        Operation.__init__(self, args)
    
    def run(self,q=Query()):
        #list of file names
        file_names=[i.split('/')[-1] for i in self.args ]
        path=q.get_path()
        if path=="":
            path=os.path.dirname(os.path.abspath(__file__))+"/Download/"
        for file_name in file_names:
            self.open(path,file_name)
        
    def open(self,path="",file_name=""):
        #open file in path and return all dimensions and all variables
        nc=Dataset(os.path.join(path,file_name),'r')
        string="\nFILENAME: "+file_name+"\n\t Dimensions: "
        for d in nc.dimensions.keys(): string+=d+" "
        string+="\n\t Variables: "
        for v in nc.variables.keys(): string+=v+" "
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