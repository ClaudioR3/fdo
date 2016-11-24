'''
Created on Jul 29, 2016

@author: claudio
'''
import urllib2 
import os
from Query import Query
from Observer import Publisher
from netCDF4 import Dataset
from Document import Document

class Operation(Publisher):
    '''
    Abstract Operation Class
    '''
    def __init__(self, args=[]):
        Publisher.__init__(self)
        self.args=args
        self.default_path=os.path.dirname(os.path.abspath(__file__))+"/Download/"
    
    def run(self,q=Query()):
        '''
        Abstract main function for Operation subclass
        '''
        raise "Operation without the core"
    
    def set_args(self,args):
        '''
        Set argoments attr.
        @param args: list with string values
        '''
        self.args=args
        
    def args_to_map(self,l):
        '''
        From list to dictionary: 
        if list = ['-key1','value1','-key2','value2',...]
        create a dictionary = {'key1':'value1','key2':'value2',...}
        Generate exceptions in other cases.
        @param l: list with string values
        @return map_of_args: dictionary
        '''
        map_of_args={}
        #check if length of list it's a multiple of two (-key value)*n_time
        if len(l)%2 !=0: raise Exception("Insert all fields and values")
        for i in range(0,len(l)/2):
            #the key is l[i*2] and value is l[i*2+1]
            #miss key
            #key is not in form '-key'
            if l[i*2][0]!='-': raise Exception("You missed key")
            key=l[i*2].split('-')[1]
            map_of_args[key]=l[i*2+1]
        return map_of_args
    
class LogOperation(Operation):
    '''
    Log operation class
    '''
    def _init_(self,args=[]):
        Operation.__init__(self, args)
        
    def run(self,q=Query()):
        '''
        dispatch all argoments into log.txt
        '''
        log=Document("log.txt")
        story=log.get_params()
        for i in story:
            self.dispatch(story[i]+"\n")
        
    
class FindOperation(Operation):
    '''
    Find operation class
    '''
    def __init__(self,args=[]):
        Operation.__init__(self, args)
        
    def run(self,q=Query()):
        '''
        Do a query to database, with own argoments trasformated from list in dictionary, and a
        selection of fields dataset, size and fname.
        The result is a table dispatched to all substribers.
        @param q: Query class, need it to send the query.
        '''
        try :
            #do query with a map of args -> args_map={camp of database: value of camp}
            #select only in dataset and size for optimization
            triple=q.do_query(args=self.args_to_map(self.args),select="dataset,size,fname")
            self.dispatch(q.triple2table(triple))
        except Exception as e:
            self.dispatch(e)
            #self.dispatch(q.find_conn_probls())

class DescribeOperation(Operation):
    '''
    Describe operation class
    '''
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
    
class ConfigOperation(Operation):
    '''
    Config operation class
    '''
    def __init__(self,args=[]):
        Operation.__init__(self, args)
        
    def run(self,q=Query()):
        '''
        Configure connection data in config.txt
        @param q: Query class
        '''
        q.config_dblink(self.args_to_map(self.args))
        
class GetconfigOperation(Operation):
    '''
    Getconfig operation class
    '''
    def __init__(self,args=[]):
        Operation.__init__(self, args)
        
    def run(self,q=Query()):
        '''
        Dispatch configuration data
        '''
        self.dispatch(q.get_config_toString())
    
class DelconfigOperation(Operation):
    '''
    Del operation class
    '''
    def __init__(self,args="none"):
        Operation.__init__(self, args)
        
    def run(self,q=Query()):
        '''
        Delete configuration data
        '''
        q.del_config()
        self.dispatch("")
    
class WgetOperation(Operation):
    '''
    Wget operation class
    '''
    def __init__(self,args=[]):
        Operation.__init__(self, args)
    
    def run (self,q=Query()):
        '''
        Dispatch wget + url for each files selected in the last query.
        Wget is a Linux and Mac command, and it used to download a file into the URL.
        The url is token into config.txt or is the default url.
        @param q: Query class
        '''
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
        
    def wget(self,q):
        s=""
        url=q.get_url()
        try:
            index_fname=q.get_index("fname")
        except:
            self.dispatch("No 'fname' camp in database")
        try : 
            for a in q.do_query(query=q.get_last_query()):
                s+="\nwget "+url+a[index_fname]
            return s
        except ():
            self.dispatch("Last Query not founded or Wrong Config Data")
        
class SelectrowOperation(Operation):
    '''
    Selectrow operation class
    '''
    def __init__(self,args=[]):
        Operation.__init__(self, args)
        
    def run(self,q=Query()):
        '''
        Select the rows into last query and dispacth the new table of selected files
        @param q: Query class
        '''
        try:
            if len(self.args)==0: self.dispatch("At least one row")
            if self.is_int_and_uniq(self.args): 
                self.dispatch( q.triple2table(q.do_query(query=self.build_new_query(q))))
        except ValueError as te:
            self.dispatch( "%s is not int"%te[0])
        except SyntaxError as se:
            self.dispatch("%s is not uniq"%se[0])
        except NameError:
            self.dispatch("Last Query not founded")
        except Exception as e:
            self.dispatch(e)
        
    def is_int_and_uniq(self,args):
        '''
        Cheak if the argoments (args) are integer and uniq.
        @param args: list.
        @return boolean: True if the all argoments are uniq and integer, otherwise False.
        '''
        for elem in args:
            #check if elem is a number (no int->raise ValueError)
            int(elem)   
            #check if elem is not uniq
            if args.count(elem)>1:raise SyntaxError(elem)
        return True
            
    
    def build_new_query(self,q):
        '''
        Add in last query of Query q, the other conditions.
        @param q: Query class.
        @return new_query: string.
        '''
        new_query=q.get_last_query()
        if new_query=="":raise NameError
        new_query_div=new_query.split("where")
        #check if exists where in query
        if len(new_query_div)==1:
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
    '''
    Download operation class
    '''
    def __init__(self,args=[]):
        Operation.__init__(self, args)
        
    def run(self,q=Query()):
        '''
        Download any files into the path
        @param q: Query class
        '''
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
        '''
        Build Request class with headers like attrs
        @param url: string, a URL
        @param headers: dictionary, contains all headers' option for Request attribute
        '''
        req=urllib2.Request(url)    #build Request
        #set headers
        for k in headers:
            req.add_header(key=k, val=headers[k])
        return req
    
    def compare(self,n1,n2):
        '''
        @param n1:integer, bit of first file
        @param n2:integer, bit of second file
        @return boolean: if the sizes are equal 
        '''
        if n1==n2:
            return True
        else:
            #approximation
            return "%.2f"%(n1/1024.0/1024)=="%.2f"%(n2/1024.0/1024)
        
    def download_with_resume(self,url="",path="",file_size=0):
        '''
        Download file of URL into path. If file already exists: DO NOT download. 
        If the file exists but it's not fully downloaded, TRY resume of file (HTTP only).
        @param url: string, download's URL.
        @param path: string, download's path (where download the file of URL).
        @param file_size: integer, bit of file, how much is the file's size.
        '''
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
    '''
    Open operation class
    '''
    def __init__(self,args=[]):
        Operation.__init__(self, args)
    
    def run(self,q=Query()):
        '''
        
        '''
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
        '''
        Check if exist files .nc into the path
        @param path: string.
        @return l : list of the files .nc
        '''
        l=[]
        for f in os.listdir(path):
            if f.endswith(".nc"):
                l.append(f)
        return l
            
    def open(self,path,file_name,oneline_opt=False):
        '''
        @param path: string
        @param file_name: string
        @param oneline_opt: boolean
        '''
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
            string="\n\nFILENAME: "+file_name
            string+='\n'+ncdump(nc)
        self.dispatch(string)
        
class HelpOperation(Operation):
    '''
    Help operation class
    '''
    def __init__(self,args=[]):
        Operation.__init__(self, args)
        self.lista=["help","find","describe","config","getconfig","delconfig","wget","selectrow","download","open","log"]
        
    def run(self,q=Query()):
        try:
            #without args shows all operations,
            if len(self.args)==0: self.show_operations()
            #otherwise shows the description of operation in args
            else: self.show_descriptions()
        except Exception as e:
            self.dispatch(e)
            
            
    def show_descriptions(self):
        '''
        Check the language's option and a operation's name into argoments of the current class.
        Open the file into Manual with the selected language's option, and find the operation's name.
        Dispatch the descfiption of operation's name.
        
        '''
        try:
            language=self.getLanguage(self.args)+".txt"
            path=os.path.dirname(os.path.abspath(__file__))+"/Manual/"
            #check if language exists into path
            if os.path.exists(os.path.join(path,language)): doc=Document(language,path)
            #otherwise raise exception
            else : raise Exception("No language founded with name : "+language)
            for a in self.args:
                description=str(doc.get_parameter(a))
                if description!="": self.dispatch(description)
                else: raise Exception("Wrong operation or No describe implementation for this operation")
        except Exception as e:
            self.dispatch(e)
        
    def show_operations(self):
        '''
        Dispatch operations' list
        @return: list of operations
        '''
        s= "List of operation: \n"
        for elem in self.lista: s+= "'"+elem+"' "
        s+="\nTo know the operation's description, do: describe 'operation_name'"
        self.dispatch(s)
        
    def getLanguage(self,args):
        '''
        Cheak a language's option
        @param args: list
        @return language: string
        '''
        #default language
        language='en'
        for elem in args: 
            if language!='en':
                #too many language's options
                raise Exception("Too many language's option")
            if elem[0]=='-': language=elem[1:]
        return language
    
class NotOperation(Operation):
    '''
    Not operation class
    '''
    def __init__(self,args=[]):
        Operation.__init__(self, args)
    
    def run(self,q=Query()):
        '''
        If operation not exists
        '''
        self.dispatch("The operation not exists, 'help' to know the list of all operations")
