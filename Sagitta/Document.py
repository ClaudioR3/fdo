'''
Created on Jul 12, 2016

@author: claudio
'''
import os

class Document:
    '''
    This class reads and writes into a text file. Converts the datas into file in a dictionary like {key:value}. 
    '''
    def __init__(self,name,path=os.getenv('HOME')):
        '''
        The initialization decides the directory to save the file. If the OS is Linux or Mac OS it saves into 
        pathToHome/.Directory/fileName.txt otherwise into pathToUSERNAME/Documents/Directory/fileName.txt (default path).
        '''
        try:
            #for linux and Mac systems
            self.name=name
            self.path=path
            self.params=self.read()
        except TypeError:
            #for windows system 
            self.path=os.path.expanduser(os.getenv('USERPROFILE'))+'\\Documents\\Sagitta'
            self.params=self.read()

    def create_file(self):
        '''
        Create the file into path.
        @return the information into file (read)
        '''
        #create the path if not exists
        if not os.path.exists(self.path):
            os.makedirs(self.path)
        #create the file in path
        doc = open(os.path.join(self.path,self.name), "w")
        #if self.name=='config.txt':
        #    message='host;www.medcordex.eu\ndb;medcordex\ntable;MEDCORDEX'
        #    message=self.cript(message,(2,8,-1,4))
        #    doc.write(message)
        doc.close()
        #retry read
        return self.read()

    def cript(self,string, key):
        new_string = ""
        i = 0
        for char in string:
            new_string += chr((ord(char) + key[i]) % 128)
            i += 1
            if i >= len(key):
                i = 0
        return new_string	

    def read(self):
        '''
        Read the file path/fileName in form : string1;string2\n then it inserts the info in a dictionary
        in this form {string1:string2}
        @return params: dictionary, info into 'fileName' Document
        '''
        try:
            f= open(os.path.join(self.path,self.name), "rb")
            doc=f.read()
            f.close()
            #if self.name=='config.txt': doc=self.cript(doc,(-2,-8,1,-4))
            p=doc.split("\n")
            params={}
            for i in p:
                lista_riga=i.split(";")
                if lista_riga!=[""]:
                    params[lista_riga[0]]=lista_riga[1]
            return params
        except :
            #if the file are not found
            return self.create_file()
    
    def update(self,params):
        '''
        Update self.params in new params. Update is not set.
        @param params: dictionary, the params that need update
        '''
        for p in params:
            self.params[p]=params[p]
        self.write(self.params)
    
    def write(self,params):
        '''
        Write the params {key:value} into file in path/fileName in this form: key;value\n 
        @param params: dictionary.
        '''
        doc = open(os.path.join(self.path,self.name), "wb")
        for p in params.keys():
            riga = "%s;%s\n"%(p,params[p])
            #if self.name=='config.txt': riga=self.cript(riga,(2,8,-1,4))
            doc.write(riga)
        doc.close()
        self.set_params(self.read())
        
    def delete(self):
        '''
        Delete all data into file path/fileName
        '''
        doc = open(os.path.join(self.path,self.name), "w")
        doc.write("")
        doc.close()
    
    def get_params(self):
        '''
        @return params: dictionary.
        '''
        return self.params
    
    def set_params(self,params):
        '''
        Set params with the new params
        @param params : dictionary
        '''
        self.params=params
    
    def get_parameter(self,key):
        '''
        Return the value of key into params (dictionary).
        @param key: string
        @return value: string
        '''
        if key in self.params:
            return self.params[key]
        return ""

    def toString(self):
        '''
        @return s: string, params in string type
        '''
        s=""
        keyset=self.get_params().keys()
        keyset.sort()
        for k in keyset :
            s+= k+" = "+self.get_params()[k]+" \n"  
        return s
    
class KL_Document(Document):
    '''
    This class is different than simple Document class because 
    it must read like map={key:[list]} by 'name'.txt
    '''
    def __init__(self,name,path):
        #initialization of super-class
        Document.__init__(self, name,path)
        
    def read(self):
        '''
        Read the file path/fileName in form : string1;string2;string3;...;stringN then it inserts the info in a dictionary
        in this form {string1:[string2,...,stringN]}
        @return params: dictionary {key:[values]}
        '''
        #self.params= a map like {key:[list]}
        try:
            doc= open(os.path.join(self.path,self.name), "r").read()
            p=doc.split("\n")
            params={}
            for i in p:
                lista_riga=i.split(";")
                for elem in lista_riga[1:]:
                    if elem!="":
                        if lista_riga[0] in params:
                            params[lista_riga[0]].append(elem)
                        else : params[lista_riga[0]]=[elem]
            return params
        except:
            return self.create_file()   
