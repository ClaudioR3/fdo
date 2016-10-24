'''
Created on Jul 12, 2016

@author: claudio
'''
import os

class Document:
    def __init__(self,name,path=os.getenv('HOME')):
	try:
            #for linux and mac systems
       	    self.name=name
            self.path=path
            self.params=self.read()
        except TypeError:
            #for windows system 
            self.path=os.path.expanduser(os.getenv('USERPROFILE'))+'\\Documents\\Sagitta'
            self.params=self.read()
            
        
    def create_file(self):
        #create the path if not exists
        if not os.path.exists(self.path):
            os.makedirs(self.path)
        #create the file in path
        doc = open(os.path.join(self.path,self.name), "w")
        if self.name=='config.txt':
            message='host;www.medcordex.eu\ndb;medcordex\ntable;MEDCORDEX'
            message=self.cript(message,(2,8,-1,4))
            doc.write(message)
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
        try:
            f= open(os.path.join(self.path,self.name), "r")
            doc=f.read()
            if self.name=='config.txt':
                doc=self.cript(doc,(-2,-8,1,-4))
            p=doc.split("\n")
            params={}
            for i in p:
                lista_riga=i.split(";")
                if lista_riga!=[""]:
                    params[lista_riga[0]]=lista_riga[1]
            f.close()
            return params
        except:
            return self.create_file()
    
    def update(self,params):
        for p in params:
            self.params[p]=params[p]
        self.write(self.params)
    
    def write(self,params):
        doc = open(os.path.join(self.path,self.name), "w")
        for p in params.keys():
            riga = "%s;%s\n"%(p,params[p])
            if self.name=='config.txt': riga=self.cript(text,(2,8,-1,4))
            doc.write(riga)
        doc.close()
        self.set_params(self.read())
        
    def delete(self):
        doc = open(os.path.join(self.path,self.name), "w")
        doc.write("")
        doc.close()
    
    def get_params(self):
        return self.params
    
    def set_params(self,params):
        self.params=params
    
    def get_parameter(self,key):
        if key in self.params:
            return self.params[key]
        return ""

    def toString(self):
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
