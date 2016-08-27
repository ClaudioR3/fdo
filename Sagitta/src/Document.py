'''
Created on Jul 12, 2016

@author: claudio
'''

class Document:
    def __init__(self,name):
        self.name=name
        self.params=self.read()
        
    def create_file(self):
        doc = open(self.name, "w")
        doc.close()
        return self.read()
        
    def read(self):
        try:
            doc= open(self.name, "r").read()
            p=doc.split("\n")
            params={}
            for i in p:
                lista_riga=i.split(";")
                if lista_riga!=[""]:
                    params[lista_riga[0]]=lista_riga[1]
            return params
        except:
            return self.create_file()
    
    def update(self,params):
        for p in params:
            self.get_params()[p]=params[p]
        self.write(self.get_params())
    
    def write(self,params):
        doc = open(self.name, "w")
        for p in params.keys():
            riga = "%s;%s\n"%(p,params[p])
            doc.write(riga)
        doc.close()
        self.set_params(self.read())
        
    def delete(self):
        doc = open(self.name, "w")
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
    
    def verif_conn_params(self):
        vcp=["host", "db", "user", "passwd", "table"]
        nfp=[i for i in vcp if i not in self.get_params().keys()]
        if len(nfp)!=0:
            if len(nfp)>1:
                return "%s are null"%nfp[0:]
            else:
                return "%s is null"%nfp[0]
        return "Wrong Config of Conn "+ "\n 'getconfig' to look your config "
        
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
    def __init__(self,name):
        #initialization of super-class
        Document.__init__(self, name)
        
    def read(self):
        #self.params= a map like {key:[list]}
        try:
            doc= open(self.name, "r").read()
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