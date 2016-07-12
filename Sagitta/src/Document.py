'''
Created on Jul 12, 2016

@author: claudio
'''

class Document:
    '''
    classdocs
    '''


    def __init__(self):
        self.params=self.get_params_from_doc()
        
    def get_params_from_doc(self):
        params={"host":"www.medcordex.eu","user":"mdcx172","passwd":"23=Dh+1","db":"medcordex","table":"MEDCORDEX"}
        return params
    
    def get_params(self):
        return self.params