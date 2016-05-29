'''
Created on 28 mag 2016

@author: claudio
'''

class Argomento:
    '''
    classdocs
    '''
    def __init__(self, nome=""):
        self.nome=nome
        
    def getNome(self):
        return self.nome
    
    def toString(self):
        raise Exception("NotImplementedMethod")