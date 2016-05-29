'''
Created on 28 mag 2016

@author: claudio
'''
from argv.Argomento import Argomento

class VariableName(Argomento):
    '''
    classdocs
    '''
    def __init__(self, nome):
        Argomento.__init__(self, nome)
        
    def toString(self):
        return "VariableName='"+ self.getNome()+"'"