'''
Created on 28 mag 2016

@author: claudio
'''
from argv.Argomento import Argomento

class Frequency(Argomento):
    '''
    classdocs
    '''
    def __init__(self, nome):
        Argomento.__init__(self, nome)
    
    def toString(self):
        return "Frequency='"+ self.getNome()+"'"