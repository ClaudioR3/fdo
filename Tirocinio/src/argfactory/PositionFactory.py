'''
Created on 28 mag 2016

@author: claudio
'''
from argfactory.Factory import Factory
from argv.VariableName import VariableName
from argv.Frequency import Frequency
import sys

class PositionFactory(Factory):
    '''
    classdocs
    '''
    def _init_(self):
        Factory._init_(self)
    def scandisciArg(self):
        mappa={}
        i=0
        for arg in sys.argv:
            if i==1:
                arg0=VariableName(arg)
                tipo_classe=arg0.__class__.__name__
                if mappa.has_key(tipo_classe)==False:
                    mappa[tipo_classe]=arg0
            elif i==2:
                arg1=Frequency(arg)
                tipo_classe=arg1.__class__.__name__
                if mappa.has_key(tipo_classe)==False:
                    mappa[tipo_classe]=arg1
            i+=1
        return mappa