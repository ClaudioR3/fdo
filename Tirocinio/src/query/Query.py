'''
Created on 29 mag 2016

@author: claudio
'''
from argfactory.PositionFactory import PositionFactory

class Query:
    '''
    classdocs
    '''

    def __init__(self,SELECT="*",FROM="table",WHERE=""):
        self.SELECT=SELECT
        self.FROM=FROM
        self.WHERE=WHERE
        
    def set_where_with_args(self):
        pf=PositionFactory()
        try:
            mappa=pf.scandisciArg()
            for k in mappa:
                try:
                    if self.WHERE!="":
                        self.WHERE+=" and "
                    self.WHERE+=mappa[k].toString()
                except ():
                    print "argomento "+k+" non inserito"
        except:
            print "Problemi scansione argomenti"
    
    def getWhere(self):
        return self.WHERE
    
    def getSelect(self):
        return self.SELECT
    
    def getFrom(self):
        return self.FROM
    
    def setFrom(self,f):
        self.FROM=f
    
    def toString(self):
        return "select "+self.getSelect()+" from "+self.getFrom()+" where "+self.getWhere()      