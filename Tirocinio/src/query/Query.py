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
        mappa=pf.scandisciArg()
        for k in mappa:
            if self.WHERE!="":
                self.WHERE+=" and "
        self.WHERE+=mappa[k].toString()

    
    def getWhere(self):
        return self.WHERE
    
    def getSelect(self):
        return self.SELECT
    
    def getFrom(self):
        return self.FROM
    
    def setFrom(self,f):
        self.FROM=f
    
    def setSelect(self,s):
        self.SELECT=s
        
    def add_in_select(self,s):
        self.SELECT+=", "+s
    
    def toString(self):
        if self.getWhere()!="":
            return "select "+self.getSelect()+" from "+self.getFrom()+" where "+self.getWhere()
        return "select "+self.getSelect()+" from "+self.getFrom()
        
        
        
        
        