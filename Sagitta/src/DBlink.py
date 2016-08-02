'''
Created on 01 lug 2016

@author: claudio
'''
import MySQLdb
from Document import Document
class DBlink:
    '''
    classdocs
    '''
    
    def __init__(self):
        self.doc=Document("config.txt")
        
    def config(self,params):
        self.doc.update(params)
    
    def get_config_toString(self):
        return self.doc.toString()
    
    def del_config(self):
        self.doc.delete()
        
    def get_doc(self):
        return self.doc
    
    def get_cursore(self):
        database=MySQLdb.connect(host=self.doc.get_parameter("host"),
                                 user=self.doc.get_parameter("user"),
                                 passwd=self.doc.get_parameter("passwd"),
                                 db=self.doc.get_parameter("db"))
        return database.cursor()
    
    def get_table(self):
        return self.doc.get_parameter("table")
    
    def get_db(self):
        return self.doc.get_parameter("db")
    
    def get_url(self):
        return "ftp://"+self.doc.get_parameter("user")+":"+self.doc.get_parameter("passwd")+"@"+self.doc.get_parameter("host")+"/ALL/"
        
    def send_query(self,query):
        cursore=self.get_cursore()
        cursore.execute(query)
        return cursore.fetchall()
    
    def find_conn_probls(self):
        try:
            self.get_cursore()
            return "Table Problem \n you can do 'config -table TABLE' to define table"
        except:
            return self.doc.verif_conn_params()
        