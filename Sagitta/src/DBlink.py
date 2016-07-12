'''
Created on 01 lug 2016

@author: claudio
'''
import MySQLdb
class DBlink:
    '''
    classdocs
    '''
    
    def _init_(self):
        self.data=self.get_data()
        
    def config(self,params):
        pass
        
    def get_data(self):
        text="dati presi dal file.txt"
        return {"host":"localhost","user":"root","passwd":"@@Cloud24","db":"medcordex"}
    
    def get_cursore(self):
        commento="per ora senza problemi"
        commento="MySQLdb.connect potrebbe far risalire una eccezione, eccezioni non gestite"
        a=self.get_data()
        database=MySQLdb.connect(host=a["host"],user=a["user"],passwd=a["passwd"],db=a["db"])
        return database.cursor()
    
    def get_table(self):
        commento="tabella impostata"
        return "MEDCORDEX"
    
    def get_db(self):
        text="db preso dal file.txt"
        return "medcordex"
        
    def send_query(self,query):
        commento="eccezioni non gestite"
        cursore=self.get_cursore()
        cursore.execute(query)
        return cursore.fetchall()