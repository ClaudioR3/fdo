'''
Created on 29 mag 2016

@author: claudio
'''
import MySQLdb
class Connection:
    '''
    classdocs
    '''


    def __init__(self, host="localhost",user="root",passwd="",db="database"):
        self.host=host
        self.user=user
        self.passwd=passwd
        self.db=db
        self.doc="config_datas.txt"
        self.save_in_file()
    
    def run(self):
        try :
            database=MySQLdb.connect(host=self.host,user=self.user,passwd=self.passwd,db=self.db)
            return database.cursor()
        except (),e:
            print e
            return ""
    
    def setHost(self,h):
        self.host=h

    def setUser(self,u):
        self.user=u

    def setPasswd(self,p):
        self.passwd=p

    def setDB(self,d):
        self.db=d
    
    def save_in_file(self):
        try:
            document=open("config_datas.txt","w")
            document.write("host "+self.host+"\n")
            document.write("user "+self.user+"\n")
            document.write("passwd "+self.passwd+"\n")
            document.write("db "+ self.db+"\n")
            document.close()
        except ():
            print "DataConfig.txt non creato"
    
    def set_from_file(self):
        try:
            pass                 
        except():
            print "DataConfig.txt non presente"
            
    def exitsed_file(self):
        try :
            open(self.doc,"r")
            return 1
        except():
            return 0
    
    def send_query(self,query):
        cursore=self.run()
        try:
            cursore.execute(query)
            return cursore.fetchall()
        except():
            print "Problema della query"
            return ""
        
    def toString(self):
        return "host "+self.host+"\n"+"user "+self.user+"\n"+"passwd "+self.passwd+"\n"+"db "+self.db