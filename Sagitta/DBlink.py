'''
Created on 01 lug 2016

@author: claudio
'''
import mysql.connector as mysql
from Document import Document
class DBlink:
    '''
    This class links the others classes with a database. The database's informations are into the document 
    'config.txt' rappresenting by class Document called config.
    '''
    
    def __init__(self):
        '''
        Initialization of object Document for configuration data and all default info
        '''
        self.config=Document("config.txt")
        self.default_host='www.medcordex.eu'
        self.default_user='sagitta'
        self.default_passwd='sagitta'
        self.default_db='medcordex'
        
    def set_config(self,params):
        '''
        Set new params of config Document
        @param params: dictionary
        '''
        self.config.update(params)
    
    def get_config_toString(self):
        '''
        Take the params of config Document
        @return params: string, params of config Document
        '''
        return self.config.toString()
    
    def del_config(self):
        '''
        Delete all info of the config Document
        '''
        self.config.delete()
        
    def get_doc(self):
        '''
        @return config: Document object, config Document
        '''
        return self.config
    
    def get_table(self):
        '''
        @return table: string, table info into config Document
        '''
        return self.config.get_parameter("table")
    
    def get_db(self):
        '''
        @return db: string, db info into config Document
        '''
        return self.config.get_parameter("db")
    
    def get_url(self):
        '''
        @return url info: string token by 'config' Document. If url is a void string and the host info is equals 
        to www.medcordex.eu return the default url for medcordex users.
        '''
        url= self.config.get_parameter("url")
        if url=="":
            #default url for medcordex users
            if self.config.get_parameter("host")=="www.medcordex.eu":
                url="ftp://"+self.config.get_parameter("user")+":"+self.config.get_parameter("passwd")+"@"+self.config.get_parameter("host")+"/ALL/"
        return url
        
    def send_query(self,query):
        '''
        Send query using mysql.connector module. To send the query, the function needs the informations
        host, user, passwd and db token by 'config' Document. If the host and db info are void string, the function send the query
        to default info.
        @param query: string in form like SELECT fields FROM table WHERE conditions
        @return tuple: query response
        '''
        if self.config.get_parameter("host")=='' and self.config.get_parameter('db')=='':
            # default configuration for medcordex users
            database=mysql.connect(host=self.default_host,
                                     user=self.default_user,
                                     passwd=self.default_passwd,
                                     db=self.default_db)
        else:
            # personal configuration for all other cases
            database=mysql.connect(host=self.config.get_parameter("host"),
                                     user=self.config.get_parameter("user"),
                                     passwd=self.config.get_parameter("passwd"),
                                     db=self.config.get_parameter("db"))
        cursore=database.cursor()
        cursore.execute(query)
        return cursore.fetchall()