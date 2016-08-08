'''
Created on 03 ago 2016

@author: claudio
'''
import urllib2
import os

class Subscriber:
    def __init__(self,name):
        self.name=name
    def update(self,message):
        if self.name=="Terminal":
            print message,

class Publisher:
    def __init__(self):
        self.subscribers=[]
    def register(self,subscriber):
        self.subscribers.append(subscriber)
    def remove_subscriber(self,subscriber):
        self.subscribers.remove(subscriber)
    def dispatch(self,message):
        for s in self.subscribers:
            s.update(message) 
            
class NC_download(Publisher):
    def __init__(self):
        Publisher.__init__(self)
    
    def download(self,url="",path="",filesize=-1):
        #size(filesize)=MB
        file_name=url.split('/')[-1]
        try:
            u=urllib2.urlopen(url)
        except:
            #TO Do
            req=""
            u=urllib2.urlopen(req)
        #check path
        if not os.path.exists(path):
            os.makedirs(path)
        #new file=file_name in path
        f=open(os.path.join(path,file_name),'wb')
        if filesize==-1:
            try:
                meta=u.info()
                #When the method is HTTP
                filesize=int(meta.getheaders("Content-Length")[0])
            except:
                self.dispatch("problems on size of file :\n")
                #filesize = casual number (never mind)
                filesize=1
        self.dispatch("Downloading: %s      MBs: %10.2f \n"%(file_name,filesize/1024/1024))
        filesize_dl=0
        block_sz=8192
        while True:
            b_buffer=u.read(block_sz)
            if not b_buffer:
                break
            filesize_dl+=len(b_buffer)
            f.write(b_buffer)
        
            status=r"%10.1f MB  [%3.1f%%]"%(filesize_dl/1024/1024,filesize_dl*100.0/filesize)
            status=status+chr(8)*(len(status)+1)
            self.dispatch(status)
        f.close()