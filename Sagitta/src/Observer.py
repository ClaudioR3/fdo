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
    
    def download(self,url="",path=""):
        file_name=url.split('/')[-1]
        filename="10MB.zip"
        url="http://download.thinkbroadband.com/"+filename
        try:
            u=urllib2.urlopen(url)
        except:
            #if python hasn't permissions
            req=urllib2.Request(url,headers={'User-Agent':"Magic Browser"})
            u=urllib2.urlopen(req)
        else:
            raise "Problems"
        #check path
        if not os.path.exists(path):
            os.makedirs(path)
        #new file=file_name in path
        f=open(os.path.join(path,file_name),'wb')
        meta=u.info()
        filesize=int(meta.getheaders("Content-Length")[0])
        self.dispatch("Downloading: %s MBs: %s\n"%(file_name,filesize/1000000))

        filesize_dl=0
        block_sz=8192
        while True:
            b_buffer=u.read(block_sz)
            if not b_buffer:
                break
            filesize_dl+=len(b_buffer)
            f.write(b_buffer)
        
            status=r"%10.1f MB  [%3.1f%%]"%(filesize_dl/1000000,filesize_dl*100/filesize)
            status=status+chr(8)*(len(status)+1)
            self.dispatch(status)
        f.close()