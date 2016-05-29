'''
Created on 29 mag 2016

@author: claudio
'''
from query.Query import Query
from connection.Connection import Connection

q=Query()
q.set_where_with_args()
q.setFrom("MEDCORDEX")
print q.toString()
c=Connection(passwd="@@Cloud24",db="medcordex")
for line in c.send_query(q.toString()):
    print line
