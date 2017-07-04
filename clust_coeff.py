from glob import iglob
import os
import requests
import json
from py2neo import Graph,Path,authenticate,Node,Relationship
authenticate("localhost:7474","t","t")
graph = Graph("http://localhost:7474/db/data/")
file=open("a.txt","r")
clus=int(file.read())
gomz=[]
gomz.append(0)
for i in range(1,clus+1):
    su=0
    j=0
    for rec in graph.run("Match (n) where n.cid={ozil} return n.deg",ozil=i):
        deg=str(rec[0])
        su+=int(deg)
        j+=1
    coeff=su/(j*(j-1))
    gomz.append(coeff)

print (gomz)
