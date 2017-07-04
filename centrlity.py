from glob import iglob
import os
import requests
import json
from py2neo import Graph,Path,authenticate,Node,Relationship
authenticate("localhost:7474","t","t")
graph = Graph("http://localhost:7474/db/data/")
file=open("a.txt","r")
clus=int(file.read())
cent={}
val=[]
ari=[]
'''for w in sorted(d, key=d.get, reverse=True):
  print w, d[w]
'''
ari.append('zero_node')
distr=[]
for i in range(1,clus+1):
    mac={}
    j=0
    for rec in graph.run("Match (n) where n.cid={cech} return n.name,n.deg",cech=i):
        j+=1
        name=str(rec[0])
        degree=str(rec[1])
        mac[name]=int(degree)
        distr.append(mac[name])
    if(j<3):
        ari.append(max(mac,key=mac.get))
    else:
        maxi=[]
        mkey=[]
        for w in sorted(mac,key=mac.get,reverse=True):
            maxi.append(mac[w])
            mkey.append(w)
        if(maxi[0]>( (4*maxi[1]) /3 ) ):
            ari.append(mkey[0])
        else:
            fit=[0,0,0]
            n1=graph.find_one("Grande",property_key='name',property_value=mkey[0])
            n2=graph.find_one("Grande",property_key='name',property_value=mkey[1])
            n3=graph.find_one("Grande",property_key='name',property_value=mkey[2])
            nodes=[n1,n2,n3]
            for i in range(0,3):
                for j in range(0,3):
                    if(i!=j):
                        for k in graph.run("match (n)--(m) where n.name={granit} and m.name={xhaka} return n",granit=nodes[i]['name'],xhaka=nodes[j]['name']):
                            fit[i]+=1
            ari.append(mkey[fit.index(max(fit))])
print (ari)
distr.sort(reverse=True)
print (distr)
