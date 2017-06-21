from glob import iglob
import os
import requests
import json
from py2neo import Graph,Path,authenticate,Node,Relationship
authenticate("localhost:7474","t","t")
graph = Graph("http://localhost:7474/db/data/")
def tofrac(a):
    b=float(10**len(str(a)))
    c=float(a)
    d=c/b
    return float(d)
#Relationship(a, "similartags", b, weight = tcount)
node=Node("Ariana",id=0,name="zero_node",type=0,date=0)
graph.create(node)
array=[]
rel_ind=1
rel_type=0
score={}
tmp={}
n4= graph.find_one("Ariana",property_key = 'id', property_value = 0)
for fname in iglob(os.path.expanduser('output1/*.json')):
    with open(fname) as fin:
        print (fname)
        hf=1
        mf=1
        tweets= json.load(fin)
        array.append(tweets)
        auth_id=tweets['meta']['author_id']
        auth_name=tweets['meta']['author_name']
        print (auth_id)
        dt=tweets['meta']['date']['$date']
        if(graph.find_one("Ariana",property_key = 'id', property_value = auth_id)==None):
            node = Node("Ariana",id=auth_id, name=tweets['meta']['author_name'],bundle_id=tweets['meta']['retweetOf'])
            graph.create(node)
            #score.setdefault(auth_name,{})
        n1= graph.find_one("Ariana",property_key = 'id', property_value = auth_id)
        #if(dt>n1['date']):
         #   n1['date']=dt
        nh=[]
        if tweets['text']['entities']['hashtags']:
            for hshtgo in tweets['text']['entities']['hashtags']:
                hshtg=hshtgo['text']
                hf=0
                if(graph.find_one("Ariana",property_key='text',property_value=hshtg)==None):
                    node=Node("Ariana",text=hshtg,type=2)
                    graph.create(node)
                score.setdefault(auth_name,{}).setdefault(hshtg,0)
                score[auth_name][hshtg]+=8
                score.setdefault(hshtg,{}).setdefault(auth_name,0)
                score[hshtg][auth_name]+=8
                n2=graph.find_one("Ariana",property_key='text',property_value=hshtg)
                nh.append(n2)
                r_id=tofrac(rel_ind)+1
                st=str(r_id)
                rel_ind=rel_ind+1
                rel=Relationship(n1,st,n2,content=tweets['text']['content'])
                graph.create(rel)
                print ('hi')
                hf=0
            for i in nh:
                for j in nh:
                    if(i!=j):
                        ina=i["text"]
                        jna=j["text"]
                        score.setdefault(ina,{}).setdefault(jna,0)
                        score[ina][jna]+=4
                        r_id=tofrac(rel_ind)+5
                        st=str(r_id)
                        rel_ind=rel_ind+1
                        rel=Relationship(i,st,j,content=tweets['text']['content'])
                        graph.create(rel)
        nm=[]
        if tweets['text']['entities']['mention']:
            for ment in tweets['text']['entities']['mention']:
                mnt=ment.values()
                m_id=str(list(mnt)[2])
                m_name=str(list(mnt)[4])
                if(graph.find_one("Ariana",property_key = 'id', property_value =m_id)==None):
                    node = Node("Ariana", name=m_name,id=m_id,type=3)
                    graph.create(node)
                n3= graph.find_one("Ariana",property_key = 'id', property_value = m_id)
                nm.append(n3)
                score.setdefault(auth_name,{}).setdefault(m_name,0)
                score[auth_name][m_name]+=8
                score.setdefault(m_name,{}).setdefault(auth_name,0)
                score[m_name][auth_name]+=8
                r_id=tofrac(rel_ind)+2
                st=str(r_id)
                rel_ind=rel_ind+1
                rel=Relationship(n1,st,n3,content=tweets['text']['content'])
                graph.create(rel)
                for i in nm:
                    for j in nm:
                        if(i!=j):
                            ina=i["text"]
                            jna=j["text"]
                            score.setdefault(ina,{}).setdefault(jna,0)
                            score[ina][jna]+=4
                            r_id=tofrac(rel_ind)+6
                            st=str(r_id)
                            rel_ind=rel_ind+1
                            rel=Relationship(i,st,j,content=tweets['text']['content'])
                            graph.create(rel)
                if(hf==0):
                    #mention and hashtag
                    for i in nm:
                        for j in nh:
                            ina=i["text"]
                            jna=j["text"]
                            score.setdefault(ina,{}).setdefault(jna,0)
                            score[ina][jna]+=5
                            r_id=tofrac(rel_ind)+3
                            st=str(r_id)
                            rel_ind=rel_ind+1
                            rel=Relationship(i,st,j,content=tweets['text']['content'])
                            graph.create(rel)
                mf=0
        if(hf==1 and mf==1):
            r_id=tofrac(rel_ind)
            st=str(r_id)
            rel_ind=rel_ind+1
            rel=Relationship(n1,st,n4,content=tweets['text']['content'])
            graph.create(rel)
for fname in iglob(os.path.expanduser('output1/*.json')):
    with open(fname) as fin:
        tweets= json.load(fin)
        array.append(tweets)
        auth_id=tweets['meta']['author_id']
        auth_name=tweets['meta']['author_name']
        print (auth_id)
        n1=graph.find_one("Ariana",property_key = 'id', property_value = auth_id)
        if (n1==None):
            hf=1
        else :
            rtof=tweets['meta']['retweetOf']
            if not(rtof is None):
                n2=graph.find_one("Ariana",property_key='bundle_id',property_value=rtof)
                r_id=tofrac(rel_ind)+4
                st=str(r_id)
                rel_ind=rel_ind+1
                rel=Relationship(n1,st,n2,content=tweets['text']['content'])
                graph.create(rel)
                jna=n2['name']
                score.setdefault(auth_name,{}).setdefault(jna,0)
                score[auth_name][jna]+=4
            #    score.setdefault(jna,{}).setdefault(auth_name,0)
            #    score[jna][auth_name]+=4
#print (score)
