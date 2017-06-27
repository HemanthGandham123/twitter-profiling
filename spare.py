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
node=Node("Grande",id=0,name="zero_node",type=0,date=0)
graph.create(node)
array=[]
rel_ind=1
rel_type=0
score={}
tmp={}
tp=[]
backpatch={}
#dic created
retweet_rel={}
n4= graph.find_one("Grande",property_key = 'id', property_value = 0)
for fname in iglob(os.path.expanduser('output1/*.json')):
    with open(fname) as fin:
        print (fname)
        hf=1
        mf=1
        exist=1
        tweets= json.load(fin)
        array.append(tweets)
        auth_id=tweets['meta']['author_id']
        auth_name=tweets['meta']['author_name']
        #print (auth_id)
        dt=tweets['meta']['date']['$date']
        n1= graph.find_one("Grande",property_key = 'name', property_value = auth_name)
        if(n1==None):
            exist=0
        #if(dt>n1['date']):
         #   n1['date']=dt
        #adding data to dic from each file
        rtof=tweets['meta']['retweetOf']
        if not(rtof is None):
            retweet_rel[auth_id]=rtof
        nh=[]
        if tweets['text']['entities']['hashtags']:
            for hshtgo in tweets['text']['entities']['hashtags']:
                hshtg=hshtgo['text']
                hf=0
                if(graph.find_one("Grande",property_key='text',property_value=hshtg)==None):
                    node=Node("Grande",text=hshtg,type=2)
                    graph.create(node)
                score.setdefault(auth_name,{}).setdefault(hshtg,0)
                score[auth_name][hshtg]+=8
                score.setdefault(hshtg,{}).setdefault(auth_name,0)
                score[hshtg][auth_name]+=8
                n2=graph.find_one("Grande",property_key='text',property_value=hshtg)
                nh.append(n2)
                if(exist==1):
                    r_id=tofrac(rel_ind)+1
                    st=str(r_id)
                    rel_ind=rel_ind+1
                    rel=Relationship(n1,st,n2,content=tweets['text']['content'])
                    graph.create(rel)
                else:
                    if(auth_name in backpatch):
                        backpatch[auth_name].append(hshtg)
                    else:
                        backpatch[auth_name]=[]
                        backpatch[auth_name].append(hshtg)
                #print ('hi')
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
        mn_dict=tweets['text']['entities']['mention']
        if (mn_dict !=None):
            mndx=0
            for ment in mn_dict:
                m_name=mn_dict[mndx]['screen_name']
                #print (m_name)
                if(graph.find_one("Grande",property_key = 'name', property_value =m_name)==None):
                    node = Node("Grande", name=m_name,type=3)
                    graph.create(node)
                n3= graph.find_one("Grande",property_key = 'name', property_value =m_name )
                nm.append(n3)
                score.setdefault(auth_name,{}).setdefault(m_name,0)
                score[auth_name][m_name]+=8
                score.setdefault(m_name,{}).setdefault(auth_name,0)
                score[m_name][auth_name]+=8
                if(exist==1):
                    r_id=tofrac(rel_ind)+2
                    st=str(r_id)
                    rel_ind=rel_ind+1
                    rel=Relationship(n1,st,n3,content=tweets['text']['content'])
                    graph.create(rel)
                else:
                    if(auth_name in backpatch):
                        backpatch[auth_name].append(m_name)
                    else:
                        backpatch[auth_name]=[]
                        backpatch[auth_name].append(m_name)
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
        #create mention when there is no hashtag and mention(extra condition for empty tweeting users (empty user becomes useless node))
        if(hf==1 and mf==1):
            if(exist==0):
                n1 = Node("Grande",id=auth_id, name=tweets['meta']['author_name'],bundle_id=tweets['meta']['retweetOf'])
                graph.create(n1)
            r_id=tofrac(rel_ind)
            st=str(r_id)
            rel_ind=rel_ind+1
            rel=Relationship(n1,st,n4,content=tweets['text']['content'])
            graph.create(rel)
print (backpatch)
#print (score)
#for each element in dic creating relationships
for my_var in retweet_rel:
    n1=graph.find_one("Grande",property_key = 'id', property_value = my_var)
    n2=graph.find_one("Grande",property_key='bundle_id',property_value=retweet_rel[my_var])
    if(n1!=None and n2!= None):
        r_id=tofrac(rel_ind)+4
        st=str(r_id)
        rel_ind=rel_ind+1
        rel=Relationship(n1,st,n2,content=tweets['text']['content'])
        graph.create(rel)
        jna=n2['name']
        score.setdefault(auth_name,{}).setdefault(jna,0)
        score[auth_name][jna]+=4
        #print (my_var, ' : ', retweet_rel[my_var])
death_note=[]
for i in backpatch:
    n1=graph.find_one("Grande",property_key = 'name', property_value =i)
    n2=graph.find_one("Grande",property_key='name',property_value=backpatch[i])
    if(n1 is None):
        death_note.append(i)
    else:
        r_id=tofrac(rel_ind)+1
        st=str(r_id)
        rel_ind=rel_ind+1
        rel=Relationship(n1,st,n2)
        graph.create(rel)
for i in death_note:
    del backpatch[i]
print (death_note)
print ("ed sheeran")
print (backpatch)    
qry="ImHagarwal"
closer=sorted(score[qry].items(), key=lambda x: x[1],reverse=True)
closest=closer[:2]
#print (score)
for i in closest:
        print (i)
