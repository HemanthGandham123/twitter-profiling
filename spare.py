from glob import iglob
import os
import requests
import json
from py2neo import Graph,Path,authenticate,Node,Relationship
authenticate("localhost:7474","t","t")
graph = Graph("http://localhost:7474/db/data/")

array=[]
rel_ind=1
rel_type=0
score={}
tmp={}
backpatch={}
cluster_id=0

def tofrac(a):
    b=float(10**len(str(a)))
    c=float(a)
    d=c/b
    return float(d)

def clstr(n1,n2):
    global cluster_id
    tp=[]
    if(n1['cid']!=n2['cid']):
        tp.append(n1['cid'])
        tp.append(n2['cid'])
        pair.append(tp)

def recreate(n1,n2,con,a):
    global r_id
    global rel_ind
    r_id=tofrac(rel_ind)+a
    st=str(r_id)
    rel_ind=rel_ind+1
    rel=Relationship(n1,st,n2,content=con)
    graph.create(rel)
    clstr(n1,n2)
    graph.merge(n1)
    n1['deg']+=1
    n1.push()
    graph.merge(n2)
    n2['deg']+=1
    n2.push()

#Relationship(a, "similartags", b, weight = tcount)
node=Node("Grande",id=0,name="zero_node",type=0,date=0,cid=0,deg=0)
graph.create(node)
#dic created
retweet_rel={}
pair=[]
n4= graph.find_one("Grande",property_key = 'id', property_value = 0)
for fname in iglob(os.path.expanduser('jason/*.json')):
    with open(fname) as fin:
        #print (fname)
        hf=1
        mf=1
        exist=1
        tweets= json.load(fin)
        array.append(tweets)
        auth_id=tweets['meta']['author_id']
        auth_name=tweets['meta']['author_name']
        con=tweets['text']['content']
        #print (auth_id)
        #dt=tweets['meta']['date']['$date']
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
                if(graph.find_one("Grande",property_key='name',property_value=hshtg)==None):
                    cluster_id+=1
                    node=Node("Grande",name=hshtg,type=2,cid=cluster_id,deg=0)
                    graph.create(node)
                score.setdefault(auth_name,{}).setdefault(hshtg,0)
                score[auth_name][hshtg]+=8
                score.setdefault(hshtg,{}).setdefault(auth_name,0)
                score[hshtg][auth_name]+=8
                n2=graph.find_one("Grande",property_key='name',property_value=hshtg)
                nh.append(n2)
                if(exist==1):
                    recreate(n1,n2,con,2)
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
                        recreate(i,j,con,5)
        nm=[]
        mn_dict=tweets['text']['entities']['mention']
        if (mn_dict !=None):
            mndx=0
            for ment in mn_dict:
                m_name=mn_dict[mndx]['screen_name']
                #print (m_name)
                if(graph.find_one("Grande",property_key = 'name', property_value =m_name)==None):
                    cluster_id+=1
                    node = Node("Grande", name=m_name,type=3,cid=cluster_id,deg=0)
                    graph.create(node)
                n3= graph.find_one("Grande",property_key = 'name', property_value =m_name )
                nm.append(n3)
                score.setdefault(auth_name,{}).setdefault(m_name,0)
                score[auth_name][m_name]+=8
                score.setdefault(m_name,{}).setdefault(auth_name,0)
                score[m_name][auth_name]+=8
                if(exist==1):
                    recreate(n1,n3,con,3)
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
                            recreate(i,j,con,6)
                if(hf==0):
                    #mention and hashtag
                    for i in nm:
                        for j in nh:
                            ina=i["text"]
                            jna=j["text"]
                            score.setdefault(ina,{}).setdefault(jna,0)
                            score[ina][jna]+=5
                            recreate(i,j,con,3)
                mf=0
        #create mention when there is no hashtag and mention(extra condition for empty tweeting users (empty user becomes useless node))
        if(hf==1 and mf==1):
            if(exist==0):
                n1 = Node("Grande",id=auth_id, name=tweets['meta']['author_name'],bundle_id=tweets['meta']['retweetOf'],deg=0)
                graph.create(n1)
                cluster_id+=1
                graph.merge(n1)
                n1['cid']=cluster_id
                n1.push()
death_note=[]
#print (score)
#print (backpatch)
for i in backpatch:
    n1=graph.find_one("Grande",property_key = 'name', property_value =i)
    if(n1 is None):
        death_note.append(i)
    else:
        for j in backpatch[i]:
            n2=graph.find_one("Grande",property_key='name',property_value=j)
            if(n2 is None):
                print (j)
            else:
                recreate(n1,n2,con,1)
#print (death_note)
for i in death_note:
    del backpatch[i]
#for each element in dic creating relationships
for my_var in retweet_rel:
    n1=graph.find_one("Grande",property_key = 'id', property_value = my_var)
    n2=graph.find_one("Grande",property_key='bundle_id',property_value=retweet_rel[my_var])
    if(n1!=None and n2!= None):
        recreate(n1,n2,con,4)
        jna=n2['name']
        score.setdefault(auth_name,{}).setdefault(jna,0)
        score[auth_name][jna]+=4
        #print (my_var, ' : ', retweet_rel[my_var])
qry="narendramodi"
closer=sorted(score[qry].items(), key=lambda x: x[1],reverse=True)
closest=closer[:2]
print (pair)
#cluster id part
switch=[]
mac=0
for li in pair:
    if(mac<max(li)):
        mac=max(li)
for i in range (0,len(pair)+1):
        switch.append(0)
new=[]
for li in pair:
    if(switch[li[0]]==0 and switch[li[1]]==0 ):
        switch[li[0]]=1
        switch[li[1]]=1
        new.append(li)
    else:
        if(switch[li[0]]!=0 and switch[li[1]]!=0 and switch[li[0]!=switch[li[1]]]):
            for i in new:
                if li[0] in i:
                    a=new.index(i)
                    break
            for i in new:
                if li[1] in i:
                    b=new.index(i)
                    break
            if(a!=b):
                for j in new[b]:
                    if(j not in new[a]):
                        new[a].append(j)
                new[b]=[0]
        elif(switch[li[0]]!=0):
            for i in new:
                if li[0] in i:
                    a=new.index(i)
                    break
            if(li[1] not in new[a]):
                new[a].append(li[1])
                switch[li[1]]=1
        else:
            for i in new:
                if li[1] in i:
                    a=new.index(i)
                    break
            if(li[0] not in new[a]):
                new[a].append(li[0])
                switch[li[0]]=1
#filter(lambda a: a !=[0], new)
new[:] = (value for value in new if value !=[0])
for li in new:
    if(0 in li):
        a=li.index(0)
        del li[a]
for i in new:
    print (i)
ficid=[]
for li in new:
    ficid.append(li[0])
    for t in li:
        n1=graph.find_one("Grande",property_key='cid',property_value=t)
        graph.merge(n1)
        n1['cid']=li[0]
        n1.push()
for rec in graph.run("match (n:Grande) where n.deg=0 return n.cid"):
    ci=str(rec[0])
    ficid.append(ci)
filter(lambda a: a !=[0],ficid)
print (ficid)

rc=0
for i in ficid:
    rc+=1
    for rec in graph.run("match (n:Grande) where n.cid={mane} return n.name",mane=i):
        naam=str(rec[0])
        n1=graph.find_one("Grande",property_key='name',property_value=naam)
        graph.merge(n1)
        n1['cid']=rc
        n1.push()
