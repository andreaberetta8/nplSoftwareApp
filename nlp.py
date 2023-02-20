import psycopg2
import spacy
import pandas as pd
import numpy as np


#queries
sql_doc = """INSERT INTO document(id,text) VALUES(%s,%s) ;"""
sql_line = """INSERT INTO line(id,doc_id,text) VALUES(%s,%s,%s)"""
sql_nmd_ent="""INSERT INTO named_entities(doc_id,line_id,id,text,startchar,endchar,label) VALUES(%s,%s,%s,%s,%s,%s,%s)"""
sql_noun_chunk = """INSERT INTO noun_chunk(doc_id,line_id,id,root_id,root_head_id,text) VALUES(%s,%s,%s,%s,%s,%s);"""
sql_token = """INSERT INTO token(doc_id,line_id,id,text,lemma,pos,tag,shape) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)"""
sql_dependency = """INSERT INTO dependency(tk_id,dep,head_id) VALUES(%s,%s,%s)"""
#connessione database
conn = psycopg2.connect(database="mydb", user='TOEDIT', password='TOEDIT', host='localhost', port= '5432')
conn.autocommit = True
cur = conn.cursor()
cur.execute("SET search_path TO nlp;")



nlp = spacy.load('en_core_web_sm')

#cur.execute(sql_doc,(d,text))  #inserimento doc
data = pd.read_csv("Desktop/tirocinio/brat/topics.csv", sep = ";")
for i in range(data.shape[0]):
    doc_id=i
    filename=data['File'].tolist()[doc_id]
    doc_text=""
    path="Desktop/tirocinio/brat/brat-project/brat-dialogs/"
    with open(path+filename) as f:
        doc_text = f.read()
    doc=nlp(doc_text)
    cur.execute(sql_doc,(doc_id,doc_text))  #inserimento doc
    
    line_id=0 #numero della frase all'inteno del doc
    #inserisco frasi
    for s in (doc.sents): #lines: #compie il set di operazioni su ciascuna frase del testo (s è di tipo Span)
        line_text=s.text #testo frase
        cur.execute(sql_line, (line_id,doc_id,line_text)) #inserisco frase
        
        nmd_ent_id=0 #indice named_entity
        #inseisco named entities
        l=nlp(line_text)#faccio diventare la frase un doc per accedere alle named entities perchè voglio le nmd_ent di ciascuna frase
        
        for e in l.ents:
            cur.execute(sql_nmd_ent,(doc_id,line_id,nmd_ent_id,e.text,e.start_char,e.end_char,e.label))
            nmd_ent_id+=1
        #inserisco token
        counter_tk=0 #indice token all'interno della frase
        list=[] #lista con terne(id,text,offset) al fine di inserire head_id nelle dependency
        tk_id="" #indicizza il token al livello piu superiore
        for token in l:
            tk_id=str(doc_id)+"#"+str(line_id)+"#"+str(counter_tk)
            #             0      1           2              3             4              5
            list.append((tk_id,token.dep_,token.text,token.head.text,token.head.idx,token.idx))
            cur.execute(sql_token, (doc_id,line_id,tk_id,token.text,token.lemma_,token.pos_,token.tag_,token.shape_,))
            counter_tk+=1
        #inserisco dependency
        for token in list:
            for head in list:
                if (token[3]==head[2] and token[4]==head[5]):
                    cur.execute(sql_dependency, (token[0],token[1],head[0],))

        #inserisco noun_chunk
        chunk_id=0
        chunk_root_id=""
        for chunk in l.noun_chunks:
            for root in list:
                if chunk.root.text==root[2] and chunk.root.idx==root[5]:
                    chunk_root_id=root[0]
            for root in list:
                if chunk.root.head.text==root[2] and chunk.root.head.idx==root[5]:
                    chunk_root_head_id=root[0]
            cur.execute(sql_noun_chunk, (doc_id,line_id,chunk_id,chunk_root_id,chunk_root_head_id,chunk.text))
            chunk_id+=1
        line_id+=1 #numero della frase all'inteno del doc
