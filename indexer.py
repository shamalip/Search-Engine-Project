import os
import json
import nltk
from nltk.tokenize import RegexpTokenizer
from pymongo import MongoClient
from bs4 import BeautifulSoup

HOME = os.environ["CRAWLEDLOC"]
UPDATEFREQ = os.environ['UPDATEFREQ']
reg_tokenizer = RegexpTokenizer('[a-zA-Z0-9]+')

def setup_db_connection():
    con = MongoClient(os.environ["DBCONNECTION"])
    return con.mydb

def get_file_list(full_file_path):
    with open(full_file_path) as f:
        content = json.loads(f.read())
    return content

def parse_documents():
    index_files_dict = get_file_list(HOME + 'bookkeeping.json')
    conn = setup_db_connection()
    i = 0
    inv_idx = {}
    for doc in index_files_dict:
        raw_html = open(HOME + "/" + doc, encoding='utf-8')
        parsed_html = BeautifulSoup(raw_html, 'html.parser')
        [s.extract() for s in parsed_html(['script', 'style','link'])]
        tkns = reg_tokenizer.tokenize(" ".join(parsed_html.strings))
        for k in range(len(tkns)):
            if(lens(tkns[k])):
            if tkns[k] in inv_idx:
                inv_idx[tkns[k]]['c'] = inv_idx[tkns[k]]['c'] + 1
                inv_idx[tkns[k]]['u'] = inv_idx[tkns[k]]['u'] + ',' + doc
            else:
                inv_idx[tkns[k]] = {
                'c' : 1,
                'u' : doc
                }
        i = i + 1
        if(i == UPDATEFREQ):
            update_index(conn,inv_idx)
            inv_idx = {}
            i = 0
        print('Successfully parsed:' , doc)
    if i > 0 and i < UPDATEFREQ:
        update_index(conn,inv_idx)
            
def update_index(conn,idx):
    for key in idx:
        obj = conn.inverted_idx.find_one({key: {"$exists": True}})
        print(obj)
        if None != obj:
            new_value ={}
            new_value['c']= idx[key]['c'] + obj[key]['c'] 
            new_value['u'] = idx[key]['u'] + ',' + obj[key]['u']           
            conn.inverted_index.update_one({"_id": obj["_id"]}, {"$set": {key: new_value}})
        else:
            conn.inverted_idx.insert_one({key: idx[key]})       
    print('write success')

parse_documents()