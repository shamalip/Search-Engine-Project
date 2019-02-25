import os
import concurrent.futures
from pymongo import MongoClient
import nltk
import json
import time
import math
from bs4 import BeautifulSoup
from nltk.corpus import stopwords
from nltk.tokenize import RegexpTokenizer
from nltk.stem import WordNetLemmatizer
import nltk
from collections import deque
import configparser
import time
import nltk
from nltk.corpus import stopwords
#nltk.download('wordnet')
#nltk.download('stopwords')
stop_words = set(stopwords.words('english'))
base_path = "WEBPAGES_RAW"
reg_tokenizer = RegexpTokenizer('[a-zA-Z0-9]+')
config = configparser.RawConfigParser()
config.read('config.properties')
dburl = config.get('DB properties', 'DBCONNECTION')
wordnet_lemmatizer = WordNetLemmatizer()

def process_queue(idx):
    conn = setup_db_connection()
    for key in idx:
        obj = conn.inverted_idx.find_one({key: {"$exists": True}})
        if None != obj:
            new_value ={}
            new_value['c']= idx[key]['c'] + obj[key]['c'] 
            new_value['u'] = idx[key]['u'] + obj[key]['u']   
            print(new_value)
            print(obj)
            conn.inverted_index.update_one({"_id": obj["_id"]}, {"$set": {key: new_value}})
        else:
            print(key,idx[key])
            conn.inverted_idx.insert_one({key: idx[key]})       
    print('Write success at: ', time.time())      
    
def get_file_list(full_file_path):
    with open(full_file_path) as f:
        content = json.loads(f.read())
    return content

def parse_document(doc):
    print('starting ' + doc)
    inv_idx = {}
    raw_html = open(base_path + "/" + doc, encoding='utf-8')
    parsed_html = BeautifulSoup(raw_html, 'html.parser')
    [s.extract() for s in parsed_html(['script', 'style','link'])]
    tkns = reg_tokenizer.tokenize(" ".join(parsed_html.strings))
    for k in range(len(tkns)):
        if tkns[k] not in stop_words and len(tkns[k]) > 2:
            word = wordnet_lemmatizer.lemmatize(tkns[k].lower())
            if word in inv_idx:
                inv_idx[word]['c'] = inv_idx[word]['c'] + 1
                inv_idx[word]['u'].append(doc)
            else:
                inv_idx[word] = {'c' : 1,'u' : list([doc])}
    ##To push message to RQ, so that DB writes can be processed in parallel. But RQ doesn't work on windows.
    ##q.enqueue(process_queue,inv_idx)  
    process_queue(inv_idx)
    print('Successfully parsed:' , doc)

def setup_db_connection():
    con = MongoClient(dburl)
    return con.mydb

if __name__ == '__main__':
    ##global q = Queue(connection=Redis())  
    with concurrent.futures.ProcessPoolExecutor() as executor:
        # Get a list of files to process
        files_map = get_file_list(base_path + '/bookkeeping.json')
        files_list = files_map.keys()
        print('starting process at' ,time.time())
        # Process the list of files, but split the work across the process pool to use all CPUs!
        deque(executor.map(parse_document, files_list))
