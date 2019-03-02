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
from collections import deque
import configparser
import time
import operator
from bookkeeping import files_map, inv_map
from urllib.parse import urljoin
#nltk.download('wordnet')
#nltk.download('stopwords')

stop_words = set(stopwords.words('english'))
base_path = "WEBPAGES_RAW"
reg_tokenizer = RegexpTokenizer('[a-zA-Z0-9]+')
config = configparser.RawConfigParser()
config.read('config.properties')
dburl = config.get('DB properties', 'DBCONNECTION')
wordnet_lemmatizer = WordNetLemmatizer()
## grouped to give equal relevance to titles and h1,h2. h3-h6 another weight, etc.
weighted_tags = [['title'],['h1','h2'], ['h3','h4','h5','h6'], ['strong','b','em','i','u','a']]

def get_file_list(full_file_path):
    with open(full_file_path) as f:
        content = json.loads(f.read())
    return content

def get_out_urls_as_doc(parsed_html):
    return parsed_html.find_all('a', href=True)

def extract_weighted_strings(html):
    weighted_strings = []
    for i in range(len(weighted_tags)):
        weighted_strings.append(" ".join(map(operator.attrgetter("text"), html.find_all(weighted_tags[i]))))
    return weighted_strings

def extract_semantic_weight(word, weighted_strings,keywords):
    ## since we have 4 weight groups + 1 for rest of html = 5 possible weights.
    max = len(weighted_strings)
    weight = 1 ## plain text
    for i in range(max):
        ## the leftmost match will ensure highest weight is returned and not overriden by lower one.
        if word in weighted_strings[i]: 
            return max - i + weight
    if None != keywords and word in keywords:
        return max + weight
    return weight
    
def get_html_tokens(doc):
    raw_html = open(base_path + "/" + doc, encoding='utf-8')
    parsed_html = BeautifulSoup(raw_html, 'html.parser')
    [s.extract() for s in parsed_html(['script', 'style','link'])]
    keywords = extract_keywords(parsed_html)
    text = " ".join(parsed_html.strings)
    if keywords != None: text = text + keywords
    tkns = reg_tokenizer.tokenize(text)
    return (parsed_html, tkns)

def extract_keywords(parsed_html):
    for tag in parsed_html.find_all("meta"):
        if tag.get("name", None) == 'keywords':
            return(tag.get("content", None))
        
def parse_document(doc):
    print('starting ' + doc)
    parsed_html,tkns = get_html_tokens(doc)
    weighted_strings = extract_weighted_strings(parsed_html)
    inv_idx = {}
    keywords = extract_keywords(parsed_html)
    
    for k in range(len(tkns)):
        if tkns[k] not in stop_words and len(tkns[k]) > 2:
            word = wordnet_lemmatizer.lemmatize(tkns[k].lower()) 
            weight = extract_semantic_weight(word, weighted_strings,keywords)
            if word in inv_idx:
                posting = inv_idx[word][doc]
                if posting[0] < weight:
                    posting[0] = weight
                posting[1] = posting[1] + 1
                inv_idx[word][doc] = posting
            else:
                inv_idx[word] = {doc: [weight,1]}
                
    ##To push message to RQ, so that DB writes can be processed in parallel. But RQ doesn't seem to work on windows. Hence removing it for now.
    ##q.enqueue(process_queue,inv_idx)  
    process_queue(inv_idx,doc,get_out_urls_as_doc(parsed_html)) ## need to calculate tfidf there.
    print('Successfully parsed:' , doc)

def process_queue(idx,doc,urls):
    conn = setup_db_connection()
    number_of_words_in_doc = len(idx)
    for key in idx:
        bucket = conn['inverted_idx'+ str(getBucket(key[0]))]
        obj = bucket.find_one({key: {"$exists": True}})
        term_freq = idx[key][doc][1] / number_of_words_in_doc
        idx[key][doc][1] = term_freq
        if None != obj:
            final_postings = {**obj[key], **idx[key]}
            bucket.update_one({"_id": obj["_id"]}, {"$set": {key: final_postings}})
        else:
            bucket.insert_one({key: idx[key]}) 
    lconn = conn.page_rank
    
    for i in range(len(urls)):
        link = urljoin(files_map[doc], urls[i]['href']) ##convert to absolute url
        if link in inv_map:
            lkey = inv_map[link]
            lrank = lconn.find_one({lkey:{"$exists":True}})
            if None != lrank:
                lconn.update_one({"_id": lrank["_id"]}, {"$set": {lkey: lrank[lkey] + 1}})
            else:
                 lconn.insert_one({lkey: 1}) 
        
    print('Write success at: ', time.time())

def setup_db_connection(): ## Each thread is a transaction.
    con = MongoClient(dburl)
    return con.mydb
    
def getBucket(s):
    i = ord(s)
    if i >= 122: return 1
    elif i >= 117: return 2
    elif i >= 110: return 3
    elif i >= 102: return 4
    elif i >= 97: return 5
    else: return 1
    
if __name__ == '__main__':
    ##global q = Queue(connection=Redis())  
    with concurrent.futures.ProcessPoolExecutor() as executor:
        # List of files to process
        files_list = files_map.keys()
        print('starting process at' ,time.time())
        # Process the list of files, but split the work across the process pool to use all CPUs!
        deque(executor.map(parse_document, files_list))
