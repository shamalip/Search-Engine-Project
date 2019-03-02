import nltk
from nltk.stem import WordNetLemmatizer
from pymongo import MongoClient
import lxml.html
import configparser

config = configparser.RawConfigParser()
config.read('config.properties')
dburl = config.get('DB properties', 'DBCONNECTION')

nltk.download('wordnet')
wordnet_lemmatizer = WordNetLemmatizer()
from bookkeeping import files_map,inv_map

def getMergedResults(terms):
	docs = {}
	for i in range(len(terms)):
		lemma = wordnet_lemmatizer.lemmatize(terms[i])
		con = MongoClient(dburl)
		bucket = con.mydb['inverted_idx'+str(getBucket(lemma[0]))]
		obj = bucket.find_one({lemma: {"$exists": True}})
		if None!=obj:
			response = obj[lemma];
			for k in response:
				if(len(docs) >= 20):
					break
				docs[k] = get_docs_as_result(k,response[k])
	return docs


def get_docs_as_result(docid, postings):
    raw_html = open("WEBPAGES_RAW" + "/" + docid, encoding='utf-8')
    t = lxml.html.parse(raw_html)
    url = files_map[docid]
    search_result = {'url':url ,'title': "Search Result"}
    title = t.find(".//title")
    if None != title:
    	search_result['title'] = title.text
    else:
    	title = t.find("h1")
    	if None != title:
    		search_result['title'] = title
    return search_result

def getBucket(s):
    i = ord(s)
    if i >= 122: return 1
    elif i >= 117: return 2
    elif i >= 110: return 3
    elif i >= 102: return 4
    elif i >= 97: return 5
    else: return 1