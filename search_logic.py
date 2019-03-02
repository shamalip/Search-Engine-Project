import nltk
from nltk.stem import WordNetLemmatizer
from pymongo import MongoClient
import lxml.html
import configparser
import numpy as np
from math import log10
from sklearn.metrics.pairwise import cosine_similarity

config = configparser.RawConfigParser()
config.read('config.properties')
dburl = config.get('DB properties', 'DBCONNECTION')
MAX_RESULTS_TO_SHOW = int(config.get('Others','MAX_RESULTS'))
nltk.download('wordnet')
wordnet_lemmatizer = WordNetLemmatizer()
from bookkeeping import files_map,inv_map
TOTAL_DOCS = len(files_map)

'''
Calls appropriate function for single word search and mutiple word searches.
'''
def get_search_results(query,terms):
	if len(terms) == 1:
		return perform_single_search(terms[0])
	return get_merged_results(query,terms)


'''
Returns results sorted by importance - i.e. presence of term in title, heading etc.
And also sorts them based on term frequency.
Note - In single word search, IDF doesn't provide any information hence is not used.
'''
def perform_single_search(term):
	idx = fetch_index(term)
	''' Example - {"literature" : {'docId':[ semantic_weight, term frequency]}}
		Here we will sort the lists just by the semantic_weight 
		and to break the ties, we could use tf. 
	''' 
	lst =  sorted(idx, key=lambda k: (idx[k][0], idx[k][1]), reverse=True)
	return get_docs_from_idx(lst)

'''
Restricts the number of results to configured value and gets formatted result from get_docs_as_result
'''
def get_docs_from_idx(lst):
	docs = {}
	for k in range(len(lst)):
		if(len(docs) >= MAX_RESULTS_TO_SHOW):
			break
		item = lst[k]
		docs[item] = get_docs_as_result(item)
	return docs

'''
Gets the url, headline to display against each search result.
'''
def get_docs_as_result(docid):
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

'''
Called in case of multiterm search
1. Get the tf-idf of the documents and computes cosine similarity with query vector and sorts document as per it.
2. Tie between similar cosine ranked documents is resolved by considering weightage on tags like title, heading etc.
'''
def get_merged_results(query,terms):
	p_dict, doc_list = get_docs_and_intersection(terms)
    ##compute tf-idf vectors for each documet and query.
	query_vector = []
	for i in range(len(terms)):
		query_vector.append(query.count(terms[i]))
	for key in doc_list:
		doc_vector = []
		for i in range(len(terms)):
			tf = p_dict[terms[i]][key][1]
			idf = 1.0 + log10(float(TOTAL_DOCS / len(p_dict[terms[i]])))
			doc_vector.append(tf * idf)
		x = np.array(query_vector)
		y = np.array(doc_vector)
		doc_list[key][1] = cosine_similarity(x.reshape(1,len(x)), x.reshape(1,len(y)))
	## sort documents by cosine similarity.
	doc_results =  sorted(doc_list, key=lambda k: (doc_list[key][1], doc_list[key][0]), reverse=True)
	## finally call get object in format needed by html and return
	return get_docs_from_idx(doc_results)

'''
Gets the document list of documents containing all the terms.
'''
def get_docs_and_intersection(terms):
	p_dict = {}
	doc_list = None
	for i in range(len(terms)):
		postings = fetch_index(terms[i])
		p_dict[terms[i]] = postings ## example format {'docId':[semantic_weight term_frequency]}
		if(None == doc_list):
			doc_list = postings
		else:
			for doc_id in doc_list.copy(): ## find intersection of documents.
				if doc_id not in postings:
					del doc_list[doc_id]
	return (p_dict,doc_list)

'''
Gets the postings for given term by querying mongoDB
'''
def fetch_index(term):
	lemma = wordnet_lemmatizer.lemmatize(term.lower())
	con = MongoClient(dburl)
	bucket = con.mydb['inverted_idx'+str(get_bucket(lemma[0]))]
	response = bucket.find_one({lemma: {"$exists": True}})
	if None!= response:
		return response[lemma]
	return []

'''
Get the index bucket to search the term in mongoDB.
'''
def get_bucket(s):
    i = ord(s)
    if i >= 122: return 1
    elif i >= 117: return 2
    elif i >= 110: return 3
    elif i >= 102: return 4
    elif i >= 97: return 5
    else: return 1