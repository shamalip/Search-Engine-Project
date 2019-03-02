# UCI Domain search (Development in progress)

A search functionality on the crawled data in from the ics.uci.edu domain.

indexer.py - generates and stores Inverted Index in mongoDB in a multi-process setting.
Dependencies - 
1. pyMongo,
2. nltk for tokenization,
3. BeautifulSoup for parsing html.

bookkeeping.py - reads the json that holds document to url mapping of crawled data. It stores the json as dictionary and also makes the inverse mapping available for use. 

config.properties
Holds configurations like DB connection.

UI - using Flask
search_app.py- Consists of the rest apis

search_logic.py - performs search and returns the urls

TODO:
- [ ] Store position information for multiple word searches
- [x] Cosine similarity -  tf-idf.
- [ ] Show partial text in search results showing the place where text appears.
- [x] Utilize the stored weights (per presence in title, headings etc.)
- [ ] Check for index compression
- [ ] Show a "show more" link if number of results from a domain are more than configurated limit. (Ensure diversity in results)
