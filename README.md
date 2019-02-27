# UCI Domain search (Development in progress)

A search functionality on the crawled data in from the ics.uci.edu domain.

indexer.py - generates and stores Inverted Index in mongoDB in a multi threaded manner.
Dependencies - 
1. pyMongo,
2. nltk for tokenization,
3. BeatifulSoup for parsing html.

bookkeeping.py - reads the json that holds document to url mapping of crawled data. It stores the json as dictionary and also makes the inverse mapping available for use. 

