import logging, sys
logging.disable(sys.maxsize)

import lucene
import argparse
from org.apache.lucene.store import NIOFSDirectory
from java.nio.file import Paths
from org.apache.lucene.search import IndexSearcher
from org.apache.lucene.index import DirectoryReader
from org.apache.lucene.queryparser.classic import QueryParser
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.analysis.core import KeywordAnalyzer

def retrieve(storedir, field, query):
    searchDir = NIOFSDirectory(Paths.get(storedir))
    searcher = IndexSearcher(DirectoryReader.open(searchDir))

    # Build QueryParser object to parse query
    parser = QueryParser(field, StandardAnalyzer())
    parsed_query = parser.parse(query)

    # Retrieve top docs based on query
    topkdocs = []

    # Retrieving top 10 results for a query
    topDocs = searcher.search(parsed_query, 10).scoreDocs
    for hit in topDocs:
        doc = searcher.doc(hit.doc)

        # Populate output results with necessary fields
        topHit = {field.name() : field.stringValue() for field in doc.iterator() if field.name() not in ['term_key', 'hashtags']}
        topkdocs.append(topHit)
    for topDoc in topkdocs:
        print(topDoc)

#Enabled command line options with argparse
args_parser =  argparse.ArgumentParser(description='Search a specific index by tweet text or hashtags.')
args_parser.add_argument('-D', '--indexdir', default='group7_lucene_index', help='Directory of the index store')
args = args_parser.parse_args()
index_dir = args.indexdir


lucene.initVM(vmargs=['-Djava.awt.headless=true'])
while (True):
    query = input("Provide you query : ")
    if query[0] == '#':
        retrieve(index_dir, 'hashtags', query[1:])
    else:
        retrieve(index_dir, 'text', query)
    print("\n\n")
    cont_res = input("Do you want to continue querying? ([Y]es/[N]o)")
    if cont_res == 'Yes' or cont_res == 'Y' or cont_res == 'y' or cont_res == 'yes':
        continue
    else:
        break
