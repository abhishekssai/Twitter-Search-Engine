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
        topHit['lucene_score'] = hit.score
        topkdocs.append(topHit)
    return topkdocs

lucene.initVM(vmargs=['-Djava.awt.headless=true'])

index_dir = 'group7_lucene_index'
def search_lucene(query):
    if query[0] == '#':
        return retrieve(index_dir, 'hashtags', query[1:])
    else:
        return retrieve(index_dir, 'text', query)
    print("\n\n")
