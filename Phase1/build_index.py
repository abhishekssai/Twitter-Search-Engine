import logging, sys, threading
logging.disable(sys.maxsize)

import argparse
import lucene
import os
import jsonlines
import time
from dateutil import parser
from org.apache.lucene.store import MMapDirectory, SimpleFSDirectory, NIOFSDirectory
from java.nio.file import Paths
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.document import Document, Field, DoublePoint, FieldType
from org.apache.lucene.queryparser.classic import QueryParser
from org.apache.lucene.index import FieldInfo, IndexWriter, IndexWriterConfig, IndexOptions, DirectoryReader, Term
from org.apache.lucene.search import IndexSearcher, BoostQuery, Query
from org.apache.lucene.search.similarities import BM25Similarity

# UI indicator to show the script running status
class Ticker(object):

    def __init__(self):
        self.tick = True

    def run(self):
        while self.tick:
            sys.stdout.write('.')
            sys.stdout.flush()
            time.sleep(4.0)

def get_epoch_from_timestr(timestr):
    return parser.parse(timestr).timestamp()

def create_index(datafile, dir):
    if not os.path.exists(datafile):
        sys.exit('Data file not found')
    if not os.path.exists(dir):
        os.mkdir(dir)
    store = SimpleFSDirectory(Paths.get(dir))
    analyzer = StandardAnalyzer()
    config = IndexWriterConfig(analyzer)
    config.setOpenMode(IndexWriterConfig.OpenMode.CREATE)
    writer = IndexWriter(store, config)

    ################################################################################################################

    #### Since twitter inherently limits the size of a tweet, we have decided to store all fields for now.

    ################################################################################################################

    # Stored, un-tokenized, un-indexed - such as profile Image URLs
    meta_type = FieldType()
    meta_type.setStored(True)
    meta_type.setTokenized(False)
    meta_type.setIndexOptions(IndexOptions.NONE)

    # Stored, un-tokenized, indexed - such as ids, timestamp, username
    string_type = FieldType()
    string_type.setStored(True)
    string_type.setTokenized(False)

    # Stored, tokenized, indexed - Display Name, Tweet text
    text_type = FieldType()
    text_type.setStored(True)
    text_type.setTokenized(True)
    text_type.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS)

    counter = 0
    ticker = Ticker()
    threading.Thread(target=ticker.run).start()
    with jsonlines.open(datafile) as inp:
        for line in inp.iter():

            # Extract data from each json object in .jsonl input file
            counter += 1
            author_id = line['author_id']
            tweet_id = line['id']
            geo = line['geo']
            text = line['text']
            hashtags = line['hashtags']
            timestamp = line['created_at']
            display_name = line['user_data']['name']
            username = line['user_data']['username']
            verified = line['user_data']['verified']
            profile_image_url = line['user_data']['profile_image_url']

            doc = Document()

            # Handling duplicate tweets
            term_key = text + str(verified)
            doc.add(Field('term_key', term_key, meta_type))
            term = Term('term_key')

            # All fields with their corresponding processing criteria
            doc.add(Field('text', text, text_type))
            doc.add(Field('hashtags', ' '.join(hashtags), text_type))
            doc.add(Field('tags', str(hashtags), meta_type))
            doc.add(Field('author_id', author_id, meta_type))
            doc.add(Field('tweet_id' , tweet_id, meta_type))
            doc.add(Field('geo', str(geo), meta_type))
            doc.add(Field('timestamp', timestamp, string_type))
            doc.add(DoublePoint('ts_epoch', get_epoch_from_timestr(timestamp)))
            doc.add(Field('display_name', display_name, text_type))
            doc.add(Field('username', username, string_type))
            doc.add(Field('verified', str(verified), string_type))
            doc.add(Field('profile_image_url', profile_image_url, meta_type))

            # using .updateDocument() instead of .add() to induce idempotency to handle duplicate tweets
            writer.updateDocument(term, doc)

        # Closing the writer object after completing indexing 
        writer.close()
        ticker.tick = False

    print("done!")
    print("Indexing completed on ", counter, " documents succesfully!")


# Enabled passing arguments to the script via argparse
arg_parser = argparse.ArgumentParser("Build index on raw twitter data")
arg_parser.add_argument('-D', '--datafile', type=str, default='data.jsonl', help='Path to the input raw data file')
arg_parser.add_argument('-O', '--outputdir', type=str, default='group7_lucene_index', help='Path to the directory to store the index')
args = arg_parser.parse_args()

lucene.initVM(vmargs=['-Djava.awt.headless=true'])

# calculating CPU run time
st = time.time()
create_index(args.datafile, args.outputdir)
et = time.time()
print("Elpased ", et - st, " seconds")
