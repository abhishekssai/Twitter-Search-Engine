import numpy as np
import torch
import os
import pandas as pd
import faiss
import time
from sentence_transformers import SentenceTransformer

def search_bert(query, k):
    df = pd.read_json('filtered.jsonl', lines=True)
    model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')
    index = faiss.read_index('faiss_index')
    t=time.time()
    query_vector = model.encode([query])
    D, I = index.search(query_vector, k)
    print('totaltime: {}'.format(time.time()-t))
    return [df[df.id == _id].to_json(orient="records", lines=True) for _id in I.tolist()[0]]
