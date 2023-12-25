import numpy as np
import torch
import os
import pandas as pd
import faiss
import time
from sentence_transformers import SentenceTransformer

chunk_size = 5000
df = pd.read_json('filtered.jsonl', lines=True, chunksize=chunk_size)

model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')

index = faiss.IndexIDMap(faiss.IndexFlatIP(model.get_sentence_embedding_dimension()))

i = 1
for chunk in df:
    print("Processing.. ", i * chunk_size, " tweets")
    documents = chunk.text.to_list()
    start = time.time()
    encoded_data = model.encode(documents)
    index.add_with_ids(encoded_data, chunk.id.values)
    end = time.time()
    print("Elapsed - ", (end - start) / 1000)
    i += 1

faiss.write_index(index, 'faiss_index')

