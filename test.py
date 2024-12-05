import faiss
import os
from llama_index.core import GPTTreeIndex, StorageContext, load_index_from_storage
from llama_index.embeddings.huggingface import HuggingFaceEmbedding
from llama_index.core.indices.tree.select_leaf_embedding_retriever import TreeSelectLeafEmbeddingRetriever, TreeSelectLeafRetriever
import logging
import json
import boto3
import time
from collections import OrderedDict
from llama_index.core import Settings
import sys
import os
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_community.vectorstores import FAISS
from langchain.schema import Document
import logging
import re
from collections import defaultdict
import json
import boto3
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()


def lmao():

    #/home/deeepakb/redshift-padb/test/stability/stability.sql

    # Load the FAISS index
    index_path = "/home/deeepakb/Projects/bedrockTest/faiss_index_embeddings"
    embeddings = HuggingFaceEmbeddings(model_name="jinaai/jina-embeddings-v2-base-code",
                                       model_kwargs={'device': "cpu"},
                                       encode_kwargs={"batch_size": 131072})    
    vector_store = FAISS.load_local(index_path, embeddings, allow_dangerous_deserialization=True)

    all_docs = vector_store.index_to_docstore_id

    # If you need the actual Document objects:
    all_nodes = [doc for doc in vector_store.docstore._dict.values() if doc.metadata.get("file_path") == "/home/deeepakb/redshift-padb/test/stability/stability.sql"]
    print(all_nodes)
    # Access raw embeddings

if __name__ == "__main__":
    lmao()
