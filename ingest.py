import boto3
import json
import os
import logging
import threading
import time
from llama_index.core import SimpleDirectoryReader
from llama_index.core import VectorStoreIndex
from llama_index.core import Settings
from llama_index.core import StorageContext, load_index_from_storage
from llama_index.embeddings.huggingface import HuggingFaceEmbedding
from llama_index.core.node_parser import CodeSplitter, SentenceSplitter
import fnmatch
import concurrent.futures

def ingest():    
   s3 = boto3.client('s3')
   Settings.embed_model = HuggingFaceEmbedding()
   Settings.llm = None  
   reader = SimpleDirectoryReader(input_dir = "./cpp_Files", recursive = True)
   code_splitter = CodeSplitter(language = "cpp" , chunk_lines= 2048)
   documents = reader.load_data()
   nodes = code_splitter.get_nodes_from_documents(documents)
  
  # Index Store
   index = VectorStoreIndex(nodes, show_progress=True)
   index.storage_context.persist(persist_dir="./indexStorage")
   storage_context = StorageContext.from_defaults(persist_dir="./indexStorage")

   for root, dirs, files in os.walk('./indexStorage'):
     for file in files:
        s3.upload_file(os.path.join(root, file), Bucket='holdingvectorstore', Key=os.path.relpath(os.path.join(root, file), './indexStorage'))


   objects = s3.list_objects_v2(Bucket = 'holdingvectorstore')
   for obj in objects['Contents']:
     s3_key = obj['Key']
     s3.download_file('holdingvectorstore', s3_key, f'./fromS3/{os.path.basename(s3_key)}')
   print("finished Download")   

if __name__ == "__main__":
   ingest()