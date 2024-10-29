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

file_type_to_language = {
        ".cpp": "cpp",
        ".hpp" : "cpp",
        ".go": "go",
        ".sql" : "text",
        ".java": "java",
        ".kt": "kotlin",
        ".ts": "ts",
        ".php": "php",
        ".py": "python",
        ".rst": "rst",
        ".sh" : "text", #check
        ".rb": "ruby",
        ".xml" : "text",
        ".proto" : "text", #check
        ".rs": "rust",
        ".scala": "scala",
        ".yaml" : "yaml",
        ".swift": "swift",
        ".md": "markdown",
        ".tex": "latex",
        ".sol": "sol",
        ".cob": "cobol",
        ".c": "c",
        ".lua": "lua",
        ".pl": "perl",
        ".cs" : "c-sharp",
        ".hs": "haskell",
        ".html" : "text",
        ".orig" : "text",
        ".txt" : "text",
        ".mk" : "text",
        ".res" : "text",
        ".conf" : "text",
        ".data" : "text",
    }

def ingest():    
   s3 = boto3.client('s3')
   Settings.embed_model = HuggingFaceEmbedding()
   Settings.llm = None  
   reader = SimpleDirectoryReader(input_dir = "/home/deeepakb/redshift-padb", recursive = True)
   documents = reader.load_data()
   nodes = []
   total_docs = len(documents)
   processed_docs = 0

   for doc in documents:
      ___ , file_type = os.path.splitext(doc.metadata.get("file_path"))
      file_type = file_type_to_language.get(file_type.lower(), None)
      print(doc.metadata.get("file_path"))
      
      if file_type is None:
         print(f"Skipping {doc.metadata.get('file_path')}")
         continue
      
      if file_type == "text":
         text_splitter = SentenceSplitter(chunk_size=16384, chunk_overlap=0)
         nodes.extend(text_splitter.get_nodes_from_documents([doc]))
         processed_docs+=1
         print(f"Processed {processed_docs} of {total_docs} documents")
      else:
        code_splitter = CodeSplitter(language = file_type , chunk_lines= 16384)
        nodes.extend(code_splitter.get_nodes_from_documents([doc]))
        processed_docs+=1
        print(f"Processed {processed_docs} of {total_docs} documents")


  
  # Index Store
   print(f"{processed_docs} out of {total_docs}")
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