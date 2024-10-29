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
import multiprocessing

import concurrent.futures
import os
from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor
from collections import deque

#Not supported -> sql, html, FFFFFFFF files break it
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
        ".csv" : "text",
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
        ".component" : "text",
    }

def process_single_document(doc):
    try:
        file_name, file_type = os.path.splitext(doc.metadata.get("file_path"))
        print(f" File: {doc.metadata.get('file_path')}")
        file_type = file_type_to_language.get(file_type.lower(), None)
        if file_type is None:
            print(f"Uknwown file {file_name}.{file_type}")
            return []
        
        if file_type == "text":
            print(f"{file_name}.{file_type}")
            text_splitter = SentenceSplitter(chunk_size=8192, chunk_overlap=0)
            return text_splitter.get_nodes_from_documents([doc])
        else:   
            print(f"{file_name}.{file_type}")
            code_splitter = CodeSplitter(language=file_type, chunk_lines=8192)
            return code_splitter.get_nodes_from_documents([doc])
    except Exception as e:
        print(f"Error processing file: {doc.metadata.get('file_path')}")
        print(e)
        return []


def ingest():  
   multiprocessing.set_start_method('spawn')  
   s3 = boto3.client('s3')
   Settings.embed_model = HuggingFaceEmbedding()
   Settings.llm = None  
   reader = SimpleDirectoryReader(input_dir = "/home/deeepakb/redshift-padb", recursive = True)
   documents = reader.load_data()
   max_docs = 10000
   if len(documents) > max_docs:
    documents = documents[:max_docs]

   nodes = []
   total_docs = len(documents)
   processed_docs = 0
   skip_count = 0
   not_skip_count = 0

   with concurrent.futures.ProcessPoolExecutor(max_workers=os.cpu_count() // 4) as executor:
        # Submit all documents for processing
        future_to_doc = {executor.submit(process_single_document, doc): doc for doc in documents}
        
        # Process completed futures as they finish
        for future in concurrent.futures.as_completed(future_to_doc):
            doc = future_to_doc[future]
            print(f" File: {doc.metadata.get('file_path')}")
            try:
                result = future.result()
                nodes.extend(result)
                processed_docs += 1
                if result:
                    not_skip_count += 1
                else:
                    print(f"skipped: {doc.metadata.get('file_path')}")
                    skip_count += 1
            except Exception as e:
                print(f"Error processing file: {doc.metadata.get('file_path')}")
                print(e)
                skip_count += 1
            
            # Print progress
            print(f"Processed {processed_docs} of {total_docs} documents")

   print(f"Finished with skipped: {skip_count}, and not skipped: {not_skip_count}")


  # Index Store Faiss/Pinecone
   nodes = list(nodes)
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