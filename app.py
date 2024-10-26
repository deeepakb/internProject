import boto3
import json
import os
import logging
import threading
import time
from flask import Flask, request, render_template, jsonify
from llama_index.core import SimpleDirectoryReader
from llama_index.core import VectorStoreIndex
from llama_index.core import Settings
from llama_index.core import StorageContext, load_index_from_storage
from llama_index.embeddings.huggingface import HuggingFaceEmbedding
from llama_index.core.node_parser import CodeSplitter, SentenceSplitter
from watchdog.observers import Observer
from watchdog.events import LoggingEventHandler
import fnmatch
import concurrent.futures
from llama_index.core import Document
from llama_index.embeddings.openai import OpenAIEmbedding
from llama_index.core.node_parser import SentenceSplitter
from llama_index.core.extractors import TitleExtractor
from llama_index.core.ingestion import IngestionPipeline


app = Flask(__name__)

language_mapping = {
   "py" : "Python File",
   "rb" : "Ruby File",
   "sql": "SQL File",
   "txt": "Text File",
   "cpp": "C++ File",
   "hpp": "Header File",
   "json": "JSON File",
   "avro": "Apache Avro File",
   "ion": "Amazon Ion File",
   "mk": "MakeFile File",
   "conf" : "Config File",
   "res" : "Resource File",
   "yaml" : "YAML File",
   "csv" : "CSV File",
   "trans" : "Tranformation File",
   "pem" : "Privacy Enhanced Mail File",
   "tbl" : "Vector Table Output File",
   "jks" : "Java KeyStore File",
   "sav" : "Storage Media File",
}

def file_metadata(x): 
   try:
      return {
     "file_name": os.path.basename(x),
     "file_type" : language_mapping[os.path.splitext(x)[1][1:]],
                             }
   except:
      return {
     "file_name": os.path.basename(x),
     "file_type" : "Unknown",
                             }
# Check for file change -> call AI to check changes
def check_for_change():
  logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
  path = "./dummyCode"
  event_handler = LoggingEventHandler()
  observer = Observer()
  observer.schedule(event_handler, path, recursive=True)
  observer.start()
  try:
    while True:
      time.sleep(1)
  except KeyboardInterrupt:
      observer.stop()
  observer.join()


@app.route('/')
def index():
   return render_template('index.html')

def split_document(document, code_splitter):
   return code_splitter.get_nodes_from_documents([document])

def ingest():
   # Llama Index
  Settings.embed_model = HuggingFaceEmbedding()
  
  
  reader = SimpleDirectoryReader(input_dir = "./dummyCode", file_metadata=file_metadata, recursive = True)
  code_splitter = CodeSplitter(language = "python" , chunk_lines=2048)
  documents = reader.load_data()

  print("Text splitting started!")
  total_documents = len(documents)
  with concurrent.futures.ThreadPoolExecutor() as executor:
     future_to_doc = {executor.submit(split_document, doc, code_splitter): doc for doc in documents if doc.metadata.get("language" =='python')}

     for i, future in enumerate(concurrent.futures.as_completed(future_to_doc), start  = 1):
        nodes = future.result()
        logging.info(f"Processed document {i}/{total_documents}")
     results = list(executor.map(lambda doc: split_document(doc, code_splitter), documents))
  nodes = [node for sublist in results for node in sublist]
  
  #nodes =  text_splitter.get_nodes_from_documents(documents, show_progress= True)
  print("text splitting")

  # Index Store
  index = VectorStoreIndex(nodes, show_progress=True)
  index.storage_context.persist(persist_dir="./indexStorage")
  storage_context = StorageContext.from_defaults(persist_dir="./indexStorage")

  # S3
  s3 = boto3.client('s3')
  print("s3 connected")
  s3.upload_file(Body= index, Bucket='deeepakb-bedrock-test1', Key="vector_store.json") 
  
  #Ingest
  agent = boto3.client('bedrock-agent')
  print("started ingestion")
  agent.start_ingestion_job(dataSourceId= 'CBWPTRESYV', knowledgeBaseId = 'DVY2XJUT2X')
  return f"Ingestion and indexing complete. Files uploaded to S3"
   
  
conversation_context = []

@app.route('/chat', methods = ['POST'])
def chat():
  user_query = request.form['query']
  
  conversation_context.append({"role" : "user", "content" : user_query})
  input_text = "\n".join([f"{entry['role']}: {entry['content']}" for entry in conversation_context])

  agent_runtime = boto3.client('bedrock-agent-runtime')     
  response = agent_runtime.retrieve_and_generate(
          input = {
             'text' : input_text
          },
          retrieveAndGenerateConfiguration = {
            'knowledgeBaseConfiguration': {
                'knowledgeBaseId': "DVY2XJUT2X",
                'modelArn': "arn:aws:bedrock:us-west-2::foundation-model/anthropic.claude-3-5-sonnet-20240620-v1:0",
            },
            'type': 'KNOWLEDGE_BASE'
        }
    )
  
  model_answer = response['output']['text']
  conversation_context.append({"role" : "assistant", "content" : model_answer})
  return jsonify({"response" : model_answer})

if __name__ == "__main__":
    background_thread = threading.Thread(target = check_for_change)
    background_thread.daemon = True
    background_thread.start()
    ingest()
    app.run(debug = False)