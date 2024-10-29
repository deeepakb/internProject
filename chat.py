import boto3
import json
import os
import logging
import threading
import time
from flask import Flask, request, render_template, jsonify
from llama_index.core import Settings
from llama_index.core import StorageContext, load_index_from_storage
from llama_index.embeddings.huggingface import HuggingFaceEmbedding
import fnmatch
import concurrent.futures
from llama_index.core.retrievers import VectorIndexRetriever
import argparse
from diff import compare_folders
from diffnew import diff_command
from collections import deque


conversation_history = deque(maxlen=20)  

def chat(input_text):
  conversation_history.clear()
  Settings.embed_model = HuggingFaceEmbedding()
  Settings.llm = None
  storage_context = StorageContext.from_defaults(persist_dir="./fromS3")
  index = load_index_from_storage(storage_context)

  retriever = VectorIndexRetriever(index=index, retrieval_mode="default", 
    maximal_marginal_relevance=True,
    include_context=True,
)
  print("Finished loading index from storage")

  response = retriever.retrieve(str_or_query_bundle = input_text)
  conversation_history.append(input_text)
  prompt = "\n".join(conversation_history)

  temp = ""
  path = response[0].metadata.get('file_path') 

  with open(path, 'r') as file:
     content = file.read()
     temp += content

  prompt_info = "Don't add unnecesary info, just give me the answer, no need for formalities"

  kwargs = {
  "modelId": "anthropic.claude-3-5-sonnet-20241022-v2:0",
  "contentType": "application/json",
  "accept": "application/json",
  "body": json.dumps({
    "anthropic_version": "bedrock-2023-05-31",
    "max_tokens": 500,
    "top_k": 10,
    "stop_sequences": [],
    "temperature": 0.1,
    "top_p": 0.95,
    "messages": [
      {
        "role": "user",
        "content": [
          {
            "type": "text",
            "text": prompt + " here is some relevant code" + temp + "heres some prompt info" + prompt_info
          }
        ]
      }
    ]
  })
}

  bedrock = boto3.client('bedrock-runtime')
  response2 = bedrock.invoke_model(
     **kwargs
  )
   
  print("\n\n\n Response: " + json.loads(response2['body'].read())['content'][0]['text'])
  
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CLI for LLM")
    parser.add_argument("-input", nargs='+', help="Input prompt for LLM")
    parser.add_argument("-diff", action="store_true", help = "returns the diff")
    
    args = parser.parse_args()
    if args.diff:
       diff_command()
    else:
       input_text = ' '.join(args.input)
       chat(input_text)