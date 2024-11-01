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


def chat(input_text):
    start_time = time.time()
    Settings.embed_model = HuggingFaceEmbedding()
    Settings.llm = None
    storage_context_time = time.time()
    storage_context = StorageContext.from_defaults(persist_dir="./indexStorage")
    print("Got storage context")
    index = load_index_from_storage(storage_context, mmap=True)

    print("Finished Loading Vector Store")

    retriever = VectorIndexRetriever(index=index, retrieval_mode="default", 
        maximal_marginal_relevance=True,
        include_context=True,
        similarity_top_k = 20,
        verbose = True,
    )

    print("Finished making Retriever")

    response = retriever.retrieve(str_or_query_bundle = input_text)
    
    code_snippets = ""
    i = 0
    for node in response:
        i+=1
        code_snippets += f"\n\n Snippet {i} : {node.text}"
        print(node)

    with open("/home/deeepakb/Projects/bedrockTest/reconstructed_document.txt", "w") as f:
        f.write(code_snippets)

    prompt_info = "Don't add unnecessary info, just give me the answer, no need for formalities"

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
                            "text": input_text + "\n\n here is some relevant code snippets" + code_snippets + "\n\nheres some prompt info" + prompt_info
                        }
                    ]
                }
            ]
        })
    }
    end_time = time.time()  # End timing
    elapsed_time = end_time - start_time
    print(f"\nTotal processing time: {elapsed_time:.2f} seconds")

    """
    bedrock = boto3.client('bedrock-runtime')
    response2 = bedrock.invoke_model(**kwargs)
   
    print("\n\n\n Response: " + json.loads(response2['body'].read())['content'][0]['text'])
    end_time = time.time()  # End timing
    elapsed_time = end_time - start_time
    print(f"\nTotal processing time: {elapsed_time:.2f} seconds")"""

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
