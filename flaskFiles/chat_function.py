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

def normalize_path(path):
    return os.path.normpath(os.path.expanduser(path))

def safe_int(value):
    try:
        return int(value)
    except ValueError:
        return float('inf')  


def chat_function(query, conversation_history):
    embeddings = HuggingFaceEmbeddings(model_name="sentence-transformers/all-mpnet-base-v2")
    index_path = "/home/deeepakb/Projects/bedrockTest/faiss_index_final"

    try:
        index_start_time = time.time()
        vector_store = FAISS.load_local(index_path, embeddings, allow_dangerous_deserialization=True)
        logger.info(f"FAISS index loaded successfully, it took {time.time() - index_start_time} Seconds")
    except Exception as e:
        logger.error(f"Error loading FAISS index: {e}")
        return "Sorry, I encountered an error while loading the index."

    search_start_time = time.time()
    results = vector_store.max_marginal_relevance_search(query, k=25, fetch_k=50)

    if not results:
        logger.info("No results found for the query.")
        return "I couldn't find any relevant information for your query."

    logger.info(f"\nTime to find Chunks is {time.time() - search_start_time} \n")
    file_groups = defaultdict(list)
    for chunk in results:
        file_path = chunk.metadata.get('file_path', '')
        file_name = os.path.basename(file_path)
        file_groups[file_name].append(chunk)

# Sort and deduplicate chunks within each file group
    sorted_deduplicated_chunks = []
    for file_name, chunks in file_groups.items():
    # Sort chunks within the file
        sorted_file_chunks = sorted(chunks, key=lambda x: safe_int(x.metadata.get('ids', '')))
    
    # Remove duplicates
        deduplicated_file_chunks = []
        seen_content = set()
        for chunk in sorted_file_chunks:
            chunk_content = chunk.page_content.strip()
            if chunk_content not in seen_content:
                deduplicated_file_chunks.append(chunk)
                seen_content.add(chunk_content)
    
        sorted_deduplicated_chunks.extend(deduplicated_file_chunks)

# Reconstruct full content
    full_content = ""
    for chunk in sorted_deduplicated_chunks:
        chunk_content = chunk.page_content.strip()
        full_content += f"File: {os.path.basename(chunk.metadata.get('file_path', 'Unknown'))}\n"
        full_content += chunk_content + "\n\n"

# Remove excessive newlines and strip
    full_content = re.sub(r'\n{3,}', '\n\n', full_content)
    full_content = full_content.strip()

# Write to file
    with open("reconstructed_document_FAISS.txt", "w") as f:
        f.write(full_content)



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
            "messages": conversation_history + [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": query + "\n\n here is some relevant code snippets" + full_content
                        }
                    ]
                }
            ]
        })
    }

    bedrock = boto3.client('bedrock-runtime')
    bedrock_response_time = time.time()
    response2 = bedrock.invoke_model(**kwargs)
    logger.info(f"\nIt took {time.time() - bedrock_response_time } seconds for the model to respond")
    
    response_content = json.loads(response2['body'].read())['content'][0]['text']
    return response_content


    