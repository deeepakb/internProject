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

def load_conversation_history():
    try:
        with open("conversation_history.json", "r") as f:
            return json.load(f)
    except:
        return []

def save_conversation_history(history):
    with open("conversation_history.json", "w") as f:
        json.dump(history, f, indent=2)

def chat():
    conversation_history = load_conversation_history()

    embeddings = HuggingFaceEmbeddings(model_name="sentence-transformers/all-mpnet-base-v2", model_kwargs={'device': "cpu"},
                                                 encode_kwargs={"batch_size": 16384})
    index_path = "/home/deeepakb/Projects/bedrockTest/faiss_index_temp"

    try:
        index_start_time = time.time()
        vector_store = FAISS.load_local(index_path, embeddings, allow_dangerous_deserialization=True)
        logger.info(f"FAISS index loaded successfully, it took {time.time() - index_start_time} Seconds")
    except Exception as e:
        logger.error(f"Error loading FAISS index: {e}")
        return

    with open('diff.txt') as f: 
        s = f.read()

    query = "What do the files in the /home/deeepakb/redshift-padb/test/raff/mv directory do?" + " Here's some conversation history to refine your query: " + " ".join(map(str, conversation_history))

    search_start_time = time.time()
    #results = vector_store.max_marginal_relevance_search(query, k=150, fetch_k = 150000, lambda_mult = 0.75)
    results = vector_store.similarity_search(query, k=50)
    if not results:
        logger.info("No results found for the query.")
        return
    else:
        logger.info(f"\nIt took {time.time() - search_start_time} seconds to search the index")

    logger.info(f"\nTime to find Chunks is {time.time() - search_start_time} \n")
    file_groups = defaultdict(list)
    for chunk in results:
        file_path = chunk.metadata.get('file_path', '')
        file_name = os.path.basename(file_path)
        file_groups[file_name].append(chunk)

    sorted_deduplicated_chunks = []
    for file_name, chunks in file_groups.items():
        sorted_file_chunks = sorted(chunks, key=lambda x: safe_int(x.metadata.get('ids', '')))
    
        deduplicated_file_chunks = []
        seen_content = set()
        for chunk in sorted_file_chunks:
            chunk_content = chunk.page_content.strip()
            if chunk_content not in seen_content:
                deduplicated_file_chunks.append(chunk)
                seen_content.add(chunk_content)
    
        sorted_deduplicated_chunks.extend(deduplicated_file_chunks)

    full_content = ""
    for chunk in sorted_deduplicated_chunks:
        chunk_content = chunk.page_content.strip()
        full_content += f"File: {os.path.basename(chunk.metadata.get('file_path', 'Unknown'))}\n"
        full_content += chunk_content + "\n\n"

    full_content = re.sub(r'\n{3,}', '\n\n', full_content)
    full_content = full_content.strip()

    with open("reconstructed_document_FAISS.txt", "w") as f:
        f.write(full_content)

    prompt_info = "Don't prepend the answer with anything like \"based on the code/snippets\""
    messages = conversation_history + [
        {
            "role": "user",
            "content": [
                {
                    "type": "text",
                    "text": query + "\n\n here is some code snippets that may or may not be relevant: " + full_content + "Here's some prompt info: " + prompt_info
                }
            ]
        }
    ]

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
            "messages": messages
        })
    }
    
    bedrock = boto3.client('bedrock-runtime')
    bedrock_response_time = time.time()
    response2 = bedrock.invoke_model(**kwargs)
    logger.info(f"\nIt took {time.time() - bedrock_response_time } seconds for the model to respond")
    
    response_content = json.loads(response2['body'].read())['content'][0]['text']
    print("\n\n\n Response: " + response_content)

    conversation_history.append({
        "role": "user",
        "content": [{"type": "text", "text": query}]
    })
    conversation_history.append({
        "role": "assistant",
        "content": [{"type": "text", "text": response_content}]
    })

    save_conversation_history(conversation_history)

if __name__ == "__main__":
    chat()
