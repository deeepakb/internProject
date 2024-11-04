import os
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_community.vectorstores import FAISS
from langchain.schema import Document
import logging
import re
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

def chat():
    embeddings = HuggingFaceEmbeddings(model_name="sentence-transformers/all-mpnet-base-v2")
    index_path = "/home/deeepakb/Projects/bedrockTest/faiss_index_final"

    try:
        index_start_time = time.time()
        vector_store = FAISS.load_local(index_path, embeddings, allow_dangerous_deserialization=True)
        logger.info(f"FAISS index loaded successfully, it took {time.time() - index_start_time} Seconds")
    except Exception as e:
        logger.error(f"Error loading FAISS index: {e}")
        return

    query = "Are there any additional features or capabilities that you think could be beneficial to incorporate in the codebase?"

    search_start_time = time.time()
    results = vector_store.similarity_search(
        query,
        k=20,
    )    


    if not results:
        logger.info("No results found for the query.")
        return
    else:
        logger.info(f"\nIt took {time.time() - search_start_time} seconds to search the index")

    logger.info(f"Number of chunks retrieved: {len(results)}")
    logger.info(f"\nTime to find Chunks is {time.time() - search_start_time} \n")
    sorted_chunks = sorted(results, key=lambda x: (safe_int(x.metadata.get('id', '')), x.page_content))

    full_content = ""
    for chunk in sorted_chunks:
        chunk_content = chunk.page_content.strip()
        if chunk_content not in full_content:
            full_content += chunk_content + "\n"

    full_content = re.sub(r'\n{3,}', '\n\n', full_content)  # Replace multiple newlines with double newlines
    full_content = full_content.strip()  # Remove leading/trailing whitespace

    with open("reconstructed_document_FAISS.txt", "w") as f:
        f.write(full_content)


    kwargs = {
        "modelId": "anthropic.claude-3-5-sonnet-20241022-v2:0",
        "contentType": "application/json",
        "accept": "application/json",
        "body": json.dumps({
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 750,
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
    print("\n\n\n Response: " + json.loads(response2['body'].read())['content'][0]['text'])

if __name__ == "__main__":
    chat()
