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
    index_path = "/home/deeepakb/Projects/bedrockTest/faiss_index_temp"

    try:
        index_start_time = time.time()
        vector_store = FAISS.load_local(index_path, embeddings, allow_dangerous_deserialization=True)
        logger.info(f"FAISS index loaded successfully, it took {time.time() - index_start_time} Seconds")
    except Exception as e:
        logger.error(f"Error loading FAISS index: {e}")
        return

    query = "Can you explain the tearDown() implementation of the TestAutoWorker in the file test_auto_vacuum.cpp?"
    search_start_time = time.time()
    results = vector_store.max_marginal_relevance_search(query, k=25, fetch_k = 10000, lambda_mult = 0.99)

    if not results:
        logger.info("No results found for the query.")
        return
    else:
        logger.info(f"\nIt took {time.time() - search_start_time} seconds to search the index")
    print(f"length is {len(results)}")
    logger.info(f"\nTime to find Chunks is {time.time() - search_start_time} \n")
    sorted_chunks = sorted(results, key=lambda x: (safe_int(x.metadata.get('id', '')), x.page_content))

    full_content = ""
    for chunk in sorted_chunks:
        chunk_content = chunk.page_content.strip()
        file_path = chunk.metadata.get('file_path', 'Unknown file')
        file_name = os.path.basename(file_path)
        full_content += f"File: {file_name}\n{chunk_content}\n\n"

    full_content = re.sub(r'\n{3,}', '\n\n', full_content) 
    full_content = full_content.strip()  

    with open("reconstructed_document_FAISS.txt", "w") as f:
        for r in results:
            f.write(f"File: {os.path.basename(r.metadata.get('file_path', 'Unknown file'))}\n")
            f.write(r.page_content)
            f.write("\n\n")




    kwargs = {
        #"modelId": "anthropic.claude-3-5-sonnet-20241022-v2:0",
        "modelId": "anthropic.claude-3-sonnet-20240229-v1:0",
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
    
"""
    response_body = json.loads(response2['body'].read())

    if 'usage' in response_body:
        input_tokens = response_body['usage'].get('input_tokens', 'N/A')
        output_tokens = response_body['usage'].get('output_tokens', 'N/A')
        print(f"\nInput tokens: {input_tokens}")
        print(f"Output tokens: {output_tokens}")
    else:
        print("\nToken usage information not available in the response.")
sample_stl_query = "select count(*) from stl_scan;"
        self.cleanup_log_files_by_type('stl_invariant')
        with cluster_session(gucs=self.GUCS):
            # The query should fail when STL query throws execption at scan
            # step, but PADB should not crash.
            with cluster.event('EtThrowExceptionWhenInitSTLIterator'):
                with db_session.cursor() as cursor:
                    error = None
                    try:
                        cursor.execute(sample_stl_query)
                    except tuple(InternalError + DatabaseError) as e:
                        error = e
                    error_msg = 'Simulate S3 client excpetion'
                    assert str(error).strip().find(error_msg) != -1
            # Run stl query again to verify the PADB is healthy.
            with db_session.cursor() as cursor:
                cursor.execute(sample_stl_query)
                scan_count = cursor.fetch_scalar()
                assert scan_count >= 0
            # Verify Systbl_Iterator won't be init multiple times.
            self.verify_no_invariant(db_session, cluster)

        
"""

if __name__ == "__main__":
    chat()
