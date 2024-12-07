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
        with open("/home/deeepakb/Projects/bedrockTest/conversation_history/conversation_history.json", "r") as f:
            history = json.load(f)
            return history if isinstance(history, list) else []
    except:
        return []

def save_conversation_history(history):
    with open("/home/deeepakb/Projects/bedrockTest/conversation_history/conversation_history.json", "w") as f:
        json.dump(history, f, indent=2)

def recursive_rag(query, vector_store, conversation_history, iterations=3, original_query=None):
    if iterations <= 0:
        return None

    if original_query is None:
        original_query = query

    results = vector_store.similarity_search(query, k=50)
    if not results:
        logger.info("No results found for the query.")
        return None

    file_groups = defaultdict(list)
    for chunk in results:
        file_path = chunk.metadata.get('file_path', '')
        file_name = os.path.basename(file_path)
        file_groups[file_name].append(chunk)

    sorted_deduplicated_chunks = []
    for file_name, chunks in file_groups.items():
        sorted_file_chunks = sorted(chunks, key=lambda x: safe_int(x.metadata.get("id", "")))
        
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
        #ignore
        #if chunk.metadata.get("file_path") == "/home/deeepakb/redshift-padb/test/raff/ddm/testfiles/lf_alp/test_lf_alp_explain_hidden.sql":
            #continue
        if "class TestAttachBurstCluster" in chunk.page_content:
            continue

        full_content += f"File: {os.path.basename(chunk.metadata.get('file_path', 'Unknown'))}\n"
        full_content += f"Chunk ID : {chunk.metadata.values}\n"
        full_content += chunk.page_content.strip() + "\n\n"
    full_content = re.sub(r'\n{3,}', '\n\n', full_content).strip()
    
    with open("/home/deeepakb/Projects/bedrockTest/text/reconstructed_document_FAISS.txt", "a") as f:
        f.write("Deepak")
        f.write(full_content)
        f.write("\n")

        prompt_info = "Keep your answer short and precise. Also when asked to generate code, give ONLY the code, nothing else. Furthermore, conversation history is being added to the message so you can see previous answers and iterate and improve on the code, Please try your best to improve the code on each iteration, if you cant improve it any more just give the code."
    messages = conversation_history + [
        {
            "role": "user",
            "content": [
                {
                    "type": "text",
                    "text": f"Original query: {original_query}\n\nFollow-up query: {query}\n\nPlease answer the follow-up query while keeping the original query in context. Here are some code snippets that may or may not be relevant:\n\n{full_content}\n\nHere's some prompt info: {prompt_info}"
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
            "max_tokens": 550,
            "top_k": 10,
            "stop_sequences": [],
            "temperature": 0.3,
            "top_p": 0.95,
            "messages": messages
        })
    }

    bedrock = boto3.client('bedrock-runtime')
    time.sleep(20)
    response = bedrock.invoke_model(**kwargs)

    response_content = json.loads(response['body'].read())['content'][0]['text']

    conversation_history.extend([
        {"role": "user", "content": [{"type": "text", "text": f"Original query: {original_query}\nFollow-up query: {query}"}]},
        {"role": "assistant", "content": [{"type": "text", "text": response_content}]}
    ])
    save_conversation_history(conversation_history)
    logger.info("\nIteration finished\n")

    next_response = recursive_rag(response_content, vector_store, conversation_history, iterations - 1, original_query)

    return next_response if next_response is not None else response_content



def chat():
    conversation_history = load_conversation_history()

    with open("/home/deeepakb/Projects/bedrockTest/text/reconstructed_document_FAISS.txt", "w") as f:
        f.write("")

    embeddings = HuggingFaceEmbeddings(model_name="sentence-transformers/all-mpnet-base-v2",
                                       model_kwargs={'device': "cpu"},
                                       encode_kwargs={"batch_size": 131072})
    index_path = "/home/deeepakb/Projects/bedrockTest/faiss_index_final_improved_2"

    try:
        index_start_time = time.time()
        vector_store = FAISS.load_local(index_path, embeddings, allow_dangerous_deserialization=True)
        logger.info(f"FAISS index loaded successfully, it took {time.time() - index_start_time} Seconds")
    except Exception as e:
        logger.error(f"Error loading FAISS index: {e}")
        return


    query = """Explain the stability.sql file.""" + " ".join(map(str, conversation_history)) 

    response = recursive_rag(query, vector_store, conversation_history, 5, query)
    #results = vector_store.max_marginal_relevance_search(query, k=150, fetch_k = 150000, lambda_mult = 0.75)
    print(response)
    print(f"\n It took {time.time() - index_start_time} to run the whole program")

if __name__ == "__main__":
    chat()
