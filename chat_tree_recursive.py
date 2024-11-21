import os
from llama_index.core import GPTTreeIndex, StorageContext, load_index_from_storage
from llama_index.embeddings.huggingface import HuggingFaceEmbedding
from llama_index.core.indices.tree.select_leaf_embedding_retriever import TreeSelectLeafEmbeddingRetriever, TreeSelectLeafRetriever
import logging
import json
import boto3
import time
from collections import OrderedDict
from llama_index.core import Settings
import sys


sys.setrecursionlimit(5000)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

def normalize_path(path):
    return os.path.normpath(os.path.expanduser(path))


def load_conversation_history():
    try:
        with open("conversation_history_tree.json", "r") as f:
            history = json.load(f)
            return history if isinstance(history, list) else []
    except:
        return []

def save_conversation_history(history):
    with open("conversation_history_tree.json", "w") as f:
        json.dump(history, f, indent=2)

def recursive_rag(query, retriever, conversation_history, iterations=3, original_query=None):
    if iterations <= 0:
        return None

    if original_query is None:
        original_query = query

    retrieved_nodes = retriever.retrieve(query)
    if not retrieved_nodes:
        logger.info("No results found for the query.")
        return None

    # Ignore
    unique_nodes = OrderedDict((node.text, node) for node in retrieved_nodes if "def test_classic_resize_mode_flag" not in node.text)
    #print(retrieved_nodes[0].metadata.get("file_path"))
    retrieved_snippets = "\n".join(unique_nodes.keys())

    
    with open("/home/deeepakb/Projects/bedrockTest/reconstructed_document_tree.txt", "a") as f:
        f.write("Deepak")
        f.write(retrieved_snippets)
        f.write("\n")

    prompt_info = "Keep your answer short and precise. Also when asked to generate code, give ONLY the code, nothing else."
    messages = conversation_history + [
        {
            "role": "user",
            "content": [
                {
                    "type": "text",
                    "text": f"Original query: {original_query}\n\nFollow-up query: {query}\n\nPlease answer the follow-up query while keeping the original query in context. Here are some code snippets that may or may not be relevant:\n\n{retrieved_snippets}\n\nHere's some prompt info: {prompt_info}"
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
            "temperature": 0.1,
            "top_p": 0.95,
            "messages": messages
        })
    }

    bedrock = boto3.client('bedrock-runtime')
    time.sleep(1)
    response = bedrock.invoke_model(**kwargs)

    response_content = json.loads(response['body'].read())['content'][0]['text']

    conversation_history.extend([
        {"role": "user", "content": [{"type": "text", "text": f"Original query: {original_query}\nFollow-up query: {query}"}]},
        {"role": "assistant", "content": [{"type": "text", "text": response_content}]}
    ])
    save_conversation_history(conversation_history)
    logger.info(f"\n {iterations - 1} Left \n")

    next_response = recursive_rag(response_content, retriever, conversation_history, iterations - 1, original_query)

    return next_response if next_response is not None else response_content

def chat():
    embed_model = HuggingFaceEmbedding(model_name="sentence-transformers/all-mpnet-base-v2")
    conversation_history = load_conversation_history()
    Settings.llm = None
    
    with open("/home/deeepakb/Projects/bedrockTest/reconstructed_document_tree.txt", "w") as f:
        f.write("")

    index_path = "/home/deeepakb/Projects/bedrockTest/tree_index"
    try:
        index_start_time = time.time()
        storage_context = StorageContext.from_defaults(persist_dir=index_path)
        index = load_index_from_storage(storage_context)
        logger.info(f"Tree Index loaded successfully, it took {time.time() - index_start_time} seconds")
    except Exception as e:
        logger.error(f"Error loading Tree Index: {e}")
        return

    retriever = TreeSelectLeafEmbeddingRetriever(
        index=index,
        embed_model=embed_model,
        child_branch_factor=40
    )

    query_time = time.time()
    query = """Based on this summary for the def test_classic_resize_mode_flag, Generate the python code for it: Based on the code files, here's a precise explanation of test_classic_resize_mode_flag:Key Components:1. Test Purpose:- Validates classic resize mode functionality during cluster resizing operations- Ensures proper cluster initialization and configuration2. Main Test Flow:```pythondef test_classic_resize_mode_flag(self, cluster_session, db_session, cluster, topology_tagging):  # Validate initial state  self.validate_first_sb_version()    # Check topology version before resize  topology_file_version_before_resize = s3_helpers.get_local_topology_file_version()    # Perform resize operation  start_sim_db(classic_resize=True, node_count=TARGET_CLUSTER_SIZE)    # Validate post-resize state  self.validate_slice_count(db_session, TARGET_CLUSTER_TOTAL_SLICES)    # Verify topology version changes  if topology_tagging:  assert ((topology_file_version_after_resize - topology_file_version_before_resize) >= 1000)```3. Key Validations:- Bootstrap messages for classic resize mode- Virgin cluster start verification- Dev_db table cleanup- Superblock version = 1- Topology file version increment- Slice count validation.""" + " ".join(map(str, conversation_history))

    response = recursive_rag(query, retriever, conversation_history, 3, query)
    print(response)
    print(f"\nIt took {time.time() - index_start_time} to run the whole program")

if __name__ == "__main__":
    chat()
