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
import random
import math

# Logging setuppy
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

# MCTS Node class
class MCTSNode:
    def __init__(self, parent=None, query=None, current_text=None):
        self.parent = parent
        self.query = query
        self.current_text = current_text  # Store the current_text as part of the node
        self.children = []
        self.visits = 0
        self.value = 0
        self.is_fully_expanded = False

    def add_child(self, child_node):
        self.children.append(child_node)

    def best_child(self):
        if self.children:
            return max(self.children, key=lambda child: child.value / (child.visits + 1))
        return self

    def select(self, exploration_weight=1.0):
        if not self.is_fully_expanded:
            return self
        return max(self.children, key=lambda child: (child.value / (child.visits + 1)) + exploration_weight * math.sqrt(math.log(self.visits + 1) / (child.visits + 1)))


# MCTS class to perform tree search
class MCTS:
    def __init__(self, root: MCTSNode, vector_store, conversation_history, iterations=3):
        self.root = root
        self.vector_store = vector_store
        self.conversation_history = conversation_history
        self.iterations = iterations

    def search(self, exploration_weight=1.0):
        current_text = self.root.current_text  # Start with the initial current_text

        for _ in range(self.iterations):
            node = self.root
            while node.is_fully_expanded:
                node = node.select(exploration_weight)

            query = node.query if node.query else "What does stability.sql file do?"
            response = recursive_rag(query, self.vector_store, self.conversation_history, 1)
            
            # Create a new node with the response text as the current_text
            new_node = MCTSNode(parent=node, query=response, current_text=response)
            node.add_child(new_node)
            node = new_node
            node.visits += 1
            node.value += self.evaluate_node(node)

            # Update current_text if the response has a better reward
            if self.evaluate_node(new_node) > self.evaluate_node(node):
                current_text = new_node.current_text

        return current_text

    def evaluate_node(self, node):
        # Dummy evaluation function: can be improved to assess quality
        return random.random()


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
            history = json.load(f)
            return history if isinstance(history, list) else []
    except:
        return []

def save_conversation_history(history):
    with open("conversation_history.json", "w") as f:
        json.dump(history, f, indent=2)

def recursive_rag(query, vector_store, conversation_history, iterations=3, original_query=None):
    if iterations <= 0:
        return None

    if original_query is None:
        original_query = query

    results = vector_store.similarity_search(query, k=35)
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
        full_content += f"File: {os.path.basename(chunk.metadata.get('file_path', 'Unknown'))}\n"
        full_content += f"Chunk ID : {chunk.metadata.values}\n"
        full_content += chunk.page_content.strip() + "\n\n"
    full_content = re.sub(r'\n{3,}', '\n\n', full_content).strip()
    
    with open("/home/deeepakb/Projects/bedrockTest/reconstructed_document_FAISS.txt", "a") as f:
        f.write("Deepak")
        f.write(full_content)
        f.write("\n")

    prompt_info = "Keep your answer short and precise. Also when asked to generate code, give ONLY the code, nothing else, if you're unsure or lack confidence in your answer, do not give the code, and state why."
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
            "temperature": 0.1,
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

    with open("/home/deeepakb/Projects/bedrockTest/reconstructed_document_FAISS.txt", "w") as f:
        f.write("")

    embeddings = HuggingFaceEmbeddings(model_name="sentence-transformers/all-mpnet-base-v2", model_kwargs={'device': "cpu"},
                                                 encode_kwargs={"batch_size": 16384})
    index_path = "/home/deeepakb/Projects/bedrockTest/faiss_index_final"

    try:
        index_start_time = time.time()
        vector_store = FAISS.load_local(index_path, embeddings, allow_dangerous_deserialization=True)
        logger.info(f"FAISS index loaded successfully, it took {time.time() - index_start_time} Seconds")
    except Exception as e:
        logger.error(f"Error loading FAISS index: {e}")
        return

    query = """What does def test_alter_add_encode_col do?""" + " ".join(map(str, conversation_history)) 

    # Initialize MCTS
    root_node = MCTSNode(query=query, current_text=None)
    mcts = MCTS(root_node, vector_store, conversation_history, iterations=5)
    improved_query = mcts.search()

    # Pass the improved query to the recursive RAG
    response = recursive_rag(improved_query, vector_store, conversation_history, 3, query)
    
    print(response)
    print(f"\n It took {time.time() - index_start_time} to run the whole program")

if __name__ == "__main__":
    chat()
