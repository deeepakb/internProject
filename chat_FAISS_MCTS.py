import os
import json
import time
import logging
import boto3
import sys
from collections import OrderedDict, defaultdict
from llama_index.embeddings.huggingface import HuggingFaceEmbedding
from langchain.vectorstores import FAISS
import re

sys.setrecursionlimit(5000)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger()

def normalize_path(path):
    return os.path.normpath(os.path.expanduser(path))


def load_conversation_history():
    try:
        with open("conversation_history_mcts.json", "r") as f:
            history = json.load(f)
            return history if isinstance(history, list) else []
    except:
        return []


def save_conversation_history(history):
    with open("conversation_history_mcts.json", "w") as f:
        json.dump(history, f, indent=2)


def safe_int(value):
    try:
        return int(value)
    except ValueError:
        return float('inf')  


def evaluate_snippet(query, snippet):
    # Ensure snippet is valid and has a 'text' attribute before proceeding
    if snippet is None or not hasattr(snippet, 'page_content'):
        return 0  # If snippet is None or doesn't have a 'page_content' attribute, return a default score of 0
    return sum(1 for word in query.split() if word in snippet.page_content)


def deduplicate_snippets(snippets):
    """
    Deduplicate snippets based on their content and metadata to remove redundant code.
    """
    file_groups = defaultdict(list)
    for snippet in snippets:
        file_path = snippet.metadata.get('file_path', '')
        file_name = os.path.basename(file_path)
        file_groups[file_name].append(snippet)

    sorted_deduplicated_snippets = []
    seen_content = set()
    for file_name, chunks in file_groups.items():
        sorted_file_chunks = sorted(chunks, key=lambda x: safe_int(x.metadata.get("id", "")))

        deduplicated_file_chunks = []
        for chunk in sorted_file_chunks:
            chunk_content = chunk.page_content.strip()
            if chunk_content not in seen_content:
                deduplicated_file_chunks.append(chunk)
                seen_content.add(chunk_content)

        sorted_deduplicated_snippets.extend(deduplicated_file_chunks)

    return sorted_deduplicated_snippets


def monte_carlo_tree_search(query, vector_store, max_iterations=10, exploration_factor=1.4):
    """
    Perform MCTS to find the most relevant code snippets.
    """
    class Node:
        def __init__(self, snippet, parent=None):
            self.snippet = snippet
            self.parent = parent
            self.children = []
            self.visits = 0
            self.score = 0

        def is_fully_expanded(self):
            return len(self.children) >= len(self.snippet.children) if self.snippet else False

        def best_child(self, exploration_factor):
            return max(
                self.children,
                key=lambda n: (n.score / (n.visits + 1)) +
                              exploration_factor * ((self.visits ** 0.5) / (n.visits + 1))
            )

    retrieved_nodes = vector_store.similarity_search(query, k=350)
    if not retrieved_nodes:
        logger.info("No results found for the query.")
        return []

    logger.info(f"Retrieved {len(retrieved_nodes)} root snippets for evaluation.")

    root = Node(snippet=None)  
    root.children = [Node(snippet=node, parent=root) for node in retrieved_nodes]

    for _ in range(max_iterations):
        current_node = root
        while current_node.children and current_node.is_fully_expanded():
            current_node = current_node.best_child(exploration_factor)

        if not current_node.is_fully_expanded():
            if current_node.snippet and hasattr(current_node.snippet, 'children'):
                child_snippet = current_node.snippet.children[len(current_node.children)]
                new_child = Node(snippet=child_snippet, parent=current_node)
                current_node.children.append(new_child)
                current_node = new_child
            else:
                logger.error("Current node snippet has no children or is None.")
                break  # or continue, depending on the logic you want

        evaluation_score = evaluate_snippet(query, current_node.snippet)

        while current_node is not None:
            current_node.visits += 1
            current_node.score += evaluation_score
            current_node = current_node.parent

    top_nodes = sorted(root.children, key=lambda n: n.score / (n.visits + 1), reverse=True)
    top_snippets = [node.snippet for node in top_nodes[:50]]

    # Deduplicate the top snippets
    top_snippets = deduplicate_snippets(top_snippets)

    with open("/home/deeepakb/Projects/bedrockTest/reconstructed_document_faiss_mcts.txt", "a") as f:
        f.write("\n".join([snippet.page_content for snippet in top_snippets if snippet and hasattr(snippet, 'page_content') and "def burst_no_backup_table_helper" not in snippet.page_content]))  # ignore
        f.write("\n")

    return top_snippets


def query_llm_with_snippets(query, snippets, conversation_history):
    """
    Pass the best snippets to the LLM with the user query.
    """
    retrieved_snippets = "\n".join([snippet.page_content for snippet in snippets if snippet and hasattr(snippet, 'page_content')])

    prompt_info = "Keep your answer short and precise. When asked to generate code, provide ONLY the code, nothing else."
    messages = conversation_history + [
        {
            "role": "user",
            "content": [
                {
                    "type": "text",
                    "text": f"Query: {query}\n\nHere are relevant code snippets:\n\n{retrieved_snippets}\n\nPrompt info: {prompt_info}"
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

    bedrock = boto3.client("bedrock-runtime")
    time.sleep(1)
    response = bedrock.invoke_model(**kwargs)
    response_content = json.loads(response["body"].read())["content"][0]["text"]

    conversation_history.extend([
        {"role": "user", "content": [{"type": "text", "text": query}]},
        {"role": "assistant", "content": [{"type": "text", "text": response_content}]}
    ])
    save_conversation_history(conversation_history)
    return response_content


def recursive_query_llm(query, vector_store, conversation_history, max_depth=5):
    """
    Recursively query the LLM with updated snippets and conversation history.
    """
    if max_depth <= 0:
        return conversation_history

    logger.info(f"Performing MCTS to find the best snippets at depth {max_depth}.")
    best_snippets = monte_carlo_tree_search(query, vector_store, max_iterations=50000)

    logger.info(f"Querying the LLM with the best snippets at depth {max_depth}.")
    response = query_llm_with_snippets(query, best_snippets, conversation_history)

    # Update the query to be the LLM's response for the next iteration
    updated_query = response  # This could be further modified if needed

    # Recurse with updated query and conversation history
    return recursive_query_llm(updated_query, vector_store, conversation_history, max_depth - 1)


def chat():
    from langchain.embeddings import HuggingFaceEmbeddings

    embed_model = HuggingFaceEmbeddings(model_name="sentence-transformers/all-mpnet-base-v2")
    conversation_history = load_conversation_history()

    with open("/home/deeepakb/Projects/bedrockTest/reconstructed_document_faiss_mcts.txt", "w") as f:
        f.write("")

    index_path = "/home/deeepakb/Projects/bedrockTest/faiss_index_final"
    try:
        index_start_time = time.time()
        vector_store = FAISS.load_local(index_path, embed_model, allow_dangerous_deserialization=True)
        logger.info(f"FAISS Index loaded successfully, it took {time.time() - index_start_time} seconds")
    except Exception as e:
        logger.error(f"Error loading FAISS Index: {e}")
        return

    query = """What does the stability.sql file do."""
    logger.info(f"Query type: {type(query)}")

    # Start the recursive querying process
    logger.info("Starting recursive querying of LLM.")
    conversation_history = recursive_query_llm(query, vector_store, conversation_history, max_depth=5)

    print(conversation_history[-1]['content'][0]['text'])
    print(f"\nIt took {time.time() - index_start_time} seconds to run the whole program")


if __name__ == "__main__":
    chat()
