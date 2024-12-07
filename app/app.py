from flask import Flask, request, jsonify, render_template
import os
import json
import time
import logging
from collections import defaultdict
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_community.vectorstores import FAISS
import boto3
import re

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

app = Flask(__name__)

# Helper functions
def load_conversation_history():
    try:
        with open("conversation_history.json", "r") as f:
            return json.load(f)
    except:
        return []

def save_conversation_history(history):
    with open("conversation_history.json", "w") as f:
        json.dump(history, f, indent=2)

# Recursive RAG function
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
        sorted_file_chunks = sorted(chunks, key=lambda x: int(x.metadata.get("id", "9999")))
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

    with open("/home/deeepakb/Projects/bedrockTest/app/reconstructed_document_FAISS.txt", "a") as f:
        f.write("Deepak")
        f.write(full_content)
        f.write("\n")

    prompt_info = (
    "You are a bot primarily aimed at helping engineers working on the Amazon Redshift database due to its large size (1.5GB).\n"
    "You are being augmented with code snippets from the Redshift codebase, however, do NOT reference the fact you have been augmented with  snippets in your responses. The user does not know you are being given code snippets. respond as if you have all the knowledge by default.\n"
    "If you believe the code snippets that you need to answer were not supplied, Simply state you're unable to answer the question, don't reference the fact that the code snippets aren't relevant. I REPEAT NEVER MENTION THE CODE SNIPPETS.\n"
    "Keep your answer short and precise. NEVER MENTION THE CODE SNIPPETS.\n"
    "Conversation history is added for context; improve code iteratively if possible. NEVER MENTION THE CODE SNIPPETS. "
    "If no improvements are possible, return the current code. NEVER MENTION THE CODE SNIPPETS. "
    "Use the conversation history to understand the context and answer based on that. NEVER MENTION THE CODE SNIPPETS.\n"
    "Provide direct answers to the user's queries. Avoid introductory phrases like 'Based on the code...' or 'Based on the query...' NEVER MENTION THE CODE SNIPPETS. ."
    )


    # Remove previous iterations for the current query, keeping only the last one
    conversation_history = [
        entry for entry in conversation_history if "Original query:" not in entry.get("content", "")
    ]

    messages = conversation_history + [
        {
            "role": "user",
            "content": f"Original query: {original_query}\n\nFollow-up query: {query}\n\n"
                       f"Here are some code code snippets which may or may not be relevant to the query, first check if the query requires them or not before factoring them in:\n{full_content}\n\n Prompt info that should be followed VERY closely: {prompt_info}"
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
    time.sleep(15)
    response = bedrock.invoke_model(**kwargs)
    response_content = json.loads(response['body'].read())['content'][0]['text']

    # Append only the last iteration for the current query
    conversation_history.extend([
        {"role": "user", "content": f"Original query: {original_query}\nFollow-up query: {query}"},
        {"role": "assistant", "content": response_content}
    ])
    
    # Save the updated history with only the latest iteration for each query
    save_conversation_history(conversation_history)
    logger.info("\nIteration finished\n")

    next_response = recursive_rag(response_content, vector_store, conversation_history, iterations - 1, original_query)
    return next_response if next_response else response_content

# Load FAISS vector store and embeddings
embeddings = HuggingFaceEmbeddings(
    model_name="sentence-transformers/all-mpnet-base-v2",
    model_kwargs={'device': "cpu"},
    encode_kwargs={"batch_size": 131072}
)
index_path = "/home/deeepakb/Projects/bedrockTest/faiss_index_final_improved"
vector_store = FAISS.load_local(index_path, embeddings, allow_dangerous_deserialization=True)
logger.info("FAISS index loaded successfully.")

# Flask routes
@app.route("/")
def home():
    return render_template("index.html")

@app.route("/chat", methods=["POST"])
def chat():
    try:
        user_query = request.json.get("query", "")
        conversation_history = load_conversation_history()
        response = recursive_rag(user_query, vector_store, conversation_history, 5, user_query)
        return jsonify({"response": response})
    except Exception as e:
        logger.error(f"Error during chat processing: {e}")
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True)
