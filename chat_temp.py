import os
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_community.vectorstores import FAISS
from langchain.schema import Document
import logging
import re

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
        vector_store = FAISS.load_local(index_path, embeddings, allow_dangerous_deserialization=True)
        logger.info("FAISS index loaded successfully")
    except Exception as e:
        logger.error(f"Error loading FAISS index: {e}")
        return

    query = "what is class TestDuplicateQueryId"
    results = vector_store.similarity_search(query, k=1)
    
    if not results:
        logger.info("No results found for the query.")
        return

    most_similar_doc = results[0]
    file_path = normalize_path(most_similar_doc.metadata.get('file_path'))
    logger.info(f"Most similar document found in file: {file_path}")

    all_docs = vector_store.similarity_search(
        "dummy query",
        k=1000,
        filter = {"file_path" : file_path}
    )

    matching_docs = all_docs
    logger.info(f"Number of chunks retrieved: {len(matching_docs)}")

    sorted_chunks = sorted(matching_docs, key=lambda x: (safe_int(x.metadata.get('id', '')), x.page_content))

    full_content = ""
    for chunk in sorted_chunks:
        chunk_content = chunk.page_content.strip()
        if chunk_content not in full_content:
            full_content += chunk_content + "\n"

    full_content = re.sub(r'\n{3,}', '\n\n', full_content)  # Replace multiple newlines with double newlines
    full_content = full_content.strip()  # Remove leading/trailing whitespace

    with open("reconstructed_document.txt", "w") as f:
        f.write(full_content)

    logger.info(f"Reconstructed document content has been saved to 'reconstructed_document.txt'")
    logger.info(f"Total length of reconstructed content: {len(full_content)} characters")

if __name__ == "__main__":
    chat()
