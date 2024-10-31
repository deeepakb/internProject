import os
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_community.vectorstores import FAISS
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

def chat():
    embeddings = HuggingFaceEmbeddings(model_name="sentence-transformers/all-mpnet-base-v2")
    index_path = "/home/deeepakb/Projects/bedrockTest/faiss_index_final"

    try:
        vector_store = FAISS.load_local(index_path, embeddings, allow_dangerous_deserialization=True)
        logger.info("FAISS index loaded successfully")
    except Exception as e:
        logger.error(f"Error loading FAISS index: {e}")
        return

    query = "what is class TestDisableSystemViewDescribeInServerless"
    results = vector_store.similarity_search_with_score(query, k=1)
    
    if not results:
        logger.info("No results found for the query.")
        return

    most_similar_doc, score = results[0]
    file_path = most_similar_doc.metadata.get('file_path')
    
    logger.info(f"Most similar document found in file: {file_path}")
    logger.info(f"Similarity score: {score}")

    # Retrieve all chunks from the same file
    all_chunks = vector_store.similarity_search(
        "dummy query to retrieve all chunks",
        k=10000,  # Increase this number if you have more chunks per file
        filter={"file_path": file_path}
    )

    # Sort chunks based on their position in the file
    sorted_chunks = sorted(all_chunks, key=lambda x: x.metadata.get('chunk_id', ''))

    # Reconstruct the full document
    full_content = ""
    for chunk in sorted_chunks:
        chunk_content = chunk.page_content.strip()
        if not full_content.endswith(chunk_content):
            full_content += chunk_content + "\n"

    print("\nReconstructed Full Content of the File:")
    print(full_content)

    with open("reconstructed_document.txt", "w") as f:
        f.write(full_content)

    logger.info(f"Reconstructed document content has been saved to 'reconstructed_document.txt'")
    logger.info(f"Number of chunks retrieved: {len(all_chunks)}")
    logger.info(f"Total length of reconstructed content: {len(full_content)} characters")

if __name__ == "__main__":
    chat()
