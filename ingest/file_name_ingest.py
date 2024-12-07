import boto3
import json
import os
import gc
import logging
import threading
from langchain_huggingface import HuggingFaceEmbeddings
import time
import faiss
from langchain_ollama import OllamaEmbeddings
from langchain.text_splitter import Language  # Import Language enum for supported languages
from langchain_community.docstore.in_memory import InMemoryDocstore
from langchain_community.vectorstores import FAISS
from llama_index.core import SimpleDirectoryReader
from langchain_community.embeddings import HuggingFaceEmbeddings
from llama_index.core import Settings
from llama_index.core import StorageContext, load_index_from_storage
from llama_index.embeddings.huggingface import HuggingFaceEmbedding
from llama_index.core.node_parser import CodeSplitter, SentenceSplitter
import uuid
import fnmatch
from langchain.text_splitter import RecursiveCharacterTextSplitter
from functools import partial
import multiprocessing
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor
from collections import deque
import queue
from logging.handlers import QueueHandler, QueueListener
import sys

log_queue = queue.Queue(-1)
queue_handler = QueueHandler(log_queue)
logger = logging.getLogger()
logger.addHandler(queue_handler)
logger.setLevel(logging.INFO)

stream_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
stream_handler.setFormatter(formatter)

listener = QueueListener(log_queue, stream_handler)
listener.start()

# Ensure output is flushed
sys.stdout.reconfigure(line_buffering=True)

# File type mappings
file_type_to_language = {
    ".cpp": "cpp", ".hpp": "cpp", ".go": "go", ".sql": "text", ".java": "java",
    ".json": "json", ".kt": "kotlin", ".ts": "ts", ".php": "php", ".py": "python",
    ".rst": "rst", ".sh": "text", ".rb": "ruby", ".xml": "text",
    ".rs": "rust", ".scala": "scala", ".swift": "swift",
    ".md": "markdown", ".tex": "latex",
    ".c": "c", ".csv": "text", ".pl": "perl", ".cs": "c-sharp",
    ".hs": "haskell", ".html": "text", ".orig": "text", ".txt": "text",
    ".mk": "text", ".res": "text",
    ".data": "text", ".component": "text", ".h": "cpp",
    ".template": "text"
}

def file_size_is_within_limit(file_path, max_size=12 * 1024 * 1024):  # 8 MB in bytes
    return os.path.getsize(file_path) <= max_size

from langchain.text_splitter import TextSplitter

class SQLSplitter(TextSplitter):
    def __init__(self, chunk_size=2048, chunk_overlap=100):
        super().__init__(chunk_size=chunk_size, chunk_overlap=chunk_overlap)

    def split_text(self, text: str) -> list:
        # Split by semicolons, assuming SQL statements end with ';'
        sql_statements = [stmt.strip() + ";" for stmt in text.split(";") if stmt.strip()]
        chunks = []
        current_chunk = []

        for statement in sql_statements:
            if sum(len(stmt) for stmt in current_chunk) + len(statement) <= self.chunk_size:
                current_chunk.append(statement)
            else:
                chunks.append(" ".join(current_chunk))
                current_chunk = [statement]

        if current_chunk:
            chunks.append(" ".join(current_chunk))

        return chunks

# Update your file processing logic to use SQLSplitter for .sql files
def process_single_document(doc, i, no_of_batches):
    try:
        file_path = doc.metadata.get("file_path")
        file_name = os.path.basename(file_path)
        logger.info(f"Processing File: {file_path}")
        
        # Save only the file name, not the code content
        return [
            {
                "text": f"File Name: {file_name}",
                "metadata": {"file_path": file_path, "id": i}  # Add 'id' to metadata here
            }
        ]
    except Exception as e:
        logger.error(f"Error processing file: {file_path}")
        logger.exception(e)
        return []

def ingest():
    no_of_batches = 0
    os.environ["PYTHONMALLOC"] = "malloc"
    multiprocessing.set_start_method('spawn')
    s3 = boto3.client('s3')

    # Use CodeBERT for code-specific embeddings
    embed_model = HuggingFaceEmbeddings(model_name="sentence-transformers/all-mpnet-base-v2",
                                        model_kwargs={'device': "cpu"},
                                        encode_kwargs={"batch_size": 131072})
    Settings.embed_model = embed_model
    Settings.llm = None

    reader = SimpleDirectoryReader(input_dir="/home/deeepakb/redshift-padb", recursive=True)
    documents = reader.load_data()
    chunk_size = 25000
    total_docs = len(documents)
    processed_docs = 0
    skip_count = 0
    not_skip_count = 0
    nodes = []
    batch_count = 0
    start_time = time.time()

    for i in range(0, total_docs, chunk_size):
        doc_chunk = documents[i:i + chunk_size]
        
        with concurrent.futures.ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
            future_to_doc = {executor.submit(process_single_document, doc, i, no_of_batches): doc for doc in doc_chunk}

            for future in future_to_doc:
                doc = future_to_doc[future]
                try:
                    result = future.result(timeout=60)
                    if result:
                        not_skip_count += 1
                        nodes.extend(result)
                        with open("/home/deeepakb/Projects/bedrockTest/text/log.txt", "a") as f:
                            f.write(f"Finished: {doc.metadata.get('file_path')}\n")
                    else:
                        skip_count += 1
                    processed_docs += 1

                except concurrent.futures.TimeoutError:
                    logger.warning(f"Timeout error processing file: {doc.metadata.get('file_path')}")
                    skip_count += 1
                    with open("/home/deeepakb/Projects/bedrockTest/text/log.txt", "w") as f:
                        f.write(f"Timeout error processing file: {doc.metadata.get('file_path')}\n")
                    future.cancel()
                except Exception as e:
                    logger.error(f"Error processing file: {doc.metadata.get('file_path')}")
                    logger.exception(e)
                    skip_count += 1

                logger.info(f"Processed {processed_docs} of {total_docs} documents")

        gc.collect()
        time.sleep(0.5)
        batch_count += 1

    logger.info(f"Finished processing with skipped: {skip_count}, and not skipped: {not_skip_count}")

    # Initialize FAISS vector store using CodeBERT embeddings
    embeddings = HuggingFaceEmbeddings(model_name="sentence-transformers/all-mpnet-base-v2",
                                       model_kwargs={'device': "cpu"},
                                       encode_kwargs={"batch_size": 131072})

    dimension = len(embeddings.embed_query("hello world"))  # Code-specific example

    index = faiss.IndexHNSWFlat(dimension,  512)
    index.hnsw.efConstruction = 2048  # Set this to a higher value for more accurate indexing

    # Set efSearch (affects search phase)
    index.hnsw.efSearch = 1024  # Set this based on desired search quality (higher = more accurate)

    vector_store = FAISS(
        embedding_function=embeddings,
        index=index,
        docstore=InMemoryDocstore(),
        index_to_docstore_id={},
    )

    total_nodes = len(nodes)
    for i, node in enumerate(nodes):
        with open("snippets.txt", "a") as f:
            f.write(f"{node['text']} \n")
            f.write(f"{node['metadata']} \n")
            f.write(f"{i} \n")

        vector_store.add_texts(
            texts=[node["text"]],
            metadatas=[node["metadata"]],
            ids=[i]
        )

        logger.info(f"Adding node {i + 1} of {total_nodes} to vector store")

    end_time = time.time()
    logger.info(f"Time taken to add all texts: {end_time - start_time} seconds")
    vector_store.save_local("/home/deeepakb/Projects/bedrockTest/faiss_index_file_names")

if __name__ == "__main__":
    try:
        ingest()
    finally:
        listener.stop()
