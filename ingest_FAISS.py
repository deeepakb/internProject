import boto3
import json
import os
import gc
import logging
import threading
from langchain_huggingface import HuggingFaceEmbeddings
import time
import faiss
from langchain.text_splitter import Language  # Import Language enum for supported languages
from langchain_community.docstore.in_memory import InMemoryDocstore
from langchain_community.vectorstores import FAISS
from llama_index.core import SimpleDirectoryReader
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
    ".rst": "rst", ".sh": "text", ".rb": "ruby", ".xml": "text", ".proto": "text",
    ".rs": "rust", ".scala": "scala", ".yaml": "yaml", ".swift": "swift",
    ".md": "markdown", ".tex": "latex", ".sol": "sol", ".cob": "cobol",
    ".c": "c", ".csv": "text", ".lua": "lua", ".pl": "perl", ".cs": "c-sharp",
    ".hs": "haskell", ".html": "text", ".orig": "text", ".txt": "text",
    ".mk": "text", ".res": "text", ".ldf": "text", ".conf": "text",
    ".data": "text", ".component": "text", ".dump": "text", ".h": "text",
    ".po": "text", ".template" : "text", ".cmake" : "text"
}

def file_size_is_within_limit(file_path, max_size=10 * 1024 * 1024):  # 8 MB in bytes
    return os.path.getsize(file_path) <= max_size

# Update your process_single_document function
def process_single_document(doc, no_of_batches):
    try:
        file_path = doc.metadata.get("file_path")
        file_name = os.path.basename(file_path)
        logger.info(f"Processing File: {file_path}")
        _, file_type = os.path.splitext(file_name)
        file_type = file_type_to_language.get(file_type.lower(), None)

        if not file_size_is_within_limit(file_path):
            logger.warning(f"Skipping large file: {file_path}")
            with open("/home/deeepakb/Projects/bedrockTest/log.txt", "a") as f:
                f.write(f"Skipping large file: {file_path}\n")
            return []

        # Check if file type is supported and use the appropriate text splitter
        if file_type is not None and file_type.upper() in Language.__members__:
            language = Language[file_type.upper()]
            text_splitter = RecursiveCharacterTextSplitter.from_language(
                language=language, chunk_size=2048, chunk_overlap=100
            )
            
        else:
            text_splitter = RecursiveCharacterTextSplitter(chunk_size=2048, chunk_overlap=100)

        # Split document and add file name to each chunk's content
        content = doc.get_content()
        chunks = text_splitter.split_text(content)
        
        with open("/home/deeepakb/Projects/bedrockTest/log.txt", "a") as f:
                f.write(f"File Exists: {file_path}")

        return [
            {
                "text": f"File Path: {file_path}\n\n{chunk}",
                "metadata": {"file_path": file_path}
            } for chunk in chunks
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

    Settings.embed_model = HuggingFaceEmbeddings(model_name="sentence-transformers/all-mpnet-base-v2",
                                                 model_kwargs={'device': "cpu"},
                                                 encode_kwargs={"batch_size": 16384})
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
            future_to_doc = {executor.submit(process_single_document, doc, no_of_batches): doc for doc in doc_chunk}

            for future in future_to_doc:
                doc = future_to_doc[future]
                try:
                    result = future.result(timeout=60)
                    if result:
                        not_skip_count += 1
                        nodes.extend(result)
                        with open("/home/deeepakb/Projects/bedrockTest/log.txt", "a") as f:
                            f.write(f"Finished: {doc.metadata.get('file_path')}\n")

                    else:
                        skip_count += 1
                    processed_docs += 1

                except concurrent.futures.TimeoutError:
                    logger.warning(f"Timeout error processing file: {doc.metadata.get('file_path')}")
                    skip_count += 1
                    with open("/home/deeepakb/Projects/bedrockTest/log.txt", "w") as f:
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

    # Initialize FAISS vector store
    embeddings = HuggingFaceEmbeddings(model_name="sentence-transformers/all-mpnet-base-v2",
                                       model_kwargs={'device': "cpu"},
                                       encode_kwargs={"batch_size": 131072})
    
    dimension = len(embeddings.embed_query("hello world"))
    index = faiss.IndexHNSWFlat(dimension, 32)
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
    vector_store.save_local("/home/deeepakb/Projects/bedrockTest/faiss_index_final")

if __name__ == "__main__":
    try:
        ingest()
    finally:
        listener.stop()
