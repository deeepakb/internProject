import boto3
import json
import os
import gc
import logging
import threading
import time
from llama_index.core import SimpleDirectoryReader
from llama_index.core import VectorStoreIndex
from llama_index.core import Settings
from llama_index.core import StorageContext, load_index_from_storage
from llama_index.embeddings.huggingface import HuggingFaceEmbedding
from llama_index.core.node_parser import CodeSplitter, SentenceSplitter
import fnmatch
import multiprocessing
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor
from collections import deque
import boto3
import json
import os
import gc
import logging
import threading
from langchain_huggingface import HuggingFaceEmbeddings
import time
from langchain.text_splitter import Language
from llama_index.core import SimpleDirectoryReader, Document
from llama_index.core import Settings
from llama_index.core import StorageContext, load_index_from_storage
from llama_index.embeddings.huggingface import HuggingFaceEmbedding
from llama_index.core.node_parser import CodeSplitter, SentenceSplitter
from llama_index.core import GPTTreeIndex
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

# Configure logging
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

# Not supported -> sql, html, FFFFFFFF files break it
file_type_to_language = {
    ".cpp": "cpp",
    ".hpp": "cpp",
    ".go": "go",
    ".sql": "text",
    ".java": "java",
    ".json" : "json",
    ".kt": "kotlin",
    ".ts": "ts",
    ".php": "php",
    ".py": "python",
    ".rst": "rst",
    ".sh": "text",  # check
    ".rb": "ruby",
    ".xml": "text",
    ".proto": "text",  # check
    ".rs": "rust",
    ".scala": "scala",
    ".yaml": "yaml",
    ".swift": "swift",
    ".md": "markdown",
    ".tex": "latex",
    ".sol": "sol",
    ".cob": "cobol",
    ".c": "c",
    ".csv": "text",
    ".lua": "lua",
    ".pl": "perl",
    ".cs": "c-sharp",
    ".hs": "haskell",
    ".html": "text",
    ".orig": "text",
    ".txt": "text",
    ".mk": "text",
    ".res": "text",
    ".ldf" : "text",
    ".conf": "text",
    ".data": "text",
    ".component": "text",
    ".dump" : "text",
    ".h" : "text",
    ".po" : "text",
    ".template" : "text",
    ".cmake" : "text", 
    ".cfg" : "text",
    "" : "text"
}

def file_size_is_within_limit(file_path, max_size=12 * 1024 * 1024):
    return os.path.getsize(file_path) <= max_size

def process_single_document(doc, i, no_of_batches):
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
            return None

        if file_type is not None and file_type.upper() in Language.__members__:
            language = Language[file_type.upper()]
            text_splitter = RecursiveCharacterTextSplitter.from_language(
                language=language, chunk_size=2048, chunk_overlap=100
            )
        elif file_type is not None:
            text_splitter = RecursiveCharacterTextSplitter(chunk_size=2048, chunk_overlap=100)
        else:
            with open("/home/deeepakb/Projects/bedrockTest/log.txt", "a") as f:
                f.write(f"Skipping Unknown file: {file_path}\n")
            return None

        content = doc.get_content()
        chunks = text_splitter.split_text(content)
        
        with open("/home/deeepakb/Projects/bedrockTest/log.txt", "a") as f:
            f.write(f"File Exists: {file_path}")

        return Document(
            text="\n\n".join([f"File Path: {file_path}\n\n{chunk}" for chunk in chunks]),
            metadata={"file_path": file_path, "id": i}
        )

    except Exception as e:
        logger.error(f"Error processing file: {file_path}")
        logger.exception(e)
        return None

def ingest():
    no_of_batches = 0
    os.environ["PYTHONMALLOC"] = "malloc"
    
    if multiprocessing.get_start_method() != 'spawn':
        try:
            multiprocessing.set_start_method('spawn')
        except RuntimeError:
            pass  # Method is already set, ignore the error

    s3 = boto3.client('s3')


    Settings.embed_model = HuggingFaceEmbedding(model_name="sentence-transformers/all-mpnet-base-v2")
    Settings.llm = None

    reader = SimpleDirectoryReader(input_dir="/home/deeepakb/Projects/bedrockTest/dummyCode", recursive=True)
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

            for future in concurrent.futures.as_completed(future_to_doc):
                doc = future_to_doc[future]
                try:
                    result = future.result(timeout=60)
                    if result:
                        not_skip_count += 1
                        nodes.append(result)
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
                except Exception as e:
                    logger.error(f"Error processing file: {doc.metadata.get('file_path')}")
                    logger.exception(e)
                    skip_count += 1

                logger.info(f"Processed {processed_docs} of {total_docs} documents")

        gc.collect()
        time.sleep(0.5)
        batch_count += 1

    logger.info(f"Finished processing with skipped: {skip_count}, and not skipped: {not_skip_count}")

    # Create GPTTreeIndex
    index = GPTTreeIndex.from_documents(nodes)

    # Save the index
    index.storage_context.persist(persist_dir="/home/deeepakb/Projects/bedrockTest/GPT_index")

    end_time = time.time()
    logger.info(f"Time taken to create and save index: {end_time - start_time} seconds")

if __name__ == "__main__":
    try:
        ingest()
    finally:
        listener.stop()


if __name__ == "__main__":
    ingest()
