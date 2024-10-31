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

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

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
}

def file_size_is_within_limit(file_path, max_size=10 * 1024 * 1024):  # 10 MB in bytes
    return os.path.getsize(file_path) <= max_size


def process_single_document(doc, no_of_batches):
    try:
        file_name, file_type = os.path.splitext(doc.metadata.get("file_path"))
        logger.info(f"Processing File: {doc.metadata.get('file_path')}")
        file_type = file_type_to_language.get(file_type.lower(), None)

        if not file_size_is_within_limit(doc.metadata.get('file_path')):
            logger.warning(f"Skipping large file: {doc.metadata.get('file_path')}")
            with open("/home/deeepakb/Projects/bedrockTest/log.txt", "a") as f:
                f.write(f"Skipping large file: {doc.metadata.get('file_path')}\n")
            return []

        if file_type is None:
            logger.warning(f"Unknown file type")
            return []

        if file_type == "text":
            text_splitter = SentenceSplitter(chunk_size=32768, chunk_overlap=0)
            logger.info(f"Processed as Text")
            print(len(text_splitter.get_nodes_from_documents([doc])))
            return text_splitter.get_nodes_from_documents([doc])
        else:   
            logger.info(f"Processed as Code")
            code_splitter = CodeSplitter(language=file_type, chunk_lines=32768)
            print(len(code_splitter.get_nodes_from_documents([doc])))
            return code_splitter.get_nodes_from_documents([doc])
    except Exception as e:
        logger.error(f"Error processing file: {doc.metadata.get('file_path')}")
        logger.exception(e)
        return []


def ingest(): 
    no_of_batches = 0
    os.environ["PYTHONMALLOC"] = "malloc"  
    multiprocessing.set_start_method('spawn')  
    s3 = boto3.client('s3')
    Settings.embed_model = HuggingFaceEmbedding(
    model_name="BAAI/bge-small-en-v1.5",
    embed_batch_size = 2048
    )    
    Settings.llm = None  
    reader = SimpleDirectoryReader(input_dir="/home/deeepakb/redshift-padb", recursive=True)
    documents = reader.load_data()
    chunk_size = 25000  # Adjust the batch size as needed
    total_docs = len(documents)
    processed_docs = 0
    skip_count = 0
    not_skip_count = 0
    nodes = []
    batch_count = 0  
    # Split documents into chunks
    for i in range(0, total_docs, chunk_size):
        doc_chunk = documents[i:i + chunk_size]
        
        with concurrent.futures.ProcessPoolExecutor(max_workers= os.cpu_count()) as executor:
            future_to_doc = {executor.submit(process_single_document, doc, no_of_batches): doc for doc in doc_chunk}

            # Process futures with a manual timeout
            for future in future_to_doc:
                doc = future_to_doc[future]
                try:
                    # Check for the result with a timeout
                    result = future.result(timeout=60)  # 3-minute timeout for each document
                    nodes.extend(result)
                    processed_docs += 1
                    if result:
                        not_skip_count += 1
                    else:
                        skip_count += 1
                except concurrent.futures.TimeoutError:
                    logger.warning(f"Timeout error processing file: {doc.metadata.get('file_path')}")
                    skip_count += 1
                    with open("/home/deeepakb/Projects/bedrockTest/log.txt", "a") as f:
                        f.write(f"Timeout error processing file: {doc.metadata.get('file_path')}\n")
                    # Optionally, you can cancel the future if it's still running
                    future.cancel()
                except Exception as e:
                    logger.error(f"Error processing file: {doc.metadata.get('file_path')}")
                    logger.exception(e)
                    skip_count += 1

                # Print progress for the current chunk
                logger.info(f"Processed {processed_docs} of {total_docs} documents")
                
        gc.collect()
        time.sleep(0.5)
        batch_count += 1  # Increment the batch counter


    logger.info(f"Finished with skipped: {skip_count}, and not skipped: {not_skip_count}")
    # Index Store Faiss/Pinecone
    index = VectorStoreIndex(list(nodes), show_progress=True)
    index.storage_context.persist(persist_dir="/home/deeepakb/Projects/bedrockTest/indexStorage")
    storage_context = StorageContext.from_defaults(persist_dir="/home/deeepakb/Projects/bedrockTest/indexStorage")

    for root, dirs, files in os.walk('./home/deeepakb/Projects/bedrockTest/tempIndexStorage'):
        for file in files:
            s3.upload_file(os.path.join(root, file), Bucket='holdingvectorstore', Key=os.path.relpath(os.path.join(root, file), './indexStorage'))

    objects = s3.list_objects_v2(Bucket='holdingvectorstore')
    for obj in objects['Contents']:
        s3_key = obj['Key']
        s3.download_file('holdingvectorstore', s3_key, f'./fromS3/{os.path.basename(s3_key)}')
    logger.info("Finished Download")   


if __name__ == "__main__":
    ingest()
