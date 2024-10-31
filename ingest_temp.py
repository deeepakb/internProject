import boto3
import json
import os
import gc
import logging
import threading
from langchain_huggingface import HuggingFaceEmbeddings
import time
import faiss
from langchain_community.docstore.in_memory import InMemoryDocstore
from langchain_community.vectorstores import FAISS
from llama_index.core import SimpleDirectoryReader
from llama_index.core import VectorStoreIndex
from llama_index.core import Settings
from llama_index.core import StorageContext, load_index_from_storage
from llama_index.embeddings.huggingface import HuggingFaceEmbedding
from llama_index.core.node_parser import CodeSplitter, SentenceSplitter
import uuid
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
    reader = SimpleDirectoryReader(input_dir="/home/deeepakb/Projects/bedrockTest/dummyCode", recursive=True)
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
                    result = future.result(timeout=60)  # 1-minute timeout for each document
                    if result:
                        for node in result:
                            # Add file path and chunk ID to the node's metadata
                            node.metadata['file_path'] = node.metadata['file_path']
                            node.metadata['chunk_id'] = node.id_                          
                            nodes.append(node)  
                        not_skip_count += 1
                    else:
                        skip_count += 1
                    processed_docs += 1

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
    embeddings = HuggingFaceEmbeddings(model_name="sentence-transformers/all-mpnet-base-v2")

    index = faiss.IndexFlatL2(len(embeddings.embed_query("hello world")))

    vector_store = FAISS(
    embedding_function=embeddings,
    index=index,
    docstore=InMemoryDocstore(),
    index_to_docstore_id={},
    )

    logger.info("Finished making Vector Store")
    texts = []
    metadatas = []
    ids = []
    
    for node in nodes:
        texts.append(node.text)
        metadatas.append(node.metadata)
        ids.append(node.id_)
    
    # Add documents in batch
    vector_store.add_texts(texts=texts, metadatas=metadatas, ids=ids)

    logger.info(f"Added {len(nodes)} documents to the vector store.")

    # Optionally, save the vector store
    vector_store.save_local("/home/deeepakb/Projects/bedrockTest/faiss_index_final")




if __name__ == "__main__":
    ingest()
