import faiss
import boto3
import json
import os
import logging
import threading
import numpy as np
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

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

# Not supported -> sql, html, FFFFFFFF files break it
file_type_to_language = {
    ".cpp": "cpp",
    ".hpp": "cpp",
    ".go": "go",
    ".sql": "text",
    ".java": "java",
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
    ".conf": "text",
    ".data": "text",
    ".component": "text",
}

def process_single_document(doc):
    try:
        file_name, file_type = os.path.splitext(doc.metadata.get("file_path"))
        logger.info(f"Processing file: {doc.metadata.get('file_path')}")
        file_type = file_type_to_language.get(file_type.lower(), None)
        if file_type is None:
            logger.warning(f"Unknown file type for: {file_name}.{file_type}")
            return []

        if file_type == "text":
            logger.info(f"Text file: {file_name}.{file_type}")
            text_splitter = SentenceSplitter(chunk_size=65536, chunk_overlap=0)
            return text_splitter.get_nodes_from_documents([doc])
        else:
            logger.info(f"Code file: {file_name}.{file_type}")
            code_splitter = CodeSplitter(language=file_type, chunk_lines=65536)
            return code_splitter.get_nodes_from_documents([doc])
    except Exception as e:
        logger.error(f"Error processing file: {doc.metadata.get('file_path')}, Error: {e}")
        return []

def ingest():
    multiprocessing.set_start_method('spawn')
    s3 = boto3.client('s3')
    Settings.embed_model = HuggingFaceEmbedding()
    Settings.llm = None  
    reader = SimpleDirectoryReader(input_dir="/home/deeepakb/redshift-padb", recursive=True)
    documents = reader.load_data()
    nodes = []
    documents = documents[:30000]
    total_docs = len(documents)
    completed_docs = 0  # Counter for successfully processed documents
    logger.info(f"Total documents to process: {total_docs}")

    with concurrent.futures.ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
        future_to_doc = {executor.submit(process_single_document, doc): doc for doc in documents}
        
        for future in concurrent.futures.as_completed(future_to_doc):
            doc = future_to_doc[future]
            try:
                result = future.result()
                nodes.extend(result)
                completed_docs += 1  # Increment the counter
                logger.info(f"Successfully processed: ({completed_docs}/{total_docs})")
            except Exception as e:
                logger.error(f"Error processing file: {doc.metadata.get('file_path')}, Error: {e}")

    # Create a Faiss index
    if nodes:
        embeddings = np.array([node.embedding for node in nodes]).astype('float32')
        index = faiss.IndexFlatL2(embeddings.shape[1])  # or another index type
        index.add(embeddings)

        # Save the Faiss index
        faiss.write_index(index, "faiss_index.bin")
        logger.info("Faiss index created and saved.")
    else:
        logger.warning("No nodes were processed, Faiss index will not be created.")

    # Upload index files to S3
    # (Code to upload to S3 remains unchanged)

if __name__ == "__main__":
    ingest()
