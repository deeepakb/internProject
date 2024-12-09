import os
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_community.vectorstores import FAISS
from langchain.schema import Document
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

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

def is_supported_file(file_path):
    _, ext = os.path.splitext(file_path)
    return ext in file_type_to_language

def get_all_codebase_files(codebase_path):
    logger.info(f"Scanning codebase directory: {codebase_path}")
    all_files = []
    for root, _, files in os.walk(codebase_path):
        for file in files:
            full_path = os.path.join(root, file)
            if is_supported_file(full_path):
                all_files.append(os.path.normpath(full_path))
    return all_files

def read_file_content(file_path):
    """Reads the content of a file."""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return f.read()
    except Exception as e:
        logger.error(f"Error reading file {file_path}: {e}")
        return None

def check_vector_store_coverage(vector_store, codebase_files):
    logger.info("Checking vector store coverage...")
    covered_files = set()
    missing_files = []

    with open("/home/deeepakb/Projects/bedrockTest/text/covered_files.txt", "a") as covered_file, open("/home/deeepakb/Projects/bedrockTest/text/missing_files.txt", "a") as missing_file:
        for idx, file_path in enumerate(codebase_files, start=1):
            # Perform similarity search with the file name/path as the query
            results = vector_store.similarity_search(file_path, k=25)

            file_snippet_found = False
            for result in results:
                retrieved_snippet = result.page_content.strip()
                snippet_metadata_path = result.metadata.get('file_path', None)

                # If the snippet's metadata path matches the file path, it's considered "covered"
                if snippet_metadata_path == file_path:
                    covered_files.add(file_path)
                    covered_file.write(file_path + "\n")
                    file_snippet_found = True
                    break  # Skip further snippets for this file

            if not file_snippet_found:
                missing_files.append(file_path)
                missing_file.write(file_path + "\n")

            # Log progress every 100 files
            if idx % 100 == 0:
                logger.info(f"Processed {idx}/{len(codebase_files)} files...")

    logger.info("Completed vector store coverage check.")
    return covered_files, missing_files

def main():
    codebase_path = "/home/deeepakb/redshift-padb"
    index_path = "/home/deeepakb/Projects/bedrockTest/faiss_index_final_improved_4"

    # Load FAISS index
    embeddings = HuggingFaceEmbeddings(model_name="sentence-transformers/all-mpnet-base-v2", 
                                       model_kwargs={'device': "cpu"})
    try:
        vector_store = FAISS.load_local(index_path, embeddings, allow_dangerous_deserialization=True)
        logger.info("FAISS index loaded successfully.")
    except Exception as e:
        logger.error(f"Error loading FAISS index: {e}")
        return

    # Scan the codebase for supported files
    codebase_files = get_all_codebase_files(codebase_path)
    logger.info(f"Total supported files in codebase: {len(codebase_files)}")

    # Check vector store coverage
    covered_files, missing_files = check_vector_store_coverage(vector_store, codebase_files)
    logger.info(f"Covered files: {len(covered_files)}")
    logger.info(f"Missing files: {len(missing_files)}")

    logger.info("Results saved to covered_files.txt and missing_files.txt")

if __name__ == "__main__":
    main()
