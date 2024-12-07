import os
import faiss
import logging
import time
import boto3
from langchain_community.vectorstores import FAISS
from langchain_huggingface import HuggingFaceEmbeddings
from collections import deque
import queue
from logging.handlers import QueueHandler, QueueListener  # Ensure this import is included

# Define file type to language mapping
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

# Set up logging
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

# Function to check coverage based on file names
def check_coverage(vector_store, codebase_directory):
    covered_files = set()
    missing_files = set()
    total_files = 0
    
    # Iterate over files in the codebase directory
    logger.info(f"Starting coverage check for codebase directory: {codebase_directory}")
    for root, dirs, files in os.walk(codebase_directory):
        total_files += len(files)
        for file in files:
            file_path = os.path.join(root, file)
            file_name = os.path.basename(file_path)
            file_extension = os.path.splitext(file_name)[1]
            
            # Only process files with the allowed extensions
            if file_extension in file_type_to_language:
                logger.debug(f"Processing file: {file_name}")
                # Perform similarity search using file name as the query
                query_result = vector_store.similarity_search(file_name, k=10)  # Adjust k as needed
                
                # Check if any retrieved snippets correspond to the current file
                found_match = any(file_name in result.metadata.get("file_path", "") for result in query_result)
                
                if found_match:
                    covered_files.add(file_name)
                    logger.debug(f"Found match for file: {file_name}")
                else:
                    missing_files.add(file_name)
                    logger.debug(f"No match found for file: {file_name}")
                
                # Log progress after processing each file
                logger.info(f"Processed {len(covered_files) + len(missing_files)} out of {total_files} files.")
                
                # Incrementally write to the text files
                with open('covered_files.txt', 'a') as f:
                    if file_name in covered_files:
                        f.write(f"{file_name}\n")
                with open('missing_files.txt', 'a') as f:
                    if file_name in missing_files:
                        f.write(f"{file_name}\n")
    
    logger.info(f"Coverage check complete. Covered: {len(covered_files)}, Missing: {len(missing_files)}")
    return covered_files, missing_files


# Function to visualize coverage using matplotlib (or other visualization tools)
def visualize_coverage(covered_files, missing_files):
    import matplotlib.pyplot as plt

    # Coverage calculation
    total_files = len(covered_files) + len(missing_files)
    covered_percentage = len(covered_files) / total_files * 100
    missing_percentage = len(missing_files) / total_files * 100

    # Plotting the results
    logger.info("Visualizing coverage...")
    plt.bar(['Covered', 'Missing'], [covered_percentage, missing_percentage], color=['green', 'red'])
    plt.title(f"Codebase Coverage\nCovered: {covered_percentage:.2f}% | Missing: {missing_percentage:.2f}%")
    plt.ylabel('Percentage')
    plt.show()
    logger.info("Coverage visualization complete.")


# Main function to run coverage check
def main():
    # Set the directory where the codebase is stored
    codebase_directory = '/home/deeepakb/redshift-padb'
    
    # Load the vector store
    index_path = "/home/deeepakb/Projects/bedrockTest/faiss_index_file_names"
    logger.info(f"Loading vector store from: {index_path}")
    embeddings = HuggingFaceEmbeddings(model_name="sentence-transformers/all-mpnet-base-v2", model_kwargs={'device': "cpu"})
    vector_store = FAISS.load_local(index_path, embeddings, allow_dangerous_deserialization=True)
    logger.info("Vector store loaded successfully.")
    
    # Perform the coverage check
    logger.info("Starting coverage check...")
    covered_files, missing_files = check_coverage(vector_store, codebase_directory)
    
    # Visualize the coverage
    visualize_coverage(covered_files, missing_files)

    # Log the results
    logger.info(f"Coverage Check Complete. Covered: {len(covered_files)} files, Missing: {len(missing_files)} files.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        listener.stop()
