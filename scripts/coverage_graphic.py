import os
import matplotlib.pyplot as plt

def read_file(file_path):
    """Reads the content of a file and returns a list of lines."""
    try:
        with open(file_path, "r") as file:
            lines = [line.strip() for line in file.readlines()]
            print(f"Read {len(lines)} lines from {file_path}")  # Debugging line
            return lines
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
        return []

def plot_coverage(covered_files, missing_files):
    """Generates a pie chart for codebase coverage."""
    total_files = len(covered_files) + len(missing_files)
    print(f"Total files: {total_files}")  # Debugging line
    covered_percentage = len(covered_files) / total_files * 100
    missing_percentage = len(missing_files) / total_files * 100
    
    # Data for the pie chart
    labels = ['Covered', 'Missing']
    sizes = [covered_percentage, missing_percentage]
    colors = ['#4CAF50', '#FF6347']  # Green for covered, red for missing
    
    # Plotting the pie chart
    fig, ax = plt.subplots()
    ax.pie(sizes, labels=labels, autopct='%1.1f%%', colors=colors, startangle=90, wedgeprops={'edgecolor': 'black'})
    ax.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
    
    # Adding a title
    plt.title(f"Codebase Coverage: {len(covered_files)} Covered / {len(missing_files)} Missing Files")
    
    # Save the plot as a file
    plt.savefig('codebase_coverage.png')
    print("Plot saved as 'codebase_coverage.png'")  # Debugging line

def main():
    print("Starting the coverage analysis...")  # Debugging line
    
    # File paths
    covered_files_path = "/home/deeepakb/Projects/bedrockTest/text/covered_files.txt"
    missing_files_path = "/home/deeepakb/Projects/bedrockTest/text/missing_files.txt"
    
    # Read the content of the files
    covered_files = read_file(covered_files_path)
    missing_files = read_file(missing_files_path)
    
    # Plot the coverage
    plot_coverage(covered_files, missing_files)

if __name__ == "__main__":
    main()
