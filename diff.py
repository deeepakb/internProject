import os
import difflib

def get_file_list(folder):
    return{ f for f in os.listdir(folder) if os.path.isfile(os.path.join(folder,f))}

def read_file(file_path):
    with open(file_path, 'r') as file:
        return file.readlines()
    
def generate_diff(file1, file2):
    d = difflib.ndiff(file1, file2)
    modified_lines = [line for line in d if line.startswith('- ') or line.startswith('+ ')]
    return '\n'.join(modified_lines)

def compare_folders(main_folder, working_folder):
    main_files = get_file_list(main_folder)
    working_files = get_file_list(working_folder)

    for file in working_files:
        if file in main_files:
            main_file_path = os.path.join(main_folder, file)
            working_file_path = os.path.join(working_folder, file)

            main_file_content = read_file(main_file_path)
            working_file_content = read_file(working_file_path)

            diff_result = generate_diff(main_file_content, working_file_content)
            print(f"Diff for {file}:\n{diff_result}\n")
            open("diff.txt", 'w').close()
            f = open("diff.txt", "a")
            f.write(f"Diff for {file}:\n{diff_result}\n")
            f.close()

        else:
            f = open("diff.txt", "a")
            f.write((f"File {file} not present in main folder\n"))
            f.close()

if __name__ == "__main__":
    compare_folders('/home/deeepakb/Projects/bedrockTest/mainRepo', '/home/deeepakb/Projects/bedrockTest/dummyCode')