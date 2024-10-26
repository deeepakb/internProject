import subprocess
import sys
import os

repo_path = "/home/deeepakb/padb/"
remote_name = 'gerrit'
branch_name = 'main'


def get_diff(remote_name, branch_name):
    result = subprocess.run(['git', 'diff'], check=True, text=True, capture_output=True, cwd='/home/deeepakb/padb', bufsize=10240)
    print(result.stdout)
    return result.stdout

def diff_command():
    diff = get_diff(remote_name, branch_name)
    print(diff)
    print(len(diff))

    try:
        with open('lmao.txt', 'w') as f:
            f.write(diff)
            f.flush()
            os.fsync(f.fileno())
    except (IOError, OSError) as e:
        print(f"Error writing to diff.txt: {e}")


if __name__ == "__main__":
    diff_command()