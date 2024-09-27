import os
import shutil

def remove_files_in_folders(root_dir, target_folders):
    # Traverse the directory tree
    for dirpath, dirnames, filenames in os.walk(root_dir):
        # Check if the current directory is one of the target folders
        if os.path.basename(dirpath) in target_folders:
            print(f"Entering folder: {dirpath}")
            for filename in filenames:
                file_path = os.path.join(dirpath, filename)
                try:
                    os.remove(file_path)
                    print(f"Removed file: {file_path}")
                except Exception as e:
                    print(f"Error removing file {file_path}: {e}")

# Define the root directory to start from and target folders to clear
root_directory = "."
target_folders = ["input", "output"]

# Call the function
remove_files_in_folders(root_directory, target_folders)