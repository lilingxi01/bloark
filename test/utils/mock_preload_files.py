import os
import random
import string
import shutil

testing_dir_path = "./testing_dir"


global testing_files


def create_testing_dir():
    """
    Create a testing directory with some mock files and folders.
    :return: the list of file paths (including the files in the folders)
    """

    if os.path.exists(testing_dir_path):
        shutil.rmtree(testing_dir_path)  # Delete the testing_dir directory if it already exists.

    # Create the testing_dir directory
    os.mkdir(testing_dir_path)

    files = []
    # Create some files directly in the testing_dir directory
    for i in range(3):
        filename = f"file_{i}.txt"
        filepath = os.path.join(testing_dir_path, filename)
        files.append(filepath)
        with open(filepath, "w") as f:
            f.write("Hello, world!")

    # Create some folders with files in them
    for i in range(2):
        folder_name = f"folder_{i}"
        folder_path = os.path.join(testing_dir_path, folder_name)
        os.mkdir(folder_path)

        for j in range(2):
            filename = "".join(random.choice(string.ascii_letters) for _ in range(10)) + ".txt"
            filepath = os.path.join(folder_path, filename)
            with open(filepath, "w") as f:
                f.write("Hello, world!")
            files.append(filepath)

    global testing_files
    testing_files = files

    return files


def delete_testing_dir():
    """
    Delete the testing directory.
    """
    shutil.rmtree(testing_dir_path)


def get_testing_files():
    """
    Get the list of file paths in the testing directory.
    :return: the list of file paths
    """
    global testing_files
    return testing_files
