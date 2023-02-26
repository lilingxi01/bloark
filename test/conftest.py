import pytest
import os
import py7zr
from utils.mock_preload_files import create_testing_dir, delete_testing_dir
from utils.mock_7z_files import get_mock_7z_filenames, get_mock_7z_temporary_dir


@pytest.fixture(scope="session", autouse=True)
def mock_testing_dir():
    # Before the test
    create_testing_dir()

    yield

    # After the test
    delete_testing_dir()


@pytest.fixture(scope="session", autouse=True)
def generate_mock_7z_files():
    original_file = './test/sample_data/minimal_sample.xml'
    temporary_dir = get_mock_7z_temporary_dir(original_file_path=original_file)

    # If temporary sample data directory does not exist, create it.
    if not os.path.exists(temporary_dir):
        os.mkdir(temporary_dir)

    # If the mock 7z files do not exist, create them.
    for file_name in get_mock_7z_filenames(original_file_path=original_file, num_file=10):
        if os.path.exists(file_name):
            continue
        with py7zr.SevenZipFile(file_name, 'w') as archive:
            archive.write(original_file, 'minimal_sample.xml')

    yield
