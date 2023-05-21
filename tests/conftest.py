import shutil

import pytest
import os
import py7zr
from multiprocessing import Pool

from utils.mock_zst_files import get_mock_zst_filenames
from utils.mock_preload_files import create_testing_dir, delete_testing_dir
from utils.mock_7z_files import get_mock_7z_filenames, get_mock_multiple_article_7z_filename


@pytest.fixture(scope="session", autouse=True)
def mock_testing_dir():
    # Before the test
    create_testing_dir()

    yield

    # After the test
    delete_testing_dir()


@pytest.fixture(scope="session", autouse=True)
def generate_mock_7z_files():
    # Generate mock 7z files in parallel.
    # Do NOT run it in cluster mode because it will consume all processes, which might exceed the quota.
    with Pool(processes=None) as p:
        p.map(_single_article_7z_generator, get_mock_7z_filenames())

    _multi_article_7z_generator(get_mock_multiple_article_7z_filename())

    yield


# Helper function for generating single-article 7z files.
def _single_article_7z_generator(file_name: str):
    original_file = './tests/sample_data/minimal_sample.xml'
    if os.path.exists(file_name):
        return
    with py7zr.SevenZipFile(file_name, 'w') as archive:
        archive.write(original_file, 'minimal_sample.xml')


# Helper function for generating multi-article 7z files.
def _multi_article_7z_generator(file_name: str):
    original_file = './tests/sample_data/multi_article_sample.xml'
    if os.path.exists(file_name):
        return
    with py7zr.SevenZipFile(file_name, 'w') as archive:
        archive.write(original_file, 'multi_article_sample.xml')


@pytest.fixture(scope="session", autouse=True)
def generate_mock_zst_files():
    original_file = './tests/sample_data/sample_block.jsonl.zst'

    for file_name in get_mock_zst_filenames():
        if os.path.exists(file_name):
            continue
        shutil.copyfile(original_file, file_name)

    yield
