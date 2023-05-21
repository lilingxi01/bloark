import os
import shutil
from typing import Tuple, List

import twb
from utils import get_mock_zst_temporary_dir
from utils.mock_zst_files import get_mock_zst_filenames


def test_reader_decompress():
    temporary_dir, test_filenames = get_mock_data_dir()

    reader = twb.Reader(num_proc=4)
    reader.preload(temporary_dir)

    reader.decompress(output_dir='./tests/output')

    # Test that the decompressed files exist.
    for test_filename in test_filenames:
        assert os.path.exists(os.path.join('./tests/output', os.path.split(test_filename)[1][:-4]))

    # TODO: Verify the decompressed file content.

    shutil.rmtree('./tests/output')


def get_mock_data_dir() -> Tuple[str, List[str]]:
    """
    Helper function: get the mock data directory.
    :return: the mock data directory
    """
    temporary_dir = get_mock_zst_temporary_dir()
    test_filenames = get_mock_zst_filenames()
    return temporary_dir, test_filenames
