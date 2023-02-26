import os
import shutil

from utils.mock_7z_files import get_mock_7z_filenames
from twb import TemporalWikiBlocks


def test_build_preload():
    # Test that nothing is raised.
    twb = TemporalWikiBlocks()
    twb.preload('./test/sample_data/minimal_sample.xml')


def test_build_no_compress():
    test_file_count = 10

    twb = TemporalWikiBlocks()
    twb.preload(get_mock_data_dir(test_file_count=test_file_count))
    twb.build('./test/output', num_proc=4, compress=False)

    for i in range(test_file_count):
        assert os.path.exists(f'./test/output/block_{str(i).zfill(5)}.jsonl')

    shutil.rmtree('./test/output')


def test_build_compressed():
    test_file_count = 10

    twb = TemporalWikiBlocks()
    twb.preload(get_mock_data_dir(test_file_count=test_file_count))
    twb.build('./test/output', num_proc=4, compress=True)

    for i in range(test_file_count):
        assert os.path.exists(f'./test/output/block_{str(i).zfill(5)}.jsonl.zst')

    shutil.rmtree('./test/output')


def get_mock_data_dir(test_file_count: int) -> str:
    """
    Helper function: get the mock data directory.
    :param test_file_count: the number of files to create
    :return: the mock data directory
    """
    original_file = './test/sample_data/minimal_sample.xml'
    compressed_files = get_mock_7z_filenames(original_file_path=original_file, num_file=test_file_count)
    mock_data_dir = os.path.join(os.path.split(original_file)[0], 'temp')

    for compressed_file in compressed_files:
        assert os.path.exists(compressed_file)

    return mock_data_dir
