import os

from utils.mock_7z_files import get_mock_7z_filenames
from twb import TemporalWikiBlocks


def test_build_preload():
    # Test that nothing is raised.
    twb = TemporalWikiBlocks()
    twb.preload('./test/sample_data/minimal_sample.xml')


def test_build():
    original_file = './test/sample_data/minimal_sample.xml'
    compressed_files = get_mock_7z_filenames(original_file_path=original_file, num_file=10)
    mock_data_dir = os.path.join(os.path.split(original_file)[0], 'temp')

    for compressed_file in compressed_files:
        assert os.path.exists(compressed_file)

    twb = TemporalWikiBlocks()
    twb.preload(mock_data_dir)
    twb.build('./test/output')
