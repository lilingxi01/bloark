import pytest

from utils.mock_preload_files import get_testing_files, testing_dir_path
from twb import TemporalWikiBlocks


def test_preload_empty_path():
    twb = TemporalWikiBlocks()
    with pytest.raises(ValueError):
        twb.preload('')


def test_preload_not_exist_path():
    twb = TemporalWikiBlocks()
    with pytest.raises(FileNotFoundError):
        twb.preload('./not_exist_path')


def test_preload_file_path():
    twb = TemporalWikiBlocks()
    twb.preload(testing_dir_path)

    testing_files = get_testing_files()

    # Check if the files are loaded correctly.
    assert len(twb.files) == len(testing_files)

    # Check if the files are all loaded.
    for file in testing_files:
        assert twb.files.index(file) != -1
