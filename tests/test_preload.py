import pytest

from .utils.mock_preload_files import get_testing_files, testing_dir_path
import bloark


def test_preload_empty_path():
    builder = bloark.Builder(output_dir='./tests/output')
    with pytest.raises(ValueError):
        builder.preload('')

    reader = bloark.Reader(output_dir='./tests/output')
    with pytest.raises(ValueError):
        reader.preload('')


def test_preload_not_exist_path():
    builder = bloark.Builder(output_dir='./tests/output')
    with pytest.raises(FileNotFoundError):
        builder.preload('./not_exist_path')

    reader = bloark.Reader(output_dir='./tests/output')
    with pytest.raises(FileNotFoundError):
        reader.preload('./not_exist_path')


def test_preload_file_path():
    builder = bloark.Builder(output_dir='./tests/output')
    builder.preload(testing_dir_path)

    reader = bloark.Reader(output_dir='./tests/output')
    reader.preload(testing_dir_path)

    testing_files = get_testing_files()

    # Check if the files are loaded correctly.
    assert len(builder.files) == len(testing_files)
    assert len(reader.files) == len(testing_files)

    # Check if the files are all loaded.
    for file in testing_files:
        assert builder.files.index(file) != -1
        assert reader.files.index(file) != -1
