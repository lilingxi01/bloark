import os


original_dir = './test/sample_data'
test_file_count = 50


def _get_mock_temporary_dir(sub_dir: str) -> str:
    parent_dir = os.path.join(original_dir, 'temp')
    if not os.path.exists(parent_dir):
        os.mkdir(parent_dir)
    temporary_dir = os.path.join(original_dir, 'temp', sub_dir)
    if not os.path.exists(temporary_dir):
        os.mkdir(temporary_dir)
    return temporary_dir


def get_mock_7z_temporary_dir() -> str:
    return _get_mock_temporary_dir(sub_dir='builder')


def get_mock_zst_temporary_dir() -> str:
    return _get_mock_temporary_dir(sub_dir='reader')
