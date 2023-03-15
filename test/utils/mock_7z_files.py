import os
from typing import List

from . import test_file_count, get_mock_7z_temporary_dir


def get_mock_7z_filenames() -> List[str]:
    target_dir = get_mock_7z_temporary_dir()
    return [os.path.join(target_dir, f'minimal_sample_{str(i).zfill(8)}.xml.7z') for i in range(test_file_count)]


def get_mock_multiple_article_7z_filename() -> str:
    return os.path.join(get_mock_7z_temporary_dir(), 'multi_article_sample.xml.7z')
