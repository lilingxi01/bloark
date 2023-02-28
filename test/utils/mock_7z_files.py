import os
from typing import List

from . import test_file_count, get_mock_7z_temporary_dir


def get_mock_7z_filenames() -> List[str]:
    target_dir = get_mock_7z_temporary_dir()
    return [os.path.join(target_dir, f'minimal_sample_{str(i).zfill(5)}.xml.7z') for i in range(test_file_count)]
