import os
from typing import List

from . import test_file_count, get_mock_zst_temporary_dir


def get_mock_zst_filenames() -> List[str]:
    target_dir = get_mock_zst_temporary_dir()
    return [os.path.join(target_dir, f'block_{str(i).zfill(8)}.jsonl.zst') for i in range(test_file_count)]
