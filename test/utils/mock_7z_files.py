import os
from typing import List


def get_mock_7z_filenames(original_file_path: str, num_file: int = 10) -> List[str]:
    original_dir, original_file = os.path.split(original_file_path)
    target_dir = os.path.join(original_dir, 'temp')
    target_file_path = os.path.join(target_dir, original_file)
    return [target_file_path + f'.{i}.7z' for i in range(num_file)]


def get_mock_7z_temporary_dir(original_file_path: str) -> str:
    original_dir, _ = os.path.split(original_file_path)
    return os.path.join(original_dir, 'temp')
