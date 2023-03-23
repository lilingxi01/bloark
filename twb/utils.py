import os
from typing import List
import zstandard as zstd
import py7zr
import shutil
import psutil


def get_file_list(input_path: str) -> List[str]:
    """
    Get the list of files in the input directory.
    :param input_path: the input directory
    :raise FileNotFoundError: if the path does not exist
    :return: the list of files
    """
    # If the path does not exist, raise an error.
    if not os.path.exists(input_path):
        raise FileNotFoundError('The path does not exist.')

    # If the input path is a file, return the list with only the file path.
    if os.path.isfile(input_path):
        return [input_path]

    # If the input path is a directory, return the list of files in the directory.
    all_files = []
    for root, directories, files in os.walk(input_path):
        for file in files:
            file_path = os.path.join(root, file)
            all_files.append(file_path)

    # Remove duplicate files.
    all_files = sorted(list(set(all_files)))
    return all_files


def get_decompress_output_path(input_path: str, output_dir: str):
    """
    Get the output path of the decompressed file.
    :param input_path: the input path
    :param output_dir: the output directory
    :return: the output path
    """
    # Split the path into its components
    _, file_name = os.path.split(input_path)

    # Might want to reconsider the output file structure.
    if not file_name.endswith('.7z'):
        return os.path.join(output_dir, file_name)

    decompressed_file_name = file_name.rstrip('.7z')

    # Add the new directory to the beginning of the path
    return os.path.join(output_dir, decompressed_file_name)


def compress_zstd(input_path: str, output_path: str):
    """
    Compress the blocks into a Zstandard file.
    :param input_path: the input path
    :param output_path: the output path
    """
    # Compress the blocks.
    compressor = zstd.ZstdCompressor()
    with open(input_path, "rb") as ifh, open(output_path, "wb") as ofh:
        compressor.copy_stream(ifh, ofh)


def decompress_zstd(input_path: str, output_path: str):
    """
    Decompress the blocks from a Zstandard file.
    :param input_path: the input path
    :param output_path: the output path
    """
    # Decompress the blocks.
    decompressor = zstd.ZstdDecompressor()
    with open(input_path, "rb") as ifh, open(output_path, "wb") as ofh:
        decompressor.copy_stream(ifh, ofh)


def compute_total_available_space(output_dir: str) -> int:
    """
    Deprecated: Compute the total available space in the output directory.
    """
    total_available_space = shutil.disk_usage(output_dir).free

    # Display the total available space in GB.
    total_available_space_gb = total_available_space / 1024 / 1024 / 1024
    print('[Build] RDS space limitation (deprecated):', round(total_available_space_gb, 2), 'GB.')

    return total_available_space


def get_estimated_size(path: str) -> int:
    if path.endswith('.7z'):
        with py7zr.SevenZipFile(path, 'r') as z:
            space = z.archiveinfo().uncompressed
        return space * 2
    elif path.endswith('.zst'):
        with open(path, 'rb') as f:
            # Get the frame information for the compressed file
            space = zstd.frame_content_size(f.read(18))
            if space <= 0:
                return os.path.getsize(path) * 2
            else:
                return space * 2
    else:
        return os.path.getsize(path) * 2


def get_memory_consumption() -> int:
    process = psutil.Process(os.getpid())
    memory_usage_mb = process.memory_info().rss / 1024 / 1024
    return round(memory_usage_mb, 3)


def cleanup_dir(path: str):
    """
    Clean up the directory.
    :param path: the directory path
    """
    if os.path.exists(path):
        shutil.rmtree(path, ignore_errors=True)
