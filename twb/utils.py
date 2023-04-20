import os
from typing import List, Callable, Union
import zstandard as zstd
import py7zr
import shutil
import psutil
from importlib.metadata import version, PackageNotFoundError
import multiprocessing as mp

from twb.logger import twb_logger


def get_curr_version():
    """
    Get the version of the package.
    """
    try:
        return version('twb-project')
    except PackageNotFoundError:
        return "Package not found"


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
            if file.startswith('.'):
                continue
            file_path = os.path.join(root, file)
            all_files.append(file_path)

    # Remove duplicate files. Sort them for determinism.
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


COMPRESSION_EXTENSION = '.zst'


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
    process = psutil.Process(mp.current_process().pid)
    memory_usage_mb = process.memory_info().rss / 1024 / 1024
    return round(memory_usage_mb, 2)


def get_line_positions(path: str):
    """
    Get all line positions in the given file. So that it could be re-used to read the file for a specific line.
    """
    line_positions = []

    with open(path, 'r') as f:
        # Get position before reading the line so that it is the beginning of the line.
        position = f.tell()
        line = f.readline()
        while line:
            if len(line) > 0:
                line_positions.append(position)
            position = f.tell()
            line = f.readline()

    return line_positions


def read_line_in_file(path: str, position: int):
    """
    Read a specific line in the file without loading the entire file into memory.
    """
    with open(path, 'r') as f:
        f.seek(position)
        return f.readline()


def _rmtree_error_handler(func, path, exc_info):
    twb_logger.error(f"Error occurred while calling {func.__name__} on {path}")
    twb_logger.error(f"Error details: {exc_info}")

    # TODO: We might be able to attempt to resolve the issue based on exc_info and then retry the operation.


def cleanup_dir(path: str, onerror: Union[Callable, None] = _rmtree_error_handler):
    """
    Clean up the directory.
    :param path: the directory path
    :param onerror: the error handler
    """
    if os.path.exists(path):
        try:
            shutil.rmtree(path, onerror=onerror)
        except Exception as e:
            twb_logger.error(f"Error occurred while removing: {path}. Check next log for details.")
            twb_logger.error(e)


def prepare_output_dir(output_dir: str):
    if os.path.exists(output_dir):
        cleanup_dir(output_dir)
    os.makedirs(output_dir)


def parse_schema(obj):
    """
    Parse the schema of the given object. Used for glimpse.
    """
    if isinstance(obj, dict):
        if not obj:
            return "empty"
        return {key: parse_schema(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        if not obj:
            return "empty"
        return [parse_schema(obj[0]), len(obj)]
    else:
        return type(obj).__name__
