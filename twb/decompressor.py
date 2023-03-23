import logging
import os
import shutil
import time
from typing import Union

import py7zr

from .logger import universal_logger_init, twb_logger
from .parallelization import RDSProcessManager, RDSProcessController
from .utils import get_file_list, decompress_zstd


class Decompressor:
    """
    The core class to decompress normal 7z files or zstd files.

    Basic usage:

        import twb
        decom = twb.Decompressor()  # Create an instance.
        decom.preload('./input/archive.7z')  # Preload the files to be processed.

    You must preload the files to be processed before starting decompressing.
    Preload function accepts a path to a file or a directory, and can be called multiple times (for multiple files).

    Attributes:
        files (list): A list of files to be read.
    """

    def __init__(self):
        self.files = []

    def preload(self, path: str):
        """
        Preload the files to be processed.
        It will not actually load to the memory until the go() method is called.
        :param path: the path of a file or a directory
        :raise ValueError: if the path is empty
        :raise FileNotFoundError: if the path does not exist
        """
        if not path:
            raise ValueError('The path cannot be empty.')
        if not os.path.exists(path):
            raise FileNotFoundError('The path does not exist.')
        self.files.extend(get_file_list(path))

    def go(self,
           output_dir: str,
           num_proc: int = None,
           log_dir: Union[str, None] = None,
           log_level: int = logging.INFO):
        """
        Decompress the files.
        :param output_dir: the output directory
        :param num_proc: the number of processes to use
        :param log_dir: the dir to the log file (default: None)
        :param log_level: the log level (default: logging.INFO)
        """

        # If log dir exists, remove it first.
        if log_dir is not None and os.path.exists(log_dir):
            shutil.rmtree(log_dir)

        universal_logger_init(log_dir=log_dir, log_level=log_level)

        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        start_time = time.time()

        pm = RDSProcessManager(
            num_proc=num_proc
        )
        for file in self.files:
            pm.apply_async(
                executable=_decompress_file,
                args=(file, output_dir),
                use_controller=True
            )

        pm.close()
        pm.join()

        end_time = time.time()
        execution_duration = end_time - start_time
        twb_logger.info(f'Decompression finished in {execution_duration:.2f} seconds.')


def _decompress_file(controller: RDSProcessController, path: str, output_dir: str):
    start_time = time.time()
    original_name = os.path.split(path)[1]
    if original_name.endswith('.zst'):
        decompressed_name = original_name.replace('.zst', '')
        decompressed_path = os.path.join(output_dir, decompressed_name)
        decompress_zstd(path, decompressed_path)
    elif original_name.endswith('.7z'):
        decompressed_name = original_name.replace('.7z', '')
        decompressed_path = os.path.join(output_dir, decompressed_name)
        with py7zr.SevenZipFile(path, mode='r', mp=False) as z:
            z.extractall(path=decompressed_path)
    end_time = time.time()
    execution_duration = end_time - start_time
    controller.logdebug(f'Decompression took {execution_duration:.2f} seconds. ({original_name})')
