import logging
import os
import time
from typing import Union

import py7zr

from .logger import universal_logger_init, twb_logger, cleanup_logger
from .parallelization import RDSProcessManager, RDSProcessController
from .utils import get_file_list, decompress_zstd, get_curr_version, prepare_output_dir

_DEFAULT_NUM_PROC = 1
_DEFAULT_LOG_LEVEL = logging.INFO


class Decompressor:
    """
    The core class to decompress normal 7z files or zstd files in cluster environment.

    NOTE: Decompressor is used for decompressing a file or a directory of files in cluster environment where
    command line decompression is not available. This class is not used to read the dataset.
    If you are trying to read the dataset, please use `twb.Reader` rather than `twb.Decompressor`.

    You must preload the files to be processed before starting decompressing.
    Preload function accepts a path to a file or a directory, and can be called multiple times (for multiple files).

    Attributes:
        num_proc (int): The number of processes to use.
        log_dir (str): The dir to the log file.
        log_level (int): The log level.
        files (list): A list of files to be read.
    """

    def __init__(self,
                 log_dir: Union[str, None] = None,
                 num_proc: int = _DEFAULT_NUM_PROC,
                 log_level: int = _DEFAULT_LOG_LEVEL):
        """
        :param log_dir: the dir to the log file (default: None)
        :param num_proc: the number of processes to use (default: 1)
        :param log_level: the log level (default: logging.INFO)
        """
        self.log_dir = log_dir
        self.num_proc = num_proc
        self.log_level = log_level

        self.files = []

        # If log dir exists, remove it first.
        cleanup_logger(log_name='decompressor', log_dir=log_dir)

        # Initialize the logger.
        universal_logger_init(log_name='decompressor', log_dir=log_dir, log_level=log_level)

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

    def start(self, output_dir: str):
        """
        Decompress the files.
        :param output_dir: the output directory
        """
        # Log the version.
        twb_logger.info(f'TWB Package Version: {get_curr_version()}')

        num_proc = self.num_proc
        log_dir = self.log_dir
        log_level = self.log_level

        # Prepare the output directory.
        prepare_output_dir(output_dir)

        start_time = time.time()

        pm = RDSProcessManager(
            log_name='decompressor',
            log_dir=log_dir,
            log_level=log_level,
            num_proc=num_proc
        )

        # TODO: Add a error handler to handle the error when decompressing a file or process terminated unexpectedly.

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
    else:
        controller.logwarn(f'Unknown file type: {original_name}')
        return
    end_time = time.time()
    execution_duration = end_time - start_time
    controller.logdebug(f'Decompression took {execution_duration:.2f} seconds. ({original_name})')
