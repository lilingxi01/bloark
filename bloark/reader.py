import json
import logging
import os
import uuid
from typing import Union, Tuple
import time

from .logger import cleanup_logger, universal_logger_init, twb_logger
from .parallelization import RDSProcessManager
from .utils import get_file_list, decompress_zstd, prepare_output_dir, get_curr_version, get_line_positions, \
    cleanup_dir, read_line_in_file, parse_schema

_DEFAULT_NUM_PROC = 1
_DEFAULT_LOG_LEVEL = logging.DEBUG


class Reader:
    """
    The core class to read the generated temporal wikipedia blocks.

    Basic usage:

        import twb
        reader = twb.Reader()  # Create an instance.
        reader.preload('./test/sample_data/minimal_sample.xml')  # Preload the files to be processed.

    You must preload the files to be processed before building the blocks.
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
        :param num_proc: the number of processes to use (default: 1)
        :param log_dir: the dir to the log file (default: None)
        :param log_level: the log level (default: logging.INFO)
        """
        self.num_proc = num_proc
        self.log_dir = log_dir
        self.log_level = log_level

        self.files = []

        # If log dir exists, remove it first.
        cleanup_logger(log_name='reader', log_dir=log_dir)

        # Initialize the logger.
        universal_logger_init(log_name='reader', log_dir=log_dir, log_level=log_level)

    def preload(self, path: str):
        """
        Preload the files to be processed.
        It will not actually load to the memory until the build() method is called.
        :param path: the path of a file or a directory
        :raise ValueError: if the path is empty
        :raise FileNotFoundError: if the path does not exist
        """
        if not path:
            raise ValueError('The path cannot be empty.')
        if not os.path.exists(path):
            raise FileNotFoundError('The path does not exist.')
        self.files.extend(get_file_list(path))

    def glimpse(self) -> Union[Tuple[dict, dict], Tuple[None, None]]:
        """
        Take a glimpse of the data.
        It could still be large if one object contains a lot of information (e.g. many revisions, long article).
        """
        if len(self.files) == 0:
            twb_logger.warning('No file is loaded.')
            return None, None

        # Randomly select a file.
        picked_index = int(uuid.uuid4().int % len(self.files))
        glimpse_path = self.files[picked_index]

        twb_logger.info(f'Randomly chosen file: {glimpse_path}')

        # Prepare the temporary directory for glimpse.
        glimpse_temp_dir = os.path.join(os.getcwd(), '.glimpse')
        os.makedirs(glimpse_temp_dir, exist_ok=True)

        twb_logger.info(f'Decompressing...')

        # Decompress the file.
        decompressed_path = _decompress_executor(glimpse_path, glimpse_temp_dir)
        first_block_text = read_line_in_file(decompressed_path, 0).rstrip('\n')

        if first_block_text[0] != '{' or first_block_text[-1] != '}':
            twb_logger.error(f'Invalid starting of block or end of block.')
            return None, None

        # Read the first block into memory and then delete the decompressed file.
        first_block = json.loads(first_block_text)
        cleanup_dir(glimpse_temp_dir)

        twb_logger.info(f'Glimpse finished.')

        return first_block, parse_schema(first_block)

    def decompress(self, output_dir: str):
        """
        Decompress the selected files.
        :param output_dir: the directory to store the decompressed files
        """
        # Log the version.
        twb_logger.info(f'TWB Package Version: {get_curr_version()}')

        # Prepare the output directory.
        prepare_output_dir(output_dir)

        start_time = time.time()

        pm = RDSProcessManager(
            log_name='decompress',
            log_dir=self.log_dir,
            log_level=self.log_level,
            num_proc=self.num_proc
        )
        for file in self.files:
            pm.apply_async(
                executable=_decompress_executor,
                args=(file, output_dir),
                use_controller=False
            )

        pm.close()
        pm.join()

        end_time = time.time()
        execution_duration = end_time - start_time
        twb_logger.info(f'Decompression finished. (Duration: {execution_duration:.2f}s)')


def _decompress_executor(path: str, output_dir: str) -> str:
    original_name = os.path.split(path)[1]
    decompressed_name = original_name.replace('.zst', '')
    decompressed_path = os.path.join(output_dir, decompressed_name)
    decompress_zstd(path, decompressed_path)
    return decompressed_path


def _line_position_processor(path: str, output_dir: str):
    """
    The processor for the line position process.
    """
    temp_id = uuid.uuid4().hex

    temp_dir = os.path.join(output_dir, 'temp', temp_id)
    os.makedirs(temp_dir, exist_ok=True)

    _decompress_executor(path, temp_dir)

    return path, get_line_positions(path)
