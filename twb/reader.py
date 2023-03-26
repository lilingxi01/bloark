import logging
import os
from typing import Union
import time

from .modifier import Modifier
from .logger import cleanup_logger_dir, universal_logger_init, twb_logger
from .parallelization import RDSProcessManager
from .utils import get_file_list, decompress_zstd, cleanup_dir

_DEFAULT_NUM_PROC = 1
_DEFAULT_LOG_LEVEL = logging.INFO


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
        modifiers (list): A list of modifiers to be applied.
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
        self.modifiers = []

        # If log dir exists, remove it first.
        cleanup_logger_dir(log_dir=log_dir)

        # Initialize the logger.
        universal_logger_init(log_dir=log_dir, log_level=log_level)

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

    def glimpse(self):
        """
        Take a glimpse of the data.
        It could still be large if one object contains a lot of information (e.g. many revisions, long article).
        """
        # TODO: Implement this.
        #  It should be the same as reading the data, but only take the first file and the first block.
        pass

    def decompress(self,
                   output_dir: str):
        """
        Decompress the selected files.
        :param output_dir: the directory to store the decompressed files
        """
        num_proc = self.num_proc
        log_dir = self.log_dir
        log_level = self.log_level

        if os.path.exists(output_dir):
            cleanup_dir(output_dir)
        os.makedirs(output_dir)

        start_time = time.time()

        pm = RDSProcessManager(
            num_proc=num_proc,
            log_dir=log_dir,
            log_level=log_level
        )
        for file in self.files:
            pm.apply_async(
                executable=_decompress_file,
                args=(file, output_dir),
                use_controller=False
            )

        pm.close()
        pm.join()

        end_time = time.time()
        execution_duration = end_time - start_time
        twb_logger.info(f'Decompression finished. (Duration: {execution_duration:.2f}s)')

    def add_modifier(self, modifier: Modifier):
        """
        Map a function to each block.
        :param modifier: the modifier to be added
        """
        self.modifiers.append(modifier)

    def build_modification(self,
                           output_dir: str):
        """
        Build the blocks after apply the modifiers.
        :param output_dir: the directory to store the modified files
        """
        # TODO: Implement this.
        pass


def _decompress_file(path: str, output_dir: str):
    original_name = os.path.split(path)[1]
    decompressed_name = original_name.replace('.zst', '')
    decompressed_path = os.path.join(output_dir, decompressed_name)
    decompress_zstd(path, decompressed_path)
