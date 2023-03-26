import logging
import os
import uuid
from typing import Union, List
import time

from .modifier import Modifier
from .logger import cleanup_logger, universal_logger_init, twb_logger
from .parallelization import RDSProcessManager, RDSProcessController
from .utils import get_file_list, decompress_zstd, prepare_output_dir, get_curr_version, get_line_positions, \
    cleanup_dir, read_line_in_file

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

    def add_modifier(self, modifier: Modifier):
        """
        Map a function to each block.
        :param modifier: the modifier to be added
        """
        self.modifiers.append(modifier)

    def build_modification(self, output_dir: str):
        """
        Build the blocks after apply the modifiers.
        :param output_dir: the directory to store the modified files
        """
        # Log the version.
        twb_logger.info(f'TWB Package Version: {get_curr_version()}')

        # Prepare the output directory.
        prepare_output_dir(output_dir)

        curr_count = 0
        total_count = len(self.files)

        global_temp_dir = os.path.join(output_dir, 'temp')
        os.makedirs(global_temp_dir, exist_ok=True)

        def _success_callback(*args):
            pass

        def _error_callback(e):
            twb_logger.error(f'Error occurred when processing block: {e}')

        for file_path in self.files:
            twb_logger.info(f'Start: {file_path}')

            # Create temporary directory for this file.
            temp_id = uuid.uuid4().hex
            temp_dir = os.path.join(output_dir, 'temp', temp_id)
            os.makedirs(temp_dir, exist_ok=True)

            decompression_temp_dir = os.path.join(temp_dir, 'decompression')
            os.makedirs(decompression_temp_dir, exist_ok=True)
            compression_temp_dir = os.path.join(temp_dir, 'compression')
            os.makedirs(compression_temp_dir, exist_ok=True)

            # Decompress the file first (this step will not be done in parallel because this is faster).
            twb_logger.debug(f'Decompressing...')
            decompressed_path = _decompress_executor(file_path, decompression_temp_dir)

            line_positions = get_line_positions(decompressed_path)

            twb_logger.info(f'Start modifying {len(line_positions)} blocks...')

            # Get line positions for all files.
            pm = RDSProcessManager(
                log_name='reader',
                log_dir=self.log_dir,
                log_level=self.log_level,
                num_proc=self.num_proc
            )

            for line_position in line_positions:
                pm.apply_async(
                    executable=_modify_executor,
                    args=(decompressed_path, line_position, self.modifiers, temp_dir),
                    use_controller=True,
                    callback=_success_callback,
                    error_callback=_error_callback
                )

            pm.close()
            pm.join()

            # Clean up the temporary directory at this step.
            cleanup_dir(temp_dir)

            twb_logger.info(f'Finished: {file_path}')

            # Log the progress.
            curr_count += 1
            twb_logger.info(f'Progress: {curr_count} / {total_count} = ({curr_count / total_count * 100:.2f}%)')

        # Clean up the global temporary directory.
        cleanup_dir(global_temp_dir)

        # Log the end of the task.
        twb_logger.info('All done! Finished all files.')


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


def _modify_executor(controller: RDSProcessController,
                     path: str,
                     position: int,
                     modifiers: List[Modifier],
                     output_dir: str):
    """
    The executor for the modification process.
    :param controller: the controller of the process
    :param path: the path of the file to be processed
    :param position: the position of the file to be processed
    :param modifiers: the list of modifiers to be applied
    :param output_dir: the directory to store the modified files
    """
    controller.logdebug(f'Processing block: {position}')
    block = read_line_in_file(path, position).rstrip('\n')
    if block[0] != '{' or block[-1] != '}':
        controller.logerr(f'Invalid starting of block or end of block: {block}')
        return

    controller.logdebug(f'Finished block: {position}')

    # TODO: Implement this.
