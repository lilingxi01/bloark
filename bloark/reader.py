import json
import logging
import os
from functools import partial
import multiprocessing as mp
from typing import List, Tuple, Union
import time
import uuid

from py7zr import py7zr

from .logger_init import _init_logger_main_process, _init_logger_sub_process, _init_logger_multiprocessing
from .utils import get_file_list, prepare_output_dir, get_curr_version, cleanup_dir, read_line_in_file, parse_schema, \
    decompress_zstd

_DEFAULT_NUM_PROC = 1
_DEFAULT_LOG_LEVEL = logging.INFO

_IGNORED_READER_FILES = [
    '.DS_Store',
    '.gitignore',
    '.gitattributes',
    '.env',
]


class Reader:
    """
    Reader is a class for reading the data from the warehouse (rather than from the original data source).

    Attributes
    ----------
    output_dir : str
        The output directory.
    num_proc : int
        The number of processes to use.
    log_level : int
        The built-in logging level.
    files : list
        A list of files to be read.

    """

    def __init__(self,
                 output_dir: str,
                 num_proc: int = _DEFAULT_NUM_PROC,
                 log_level: int = _DEFAULT_LOG_LEVEL):
        """
        Parameters
        ----------
        output_dir : str
            The output directory.
        num_proc : int, default=1
            The number of processes to use.
        log_level : int, default=logging.INFO
            The built-in logging level.

        """
        self.output_dir = output_dir
        self.num_proc = num_proc
        self.log_level = log_level

        self.files: List[str] = []

        _init_logger_main_process(log_level=self.log_level)

    def preload(self, path: str):
        """
        Preload the files to be processed.
        It will not actually load to the memory until any other method is called.

        Parameters
        ----------
        path : str
            The path of a file or a directory.

        Raises
        ------
        ValueError
            If the path is empty.
        FileNotFoundError
            If the path does not exist.

        """
        if not path:
            raise ValueError('The path cannot be empty.')
        if not os.path.exists(path):
            raise FileNotFoundError('The path does not exist.')
        self.files.extend(get_file_list(path))

    def _worker_initializer(self, q):
        """
        Initialize the worker process.
        """
        # Initialize the logger within the sub-process.
        _init_logger_sub_process(q, log_level=self.log_level)

    def _decompress_executor(self, file_path: str, temporarily: bool = False) -> List[str]:
        """
        Decompress the file.
        """
        # Initialize output directory.
        output_dir = os.path.join(self.output_dir, 'temp') if temporarily else self.output_dir
        os.makedirs(output_dir, exist_ok=True)

        archive_filename = os.path.basename(file_path)
        random_id = uuid.uuid4().hex
        decompressed_dir_path = os.path.join(output_dir, random_id) if temporarily else output_dir
        if temporarily:
            os.makedirs(decompressed_dir_path, exist_ok=True)

        logging.debug(f'Decompressing [{archive_filename}]...')

        try:
            start_time = time.time()
            if file_path.endswith('.7z'):
                with py7zr.SevenZipFile(file_path, mode='r', mp=False) as z:
                    z.extractall(path=decompressed_dir_path)
                decompressed_files = get_file_list(decompressed_dir_path)
            elif file_path.endswith('.zst'):
                decompress_zstd(file_path, os.path.join(decompressed_dir_path, archive_filename[:-4]))
                decompressed_files = [os.path.join(decompressed_dir_path, archive_filename[:-4])]
            else:
                decompressed_files = []
            end_time = time.time()
            execution_duration = (end_time - start_time) / 60
            logging.debug(f'Decompression took {execution_duration:.2f} min. ({archive_filename})')
            return decompressed_files

        except Exception as e:
            logging.critical(f'Failed to decompress [{archive_filename}] for this reason: {e}')
            return []

    def decompress(self):
        """
        Decompress the preloaded files.
        """
        # Log the version.
        logging.info(f'Builder Version: {get_curr_version()}')

        start_time = time.time()

        # Prepare the output directory.
        prepare_output_dir(self.output_dir)

        total_count = len(self.files)

        # Initialize multiprocessing logger.
        ql, q = _init_logger_multiprocessing(log_level=self.log_level)

        # Tasks is a list of tuples (task_type, args).
        # Task types: 'decompress', 'process', 'warehouse', 'cleanup'
        tasks: List[Tuple[str, Tuple]] = []

        available_process_count = self.num_proc

        decompression_pool = mp.Pool(
            processes=self.num_proc,
            initializer=self._worker_initializer,
            initargs=(q,),
        )

        processed_count = 0

        def _decompress_callback(decompressed_files: List[str]):
            nonlocal processed_count, total_count
            if not decompressed_files:
                logging.debug(f'Decompressed (EMPTY): {decompressed_files}')
                return

            processed_count += 1

            decompressed_dir = os.path.dirname(decompressed_files[0])
            logging.debug(f'Decompressed (OK): {decompressed_dir} ({processed_count} / {total_count})')

        def _success_callback(task_type, file_path):
            nonlocal available_process_count
            if task_type == 'decompress':
                _decompress_callback(file_path)
            available_process_count += 1

        def _error_callback(e):
            nonlocal available_process_count
            logging.critical(f'Process terminated-level error: {e}')
            available_process_count += 1

        # Built-up initial tasks.
        for curr_file_path in self.files:
            if curr_file_path.endswith('.metadata') or os.path.basename(curr_file_path) in _IGNORED_READER_FILES:
                continue
            if not curr_file_path.endswith('.zst'):
                logging.warning(f'Unsupported file format: {curr_file_path}')
                continue
            tasks.append(('decompress', (curr_file_path,)))

        # Main semaphore loop.
        while tasks or available_process_count < self.num_proc:
            if tasks and available_process_count > 0:
                available_process_count -= 1
                task_type, args = tasks.pop(0)
                if task_type == 'decompress':
                    file_path, = args
                    decompression_pool.apply_async(
                        func=self._decompress_executor,
                        args=(file_path,),
                        callback=partial(_success_callback, task_type),
                        error_callback=_error_callback
                    )
                else:
                    logging.critical(f'Unknown task type: {task_type}')
                    available_process_count += 1
            else:
                time.sleep(0.1)

        logging.info('Main loop finished.')

        end_time = time.time()
        execution_duration = (end_time - start_time) / 60

        # Log the end of the task.
        logging.info(f'All done! Finished {processed_count} files. (took {execution_duration:.2f} min in total)')

    def glimpse(self) -> Union[Tuple[dict, dict], Tuple[None, None]]:
        """
        Take a glimpse of the preloaded data.
        It could still be large if one object contains a lot of information (e.g. many revisions, long article).

        Returns
        -------
        Tuple[dict, dict]
            A tuple of two dictionaries: (page, revision). If there is no file loaded, it returns (None, None).

        Notes
        -----
        This function does not use any parallelization technique.

        """
        if len(self.files) == 0:
            logging.warning('No file is loaded.')
            return None, None

        # Randomly select a file.
        warehouse_files = [f for f in self.files if f.endswith('.zst')]
        picked_index = int(uuid.uuid4().int % len(warehouse_files))
        glimpse_path = warehouse_files[picked_index]

        logging.info(f'Randomly chosen file: {glimpse_path}')

        # Prepare the temporary directory for glimpse.
        glimpse_temp_dir = os.path.join(os.getcwd(), '.glimpse')
        os.makedirs(glimpse_temp_dir, exist_ok=True)

        logging.info(f'Decompressing...')

        # Decompress the file.
        decompressed_files = self._decompress_executor(file_path=glimpse_path, temporarily=True)
        if not decompressed_files:
            logging.error(f'Failed to decompress the file: {glimpse_path}')
            return None, None

        decompressed_path = decompressed_files[0]
        first_block_text = read_line_in_file(decompressed_path, 0).rstrip('\n')

        if first_block_text[0] != '{' or first_block_text[-1] != '}':
            logging.error(f'Invalid starting of block or end of block.')
            return None, None

        # Read the first block into memory and then delete the decompressed file.
        first_block = json.loads(first_block_text)
        cleanup_dir(glimpse_temp_dir)

        logging.info(f'Glimpse finished.')

        return first_block, parse_schema(first_block)
