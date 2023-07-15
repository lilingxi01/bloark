import json
import logging
import os
from functools import partial
from multiprocessing import Pool
from typing import Union, List, Tuple
from abc import ABC, abstractmethod
import time

from .logger_init import _init_logger_main_process, _init_logger_sub_process, _init_logger_multiprocessing
from .utils import get_file_list, decompress_zstd, prepare_output_dir, get_curr_version, \
    cleanup_dir, compress_zstd, COMPRESSION_EXTENSION
from .warehouse import Warehouse, get_warehouse_filenames

_DEFAULT_NUM_PROC = 1
_DEFAULT_LOG_LEVEL = logging.INFO


class ModifierProfile(ABC):
    """
    The core class to define how to modify the JSON content.
    """
    @abstractmethod
    def block(self, content: dict) -> Union[dict, None]:
        """
        Returns a list of batches of URLs to download.
        :param content: The JSON content to be modified.
        :return: Modified JSON content. Return None if the content should be removed.
        """
        pass


class Modifier:
    """
    Attributes:
        num_proc (int): The number of processes to use.
        log_level (int): The log level.
        files (list): A list of files to be read.
        modifiers (list): A list of modifiers to be applied.
    """

    def __init__(self,
                 output_dir: str,
                 num_proc: int = _DEFAULT_NUM_PROC,
                 log_level: int = _DEFAULT_LOG_LEVEL):
        """
        :param output_dir: the output directory
        :param num_proc: the number of processes to use (default: 1)
        :param log_level: the log level (default: logging.INFO)
        """
        self.output_dir = output_dir
        self.num_proc = num_proc
        self.log_level = log_level

        self.files: List[str] = []
        self.modifiers: List[ModifierProfile] = []

        _init_logger_main_process(log_level=self.log_level)

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

    def add_profile(self, profile: ModifierProfile):
        """
        Map a function to each block.
        :param profile: the modifier profile to be added
        """
        self.modifiers.append(profile)

    def _worker_initializer(self, q):
        """
        Initialize the worker process.
        """
        # Initialize the logger within the sub-process.
        _init_logger_sub_process(q, log_level=self.log_level)

    def _process_executor(self, file_path: str):
        temp_dir = os.path.join(self.output_dir, 'temp')
        os.makedirs(temp_dir, exist_ok=True)

        original_name = os.path.split(file_path)[1]
        decompressed_name = original_name.replace('.zst', '')
        decompressed_path = os.path.join(temp_dir, decompressed_name)

        # Decompress the file.
        decompress_zstd(file_path, decompressed_path)

        modified_name = decompressed_name + '.modified'
        modified_path = os.path.join(temp_dir, modified_name)

        modified_file = open(modified_path, 'w')

        # Read the file line by line, and apply the modifiers.
        with open(decompressed_path, 'r') as f:
            for line in iter(f.readline, ''):
                block = json.loads(line)
                for modifier in self.modifiers:
                    block = modifier.block(block)
                    if block is None:
                        break
                if block is not None:
                    modified_file.write(json.dumps(block) + '\n')

        modified_file.close()

        # Remove the decompressed file.
        os.remove(decompressed_path)

        return modified_path

    def _warehouse_executor(self, file_path: str, assigned_warehouse: str):
        try:
            warehouse_filename, warehouse_metadata_filename = get_warehouse_filenames(assigned_warehouse)
            warehouse_path = os.path.join(self.output_dir, warehouse_filename)
            # warehouse_metadata_path = os.path.join(self.output_dir, warehouse_metadata_filename)  # TODO.

            warehouse_file = open(warehouse_path, 'a')

            # Read the file line by line, and move directly to the warehouse (without last empty line).
            with open(file_path, 'r') as f:
                for line in iter(f.readline, ''):
                    warehouse_file.write(line + '\n')

            warehouse_file.close()

            # Remove the modified file.
            os.remove(file_path)

        except Exception as e:
            logging.critical(f'Error occurred when moving the file to the warehouse. {e}')

        return assigned_warehouse

    def _cleanup_executor(self, warehouse_path: str):
        try:
            original_file_path = os.path.join(self.output_dir, warehouse_path)
            if not os.path.exists(original_file_path):
                logging.critical(f'The file {warehouse_path} [{original_file_path}] does not exist.')
                return None

            compressed_path = original_file_path + COMPRESSION_EXTENSION
            compress_zstd(original_file_path, compressed_path)
            os.remove(original_file_path)

            return compressed_path

        except:
            logging.critical(f'Error occurred when compressing the file {warehouse_path}.')
            return None

    def build(self):
        """
        Build the blocks after apply the modifiers.
        """
        # Log the version.
        logging.info(f'Modifier Version: {get_curr_version()}')

        warehouse = Warehouse(
            output_dir=self.output_dir,
            max_size=8,
            compress=True,
        )

        start_time = time.time()

        # Prepare the output directory.
        prepare_output_dir(self.output_dir)

        curr_count = 0
        total_count = len(self.files)

        global_temp_dir = os.path.join(self.output_dir, 'temp')
        os.makedirs(global_temp_dir, exist_ok=True)

        # Initialize multiprocessing logger.
        ql, q = _init_logger_multiprocessing(log_level=self.log_level)

        # Tasks is a list of tuples (task_type, args).
        # Task types: 'process', 'warehouse', 'cleanup'
        tasks: List[Tuple[str, Tuple]] = []

        available_process_count = self.num_proc

        modification_pool = Pool(
            processes=self.num_proc,
            initializer=self._worker_initializer,
            initargs=(q,)
        )

        processed_count = 0
        finished_count = 0

        def _process_callback(file_path):
            nonlocal warehouse, available_process_count, processed_count
            processed_count += 1

            assigned_warehouse = warehouse.assign_warehouse()

            logging.info(f'({processed_count / total_count * 100:.2f}% = {processed_count} / {total_count}) | '
                         f'Processed: {os.path.basename(file_path)} | Warehouse: {assigned_warehouse}')

            next_task_type = 'warehouse'
            next_args = (file_path, assigned_warehouse)
            tasks.insert(0, (next_task_type, next_args))

            available_process_count += 1

        def _warehouse_callback(warehouse_basename):
            nonlocal warehouse, available_process_count, finished_count

            logging.debug(f'Saved into warehouse: {warehouse_basename}')

            cleanup_path = warehouse.release_warehouse(warehouse_basename)
            if cleanup_path is not None:
                next_task_type = 'cleanup'
                next_args = (cleanup_path,)
                tasks.insert(0, (next_task_type, next_args))

            finished_count += 1
            available_process_count += 1

        def _cleanup_callback(cleanup_path):
            nonlocal available_process_count
            logging.info(f'Warehouse packed: {cleanup_path}')

            available_process_count += 1

        def _success_callback(task_type, file_path):
            if task_type == 'process':
                _process_callback(file_path)
            elif task_type == 'warehouse':
                _warehouse_callback(file_path)
            elif task_type == 'cleanup':
                _cleanup_callback(file_path)

        def _error_callback(e):
            logging.error(f'Error occurred when processing block: {e}')

        # Built-up initial tasks.
        for curr_file_path in self.files:
            tasks.append(('process', (curr_file_path,)))

        # Main semaphore loop.
        while tasks or available_process_count < self.num_proc:
            if tasks and available_process_count > 0:
                available_process_count -= 1
                task_type, args = tasks.pop(0)
                if task_type == 'process':
                    file_path, = args
                    modification_pool.apply_async(
                        func=self._process_executor,
                        args=(file_path,),
                        callback=partial(_success_callback, task_type),
                        error_callback=_error_callback
                    )
                elif task_type == 'warehouse':
                    file_path, assigned_warehouse = args
                    modification_pool.apply_async(
                        func=self._warehouse_executor,
                        args=(file_path, assigned_warehouse),
                        callback=partial(_success_callback, task_type),
                        error_callback=_error_callback
                    )
                elif task_type == 'cleanup':
                    cleanup_path, = args
                    modification_pool.apply_async(
                        func=self._cleanup_executor,
                        args=(cleanup_path,),
                        callback=partial(_success_callback, task_type),
                        error_callback=_error_callback
                    )
            else:
                time.sleep(0.1)

        logging.debug(f'Semaphore loop finished.')

        # Clean up the global temporary directory.
        cleanup_dir(global_temp_dir)

        end_time = time.time()
        execution_duration = end_time - start_time

        # Log the end of the task.
        logging.info(f'All done! Finished all files. (took {execution_duration:.2f}s in total)')
