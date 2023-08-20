import json
import logging
import os
from functools import partial
from multiprocessing import Pool
from typing import List, Tuple, Optional, TextIO
from abc import ABC, abstractmethod
import time

from .logger_init import _init_logger_main_process, _init_logger_sub_process, _init_logger_multiprocessing
from .utils import get_file_list, decompress_zstd, prepare_output_dir, get_curr_version, \
    cleanup_dir, compress_zstd, COMPRESSION_EXTENSION, get_line_positions, read_line_in_file
from .decorators import deprecated
from .warehouse import Warehouse, get_warehouse_filenames

_DEFAULT_NUM_PROC = 1
_DEFAULT_LOG_LEVEL = logging.INFO


class ModifierProfile(ABC):
    """
    The core class to define how to modify the JSON content.
    """
    @abstractmethod
    def block(self, content: dict, metadata: dict) -> Tuple[Optional[dict], Optional[dict]]:
        """
        Returns a list of batches of URLs to download.

        Parameters
        ----------
        content : dict
            The JSON content to be modified.
        metadata : dict
            The metadata of the JSON content. This will be updated within one segment from the previous return value.
        logger : logging.Logger
            The logger that should be used if you want to print out something. Check log standards for more details.

        Returns
        -------
        Tuple[Optional[dict], Optional[dict]]
            Data JSON content and metadata (whatever modified or not).
            Return None in the first value if the content should be removed.
            Return None in the second value if the entire segment should be removed.

        """
        pass


class Modifier:
    """
    Modifier is the class to define how to modify the JSON content of a block (or a segment) from the warehouse.

    Attributes
    ----------
    num_proc : int
        The number of processes to use.
    log_level : int
        The log level.
    files : list
        A list of files to be read.
    modifiers : list
        A list of modifiers to be applied.

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
            The log level.

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

    def add_profile(self, profile: ModifierProfile):
        """
        Map a function to each block.

        Parameters
        ----------
        profile : ModifierProfile
            The modifier profile to be added.

        """
        self.modifiers.append(profile)

    def _worker_initializer(self, q):
        """
        Initialize the worker process.
        """
        # Initialize the logger within the sub-process.
        _init_logger_sub_process(q, log_level=self.log_level)

    def _modify_executor(self,
                         old_warehouse_path: str,
                         old_warehouse_metadata_path: str,
                         warehouse: Warehouse) -> List[str]:
        if not os.path.exists(old_warehouse_path) or not os.path.exists(old_warehouse_metadata_path):
            logging.critical(f'The file {old_warehouse_path} or {old_warehouse_metadata_path} does not exist.')
            return []

        try:
            # Prepare the temporary directory.
            temp_dir = os.path.join(self.output_dir, 'temp')
            os.makedirs(temp_dir, exist_ok=True)

            # Prepare the path for temporary decompressed file.
            original_name = os.path.split(old_warehouse_path)[1]
            decompressed_name = original_name.replace('.zst', '')
            decompressed_path = os.path.join(temp_dir, decompressed_name)

            # Decompress the old warehouse.
            decompress_zstd(old_warehouse_path, decompressed_path)

            # Prepare the path for the new warehouse.
            assigned_warehouse: Optional[str] = None  # The assigned target warehouse name.
            new_warehouse_path: Optional[str] = None  # The path of the new warehouse.
            new_warehouse_metadata_path: Optional[str] = None  # The path of the new warehouse metadata.
            new_warehouse_file: Optional[TextIO] = None  # Warehouse JSONL IO.
            segment_metadata: Optional[dict] = None  # The metadata of the current segment.

            byte_start: int = -1  # The byte start of the current article.
            full_warehouse_paths: List[str] = []
            metadata_line_positions = get_line_positions(old_warehouse_metadata_path)

        except Exception as e:
            logging.critical(f'Error occurred when preparing the modification: {e}')
            return []

        def _read_next_segment():
            nonlocal segment_metadata, assigned_warehouse, new_warehouse_path, new_warehouse_metadata_path, \
                new_warehouse_file, byte_start, old_warehouse_metadata_path

            if not metadata_line_positions:
                return False

            try:
                assigned_warehouse = warehouse.assign_warehouse()
                new_warehouse_filename, new_warehouse_metadata_filename = get_warehouse_filenames(assigned_warehouse)
                new_warehouse_path = os.path.join(self.output_dir, new_warehouse_filename)
                new_warehouse_metadata_path = os.path.join(self.output_dir, new_warehouse_metadata_filename)
                new_warehouse_file = open(new_warehouse_path, 'a')

                segment_metadata = read_line_in_file(old_warehouse_metadata_path, metadata_line_positions[0])
                metadata_line_positions.pop(0)  # Remove the first element to push the queue forward.
                segment_metadata = json.loads(segment_metadata)

                # Record the byte start of the article. This variable will be stored by the end of the modification.
                byte_start = new_warehouse_file.tell()

            except Exception as e:
                logging.error(f'Error occurred when loading metadata: {e}')
                return False

            return True

        def _on_segment_finished():
            nonlocal assigned_warehouse, new_warehouse_file, full_warehouse_paths, segment_metadata, byte_start
            segment_metadata = None
            byte_start = -1
            if new_warehouse_file is not None:
                new_warehouse_file.close()
                new_warehouse_file = None
            if assigned_warehouse is not None:
                full_warehouse_path = warehouse.release_warehouse(assigned_warehouse)
                assigned_warehouse = None
                if full_warehouse_path is not None:
                    full_warehouse_paths.append(full_warehouse_path)

        # Main modification loop for each segment.
        while _read_next_segment():
            try:
                skip_segment = False  # Whether to skip the current segment.

                old_warehouse_byte_start = segment_metadata['byte_start']
                old_warehouse_byte_end = segment_metadata['byte_end']

                old_warehouse_file = open(decompressed_path, 'r')
                old_warehouse_file.seek(old_warehouse_byte_start)

                while old_warehouse_file.tell() < old_warehouse_byte_end:
                    # Read the current block from the old warehouse.
                    original_block = old_warehouse_file.readline()
                    original_block = json.loads(original_block)
                    modified_block = original_block
                    for modifier in self.modifiers:
                        # If the block becomes None, it means that this block should be skipped.
                        if modified_block is None:
                            break
                        try:
                            modified_block, modified_segment_metadata = modifier.block(
                                content=modified_block,
                                metadata=segment_metadata,
                            )
                        except Exception as e:
                            logging.error(f'Error occurred within the user-defined modifier: {e}')
                            modified_block = None
                            break
                        # If the segment metadata becomes None, it means that the segment should be skipped.
                        if modified_segment_metadata is None:
                            skip_segment = True
                            break
                    if skip_segment:
                        break
                    if modified_block is None:
                        continue
                    # Write the modified block to the new warehouse.
                    new_warehouse_file.write(json.dumps(modified_block) + '\n')
                    # Garbage collection to release memory.
                    del original_block
                    del modified_block

                old_warehouse_file.close()
            except Exception as e:
                logging.error(f'Error occurred when modifying the segment: {e}')
                _on_segment_finished()
                continue

            try:
                # Finalize the segment.
                segment_metadata['byte_start'] = byte_start
                segment_metadata['byte_end'] = new_warehouse_file.tell()
                with open(new_warehouse_metadata_path, 'a') as f:
                    f.write(json.dumps(segment_metadata) + '\n')
            except Exception as e:
                logging.error(f'Error occurred when finalizing the segment: {e}')

            _on_segment_finished()

        # Remove the decompressed file.
        os.remove(decompressed_path)

        return full_warehouse_paths

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

    @deprecated(version='2.1.2', message='''
        This function name is opaque. Please use `start()` instead. This API will be removed after v2.4.
    ''')
    def build(self):
        self.start()

    def start(self):
        """
        Start applying modifiers over blocks and segments. Check documentation for more details of our architecture.
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

        # Only count the number of zst files for total count at progress bar.
        total_count = len(list(filter(lambda x: x.endswith('.zst'), self.files)))

        global_temp_dir = os.path.join(self.output_dir, 'temp')
        os.makedirs(global_temp_dir, exist_ok=True)

        # Initialize multiprocessing logger.
        ql, q = _init_logger_multiprocessing(log_level=self.log_level)

        # Tasks is a list of tuples (task_type, args).
        # Task types: 'process', 'warehouse', 'cleanup'
        tasks: List[Tuple[str, Tuple]] = []

        available_process_count = self.num_proc

        mp_pool = Pool(
            processes=self.num_proc,
            initializer=self._worker_initializer,
            initargs=(q,)
        )

        modified_count = 0

        def _modify_callback(full_warehouse_paths: List[str]):
            nonlocal warehouse, available_process_count, modified_count

            for full_warehouse_path in full_warehouse_paths:
                next_task_type = 'cleanup'
                next_args = (full_warehouse_path,)
                tasks.insert(0, (next_task_type, next_args))

            available_process_count += 1
            modified_count += 1

            logging.info(f'({modified_count / total_count * 100:.2f}% = {modified_count} / {total_count}) | '
                         f'Full warehouses: {len(full_warehouse_paths)}')

        def _cleanup_callback(cleanup_path):
            nonlocal available_process_count
            logging.info(f'Warehouse packed: {cleanup_path}')

            available_process_count += 1

        def _success_callback(task_type, file_path):
            if task_type == 'modify':
                _modify_callback(file_path)
            elif task_type == 'cleanup':
                _cleanup_callback(file_path)

        def _error_callback(e):
            logging.error(f'Error occurred when processing block: {e}')

        # Built-up initial tasks.
        # Needed to match corresponding zst files with their metadata files if there is any.
        zst_files = set()
        metadata_files = set()
        for curr_file_path in self.files:
            if curr_file_path.endswith('.zst'):
                zst_files.add(curr_file_path)
            elif curr_file_path.endswith('.metadata'):
                metadata_files.add(curr_file_path)
        zst_files = list(zst_files)
        metadata_files = list(metadata_files)
        logging.debug(f'zst_files: {zst_files}')
        logging.debug(f'metadata_files: {metadata_files}')
        for curr_file_path in zst_files:
            curr_file_basename = os.path.basename(curr_file_path).rstrip('.jsonl.zst')
            metadata_file_path = os.path.join(os.path.dirname(curr_file_path), curr_file_basename + '.metadata')
            if metadata_file_path in metadata_files:
                tasks.append(('modify', (curr_file_path, metadata_file_path)))

        # Main semaphore loop.
        while tasks or available_process_count < self.num_proc:
            if tasks and available_process_count > 0:
                available_process_count -= 1
                task_type, args = tasks.pop(0)
                if task_type == 'modify':
                    curr_file_path, metadata_file_path = args
                    mp_pool.apply_async(
                        func=self._modify_executor,
                        args=(curr_file_path, metadata_file_path, warehouse),
                        callback=partial(_success_callback, task_type),
                        error_callback=_error_callback
                    )
                elif task_type == 'cleanup':
                    cleanup_path, = args
                    mp_pool.apply_async(
                        func=self._cleanup_executor,
                        args=(cleanup_path,),
                        callback=partial(_success_callback, task_type),
                        error_callback=_error_callback
                    )
            else:
                time.sleep(0.1)

        logging.debug(f'Semaphore loop finished.')

        for final_warehouse in warehouse.available_warehouses:
            next_task_type = 'cleanup'
            warehouse_filename = get_warehouse_filenames(final_warehouse)[0]
            next_args = (warehouse_filename,)
            tasks.append((next_task_type, next_args))

        # Final cleanup loop.
        while tasks or available_process_count < self.num_proc:
            if tasks and available_process_count > 0:
                available_process_count -= 1
                task_type, args = tasks.pop(0)
                if task_type == 'cleanup':
                    warehouse_filename, = args
                    mp_pool.apply_async(
                        func=self._cleanup_executor,
                        args=(warehouse_filename,),
                        callback=partial(_success_callback, task_type),
                        error_callback=_error_callback
                    )
                else:
                    logging.critical(f'Unknown task type: {task_type}')
                    available_process_count += 1
            else:
                time.sleep(0.1)

        logging.info(f'Cleanup loop finished.')

        mp_pool.close()
        mp_pool.join()

        # Clean up the global temporary directory.
        cleanup_dir(global_temp_dir)

        end_time = time.time()
        execution_duration = end_time - start_time

        # Log the end of the task.
        logging.info(f'All done! Finished all files. (took {execution_duration:.2f}s in total)')
