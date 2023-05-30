import json
import logging
import os
import shutil
from functools import partial
import multiprocessing as mp
from typing import List, Tuple, Optional, TextIO
import time
import uuid

import xmltodict
from py7zr import py7zr

from .builder_helpers import extract_categories
from .logger_init import _init_logger_main_process, _init_logger_sub_process, _init_logger_multiprocessing
from .utils import get_file_list, prepare_output_dir, get_curr_version, cleanup_dir, compress_zstd, \
    COMPRESSION_EXTENSION
from .warehouse import Warehouse, get_warehouse_filenames

_DEFAULT_NUM_PROC = 1
_DEFAULT_LOG_LEVEL = logging.INFO


class Builder:
    """
    Attributes:
        output_dir (str): The output directory.
        num_proc (int): The number of processes to use.
        log_level (int): The log level.
        compress (bool): Whether to compress the output files.
        files (list): A list of files to be read.
    """

    def __init__(self,
                 output_dir: str,
                 num_proc: int = _DEFAULT_NUM_PROC,
                 log_level: int = _DEFAULT_LOG_LEVEL,
                 max_size: int = 16,
                 compress: bool = True):
        """
        :param output_dir: the output directory
        :param num_proc: the number of processes to use (default: 1)
        :param log_level: the log level (default: logging.INFO)
        :param max_size: the maximum size of an uncompressed warehouse (default: 12)
        :param compress: whether to compress the output files (default: True)
        """
        self.output_dir = output_dir
        self.num_proc = num_proc
        self.log_level = log_level
        self.max_size = max_size
        self.compress = compress

        self.files: List[str] = []

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

    def _worker_initializer(self, q):
        """
        Initialize the worker process.
        """
        # Initialize the logger within the sub-process.
        _init_logger_sub_process(q, log_level=self.log_level)

    def _decompress_executor(self, file_path: str) -> List[str]:
        # Initialize temporary directory.
        temp_dir = os.path.join(self.output_dir, 'temp')
        os.makedirs(temp_dir, exist_ok=True)

        archive_filename = os.path.basename(file_path)
        random_id = uuid.uuid4().hex
        decompressed_dir_path = os.path.join(temp_dir, random_id)
        os.makedirs(decompressed_dir_path, exist_ok=True)

        logging.debug(f'Decompressing [{archive_filename}]...')

        try:
            with py7zr.SevenZipFile(file_path, mode='r', mp=False) as z:
                start_time = time.time()
                z.extractall(path=decompressed_dir_path)
                end_time = time.time()
                execution_duration = (end_time - start_time) / 60
                logging.debug(f'Decompression took {execution_duration:.2f} min. ({archive_filename})')
            decompressed_files = get_file_list(decompressed_dir_path)
            return decompressed_files

        except Exception as e:
            logging.critical(f'Failed to decompress [{archive_filename}] for this reason: {e}')
            return []

    def _process_executor(self, xml_path: str, warehouse: Warehouse) -> List[str]:
        """
        Process the file.
        :param xml_path: the path of the file to be processed.
        :return: the number of URLs processed
        """
        logging.debug(f'Processing {os.path.basename(xml_path)}...')

        # Article Metadata.
        article_id: Optional[str] = None
        article_title: Optional[str] = None
        article_info: Optional[dict] = None

        assigned_warehouse: Optional[str] = None

        # Warehouse JSONL file path.
        warehouse_path: Optional[str] = None

        # Warehouse Metadata JSON file (which is metadata exclusively for this article).
        warehouse_metadata_path: Optional[str] = None

        # Warehouse JSONL IO.
        warehouse_file: Optional[TextIO] = None

        processed_count: int = 0
        full_warehouse_paths: List[str] = []

        def _create_new_article(new_article_id: str):
            nonlocal article_id, article_info, assigned_warehouse, warehouse_path, warehouse_metadata_path, \
                warehouse_file
            article_id = new_article_id
            # NOTE: We do not clean up the title because the title should be seen before the article id.
            article_info = dict()

            assigned_warehouse = warehouse.assign_warehouse()
            warehouse_filename, warehouse_metadata_filename = get_warehouse_filenames(assigned_warehouse)
            warehouse_path = os.path.join(self.output_dir, warehouse_filename)
            warehouse_metadata_path = os.path.join(self.output_dir, warehouse_metadata_filename)
            warehouse_file = open(warehouse_path, 'a')

            # Record the byte start of the article.
            article_info['byte_start'] = warehouse_file.tell()

        def _cleanup_article():
            nonlocal article_id, article_info, article_title, assigned_warehouse, warehouse_path, \
                warehouse_metadata_path, warehouse_file, full_warehouse_paths
            article_id = None
            article_info = None
            article_title = None
            warehouse_path = None
            warehouse_metadata_path = None
            if warehouse_file is not None:
                warehouse_file.close()
                warehouse_file = None
            if assigned_warehouse is not None:
                full_warehouse_path = warehouse.release_warehouse(assigned_warehouse)
                assigned_warehouse = None
                if full_warehouse_path is not None:
                    full_warehouse_paths.append(full_warehouse_path)

        def _finalizing_article():
            """
            After finishing writing an article, we will finalize the metadata file.
            """
            nonlocal article_id, article_info, article_title, assigned_warehouse, warehouse_path, \
                warehouse_metadata_path, warehouse_file, processed_count, full_warehouse_paths
            if article_id is None or warehouse_file is None:
                return
            with open(warehouse_metadata_path, 'a') as metadata_file:
                article_info['id'] = article_id
                article_info['title'] = article_title
                article_info['byte_end'] = warehouse_file.tell()
                if 'last_valid_text_content' in article_info:
                    article_info['categories'] = extract_categories(article_info['last_valid_text_content'])
                    del article_info['last_valid_text_content']
                else:
                    article_info['categories'] = []
                metadata_file.write(json.dumps(article_info) + '\n')
            warehouse_file.close()
            warehouse_file = None
            full_warehouse_path = warehouse.release_warehouse(assigned_warehouse)
            assigned_warehouse = None
            processed_count += 1
            if full_warehouse_path is not None:
                full_warehouse_paths.append(full_warehouse_path)

        def _inner_callback(path, item):
            nonlocal article_id, article_info, article_title, warehouse_file
            if path[-1][0] == 'title':
                # When a new article title is encountered, clean up the previous article and save the title for later.
                _finalizing_article()
                _cleanup_article()
                article_title = item.strip()
            if path[-1][0] == 'id':
                # When a new article id is encountered, create a new file for processing the article.
                _create_new_article(new_article_id=item.strip())
            if type(item) is dict and 'text' in item and '#text' in item['text']:
                to_be_written = dict()
                to_be_written['article_id'] = article_id
                if 'id' in item:
                    to_be_written['revision_id'] = item['id']
                    del item['id']
                if 'parentid' in item:
                    to_be_written['parent_id'] = item['parentid']
                    del item['parentid']
                if 'timestamp' in item:
                    to_be_written['timestamp'] = item['timestamp']
                    del item['timestamp']
                to_be_written.update(item)
                warehouse_file.write(json.dumps(to_be_written) + '\n')

                # Prepare text content for extracting categories.
                text_content = item['text']['#text']
                if len(text_content) > 0 and text_content[0] != '#REDIRECT':
                    # Extract categories.
                    article_info['source_revision'] = to_be_written['revision_id']
                    article_info['last_valid_text_content'] = text_content
            return True

        try:
            with open(xml_path, 'rb') as xml_file:
                xmltodict.parse(
                    xml_file,
                    item_depth=3,
                    item_callback=_inner_callback,
                )

            # Finalize the last article.
            _finalizing_article()

        except Exception as e:
            logging.critical(f'Error occurred when processing the file [{xml_file}]: {e}')

        try:
            # Remove XML file.
            os.remove(xml_path)

            # Remove the directory if it becomes empty.
            if not get_file_list(os.path.dirname(xml_path)):
                shutil.rmtree(os.path.dirname(xml_path))

        except:
            logging.error(f'Failed to remove the XML file [{xml_path}].')

        logging.debug(f'Processed (OK): count = {processed_count}')

        return full_warehouse_paths

    def _cleanup_executor(self, warehouse_filename: str):
        logging.debug(f'Warehouse cleaning: {warehouse_filename}')

        try:
            original_file_path = os.path.join(self.output_dir, warehouse_filename)
            if not os.path.exists(original_file_path):
                logging.critical(f'The file {warehouse_filename} [{original_file_path}] does not exist.')
                return None

            compressed_path = original_file_path + COMPRESSION_EXTENSION
            compress_zstd(original_file_path, compressed_path)
            os.remove(original_file_path)

            logging.debug(f'Warehouse cleaned (OK): {warehouse_filename}')

            return compressed_path

        except:
            logging.critical(f'Error occurred when compressing warehouse [{warehouse_filename}].')
            return None

    def build(self):
        """
        Build the blocks after apply the modifiers.
        """
        # Log the version.
        logging.info(f'Builder Version: {get_curr_version()}')

        warehouse = Warehouse(
            output_dir=self.output_dir,
            max_size=self.max_size,
            compress=self.compress,
        )

        start_time = time.time()

        # Prepare the output directory.
        prepare_output_dir(self.output_dir)

        total_count = len(self.files)

        global_temp_dir = os.path.join(self.output_dir, 'temp')
        os.makedirs(global_temp_dir, exist_ok=True)

        # Initialize multiprocessing logger.
        ql, q = _init_logger_multiprocessing(log_level=self.log_level)

        # Tasks is a list of tuples (task_type, args).
        # Task types: 'decompress', 'process', 'warehouse', 'cleanup'
        tasks: List[Tuple[str, Tuple]] = []

        available_process_count = self.num_proc

        modification_pool = mp.Pool(
            processes=self.num_proc,
            initializer=self._worker_initializer,
            initargs=(q,),
        )

        processed_count = 0

        def _decompress_callback(decompressed_files: List[str]):
            if not decompressed_files:
                logging.debug(f'Decompressed (EMPTY): {decompressed_files}')
                return

            decompressed_dir = os.path.dirname(decompressed_files[0])
            logging.debug(f'Decompressed (OK): {decompressed_dir} ({len(decompressed_files)})')

            for decompressed_file in decompressed_files:
                next_task_type = 'process'
                next_args = (decompressed_file,)
                tasks.insert(0, (next_task_type, next_args))

        def _process_callback(full_warehouse_paths: List[Tuple[str, str]]):
            nonlocal processed_count
            processed_count += 1

            logging.info(f'({processed_count / total_count * 100:.2f}% = {processed_count} / {total_count}) | '
                         f'Full warehouses: {len(full_warehouse_paths)}')

            for full_warehouse_path in full_warehouse_paths:
                next_task_type = 'cleanup'
                next_args = (full_warehouse_path,)
                tasks.insert(0, (next_task_type, next_args))

        def _cleanup_callback(cleanup_path):
            logging.info(f'Warehouse packed: {cleanup_path}')

        def _success_callback(task_type, file_path):
            nonlocal available_process_count
            if task_type == 'decompress':
                _decompress_callback(file_path)
            elif task_type == 'process':
                _process_callback(file_path)
            elif task_type == 'cleanup':
                _cleanup_callback(file_path)
            available_process_count += 1

        def _error_callback(e):
            nonlocal available_process_count
            logging.critical(f'Process terminated-level error: {e}')
            available_process_count += 1

        # Built-up initial tasks.
        for curr_file_path in self.files:
            tasks.append(('decompress', (curr_file_path,)))

        # Main semaphore loop.
        while tasks or available_process_count < self.num_proc:
            if tasks and available_process_count > 0:
                available_process_count -= 1
                task_type, args = tasks.pop(0)
                if task_type == 'decompress':
                    file_path, = args
                    modification_pool.apply_async(
                        func=self._decompress_executor,
                        args=(file_path,),
                        callback=partial(_success_callback, task_type),
                        error_callback=_error_callback
                    )
                elif task_type == 'process':
                    xml_file_path, = args
                    modification_pool.apply_async(
                        func=self._process_executor,
                        args=(xml_file_path, warehouse),
                        callback=partial(_success_callback, task_type),
                        error_callback=_error_callback
                    )
                elif task_type == 'cleanup':
                    warehouse_filename, = args
                    modification_pool.apply_async(
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

        logging.info('Main loop finished. Waiting for cleanup...')

        if self.compress:
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
                    modification_pool.apply_async(
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

        # Clean up the global temporary directory.
        cleanup_dir(global_temp_dir)

        end_time = time.time()
        execution_duration = (end_time - start_time) / 60

        # Log the end of the task.
        logging.info(f'All done! Finished all files. (took {execution_duration:.2f} min in total)')
