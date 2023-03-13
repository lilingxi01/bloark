import os
import shutil
import time
from typing import List, Union, Callable
import xmltodict
import py7zr
import jsonlines
import uuid

from .utils import get_file_list, compress_zstd, get_memory_consumption, cleanup_dir
from .bip import BlockInteriorProcessor, DefaultBIP
from .parallelization import RDSProcessManager, RDSProcessController


class Builder:
    """
    The core class to generate the blocks from Wikipedia Edit History chunk.

    Basic usage:

        import twb
        builder = twb.Builder()  # Create an instance.
        builder.preload('./test/sample_data/minimal_sample.xml')  # Preload the files to be processed.
        builder.build('./test/output')  # Build the blocks.

    You must preload the files to be processed before building the blocks.
    Preload function accepts a path to a file or a directory, and can be called multiple times (for multiple files).

    Attributes:
        files (List[str]): The list of files to be processed. It will be finalized when the build() method is called.
    """

    def __init__(self):
        self.files = []

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

    def build(self,
              output_dir: str,
              processor: BlockInteriorProcessor = DefaultBIP(),
              num_proc: Union[int, None] = 1,
              articles_per_block: int = 100,
              start_index: int = 0,
              compress: bool = True):
        """
        Build the blocks.
        :param processor: the interior processor for blocks (default: DefaultBIP)
        :param output_dir: the output directory for the blocks (will be created if not exists)
        :param num_proc: the number of processes (default: 1) (Set to None to use all available processes)
        :param articles_per_block: the number of articles per block (default: 50)
        :param start_index: the starting index of the blocks (default: 0)
        :param compress: whether to compress the blocks (default: True)
        :raise Warning: if there is no file to process
        """
        zip_file_list = self.files

        # If there is no file to process, raise a warning and return.
        if len(zip_file_list) == 0:
            raise Warning('There is no file to process.')

        # If output dir exists, remove it first.
        if os.path.exists(output_dir):
            shutil.rmtree(output_dir)

        # Create the output directory.
        os.makedirs(output_dir)

        # Create the temporary directory.
        temp_dir = os.path.join(output_dir, 'temp')
        os.makedirs(temp_dir)

        # Decompress the files in parallel.
        print('[Build] Start decompressing files.')

        process_manager_context = {
            'processor': processor,  # The interior processor for blocks.
            'compress': compress,  # Whether to compress the blocks.
            'articles_per_block': articles_per_block,  # Number of articles per block.
            'total_count': len(zip_file_list),  # Total number of files to process.
        }

        start_time = time.time()

        pm = RDSProcessManager(
            num_proc=num_proc,
            start_index=start_index,
        )
        for path in zip_file_list:
            pm.apply_async(
                executable=_file_processor,
                args=(path, output_dir, process_manager_context)
            )

        pm.close()
        pm.join()

        end_time = time.time()
        execution_duration = end_time - start_time
        print(f"[Build] All done. (Took {execution_duration:.2f} seconds in total)")

        # Clean up the temporary directory.
        cleanup_dir(temp_dir)


def _file_processor(controller: RDSProcessController,
                    path: str,
                    output_dir: str,
                    context: dict):
    # If the file does not exist, skip it.
    if not os.path.exists(path):
        controller.logerr(f'File does not exist: {path}')
        return

    # If the file is not a 7z file, skip it.
    if not path.endswith('.7z'):
        controller.logwarn(f'Skipped because the file is not a 7z file. ({path})')
        return

    # Get the block interior processor from the running context.
    block_interior_processor = context['processor'] if 'processor' in context else DefaultBIP()
    should_compress = context['compress'] if 'compress' in context else True
    articles_per_block = context['articles_per_block'] if 'articles_per_block' in context else 100
    total_count = context['total_count'] if 'total_count' in context else 100

    # Generate a temporary id for the file.
    temp_id = uuid.uuid4().hex

    # Create the temporary directory for this task (associated with this temp_id).
    temp_dir = os.path.join(output_dir, 'temp', temp_id)
    os.makedirs(temp_dir)

    # Create the temporary directories for compression and decompression.
    compression_temp_dir = os.path.join(temp_dir, 'compression_temp')
    os.makedirs(compression_temp_dir)
    decompression_temp_dir = os.path.join(temp_dir, 'decompression_temp')
    os.makedirs(decompression_temp_dir)

    very_start_time = time.time()

    # If the temporary directories cannot be created, skip it.
    if not os.path.exists(compression_temp_dir) or not os.path.exists(decompression_temp_dir):
        controller.logerr(f'Failed to create temporary directories for archive: {path}')
        return

    # First try block, catching issues during decompression.
    try:
        # Decompress the file. It has multi-threading support built-in and enabled by default.
        with py7zr.SevenZipFile(path, mode='r', mp=False) as z:
            start_time = time.time()
            z.extractall(path=decompression_temp_dir)
            end_time = time.time()
            execution_duration = end_time - start_time
            controller.loginfo(f'Decompression took {execution_duration:.2f} seconds. ({path})')
        decompressed_files = get_file_list(decompression_temp_dir)
    except Exception as e:
        controller.logerr(f'Decompression failed at {path} with error:', e)
        cleanup_dir(decompression_temp_dir)
        return

    # Second try block, catching issues during processing (XML parsing, etc).
    try:
        def get_new_output_path():
            """
            Get the path of the next output file.
            """
            nonlocal controller, compression_target_dir
            curr_index = str(controller.declare_index()).zfill(5)
            return os.path.join(compression_target_dir, f'block_{curr_index}.jsonl')

        compression_target_dir = compression_temp_dir if should_compress else output_dir

        total_article_count = 0
        article_count = 0
        memory_usage_records = []
        all_memory_usage_records = []
        curr_output_path = get_new_output_path()

        def _super_callback(article: dict):
            """
            The super callback for the XML parser.
            """
            nonlocal total_article_count, article_count, memory_usage_records, curr_output_path
            if article_count >= articles_per_block:
                # Store the max memory usage in previous batch.
                all_memory_usage_records.append(max(memory_usage_records))
                # Reset the counters.
                article_count = 0
                memory_usage_records = []
                curr_output_path = get_new_output_path()
            article_count += 1
            total_article_count += 1
            _store_article_to_jsonl(article=article, output_path=curr_output_path)
            memory_usage_records.append(get_memory_consumption())

        # We store articles into JSONL files in a streaming manner, along the way of parsing XML files.
        # Therefore, we don't have to keep all the results in the memory, which is a huge problem.
        for path in decompressed_files:
            _parse_xml(path=path, processor=block_interior_processor, super_callback=_super_callback)

        if len(memory_usage_records) > 0:
            all_memory_usage_records.append(max(memory_usage_records))

        controller.loginfo(f'Parsing done. {total_article_count} articles processed in total.'
                           f'{total_article_count / articles_per_block:.2f} blocks created in total.',
                           f'(Highest Memory: {max(all_memory_usage_records)} MB)')

        cleanup_dir(decompression_temp_dir)  # Delete temporary files used for decompression.

        if should_compress:
            # Store the results to JSONL files.
            json_files = get_file_list(compression_temp_dir)

            # Compress the files.
            for json_file in json_files:
                _compress_file(path=json_file, output_dir=output_dir, controller=controller)

            controller.loginfo(f'Compression done. {len(json_files)} files compressed in total.')
    except Exception as e:
        controller.logerr(f'Parsing failed at {path}:', e)
    finally:
        # Clean up the temporary directory.
        cleanup_dir(decompression_temp_dir)
        cleanup_dir(compression_temp_dir)
        controller.loginfo(f'Cleaned up folder for temporary ID: {temp_id}')

        very_end_time = time.time()
        total_execution_duration = very_end_time - very_start_time
        curr_processed_count = controller.count_forward()
        curr_processed_progress = curr_processed_count / total_count * 100
        controller.logprogress(f'({curr_processed_progress:.2f}%) Finished processing {temp_id} @ {path}.',
                               f'Took {total_execution_duration:.2f} seconds in total for this archive.')


def _xml_parser_callback(path, item, processor: BlockInteriorProcessor, super_callback: Callable[[dict], None]):
    """
    The callback function for the XML parser.
    :param path: the path of the current item
    :param item: children dictionary
    :param processor: the interior processor for blocks
    :return: True if the parsing should continue, False otherwise
    """
    tag_name = path[-1][0]
    item_type = type(item)

    # If the item is a page, we don't need to process it.
    if tag_name != 'page':
        return True

    # If the item is not a dictionary, it means that the item is a leaf node, and we don't expect it to be a block.
    if item_type is not dict:
        return True

    processed_item = processor.parse(tag=tag_name, meta={}, tree=item)

    # If the item is not None, it means that the item is a block and we should append it to the results.
    if processed_item is not None:
        super_callback(processed_item)

    return True


def _parse_xml(path: str, processor: BlockInteriorProcessor, super_callback: Callable[[dict], None]):
    """
    Parse the XML file into a list of JSON objects.
    :param path: the path of the XML file
    :param processor: the interior processor for blocks
    :return: the list of parsed results
    """
    with open(path, 'rb') as xml_file:
        xmltodict.parse(
            xml_file,
            item_depth=processor.read_depth,
            item_callback=lambda x, y: _xml_parser_callback(x, y, processor, super_callback)
        )


def _store_article_to_jsonl(article: dict, output_path: str):
    """
    Store the blocks to a JSONL file.
    :param article: the article to store
    :param output_path: the path to store the JSONL file
    :return: the list of JSONL file paths
    """
    with jsonlines.open(output_path, 'a') as writer:
        writer.write(article)


# The extension of the compressed file.
compression_extension = '.zst'


def _compress_file(path: str, output_dir: str, controller: RDSProcessController):
    """
    Compress a file.
    :param path: the path of the file to compress
    :param output_dir: the directory to store the compressed file
    :return:
    """
    # If the file is not a JSONL file, skip it.
    if not path.endswith('.jsonl'):
        controller.logwarn(f'Skipped because the file is not a JSONL file. ({path})')
        return

    output_path = os.path.join(output_dir, os.path.basename(path) + compression_extension)

    try:
        # Compress the file using Zstandard.
        # TODO: Might be able to specify multiprocessing or multithreading. Check the documentation.
        #  If possible, we should use multithreading in order to utilize the CPU cores in this case.
        compress_zstd(input_path=path, output_path=output_path)
    except Exception as e:
        controller.logerr(f'Compression failed at {path}:', e)
