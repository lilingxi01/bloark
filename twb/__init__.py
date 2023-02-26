import os
import shutil
import time
from typing import List, Union
import xmltodict
import py7zr
import jsonlines

from .utils import get_file_list, compress_zstd, get_estimated_size
from .bip import BlockInteriorProcessor, DefaultBIP
from .parallelization import RDSProcessManager, RDSProcessController

import cuid


class TemporalWikiBlocks:
    """
    The core class to generate the blocks from Wikipedia Edit History chunk.

    Basic usage:

        twb = TemporalWikiBlocks()  # Create an instance.
        twb.preload('./test/sample_data/minimal_sample.xml')  # Preload the files to be processed.
        twb.build('./test/output')  # Build the blocks.

    You must preload the files to be processed before building the blocks.
    Preload function accepts a path to a file or a directory, and can be called multiple times (for multiple files).
    """

    def __init__(self):
        self.files = []
        self.blocks = []
        self.block_count = 0

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
        self.files.extend(get_file_list(path))

    def build(self,
              output_dir: str,
              processor: BlockInteriorProcessor = DefaultBIP(),
              total_space: Union[int, None] = None,
              num_proc: Union[int, None] = None,
              compress: bool = True):
        """
        Build the blocks.
        :param processor: the interior processor for blocks (default: DefaultBIP)
        :param output_dir: the output directory for the blocks (will be created if not exists)
        :param total_space: the total space for the temporary files (default: all available space on the disk)
        :param num_proc: the number of processes (default: number of CPUs)
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

        # Create the temporary directory for compression.
        compression_temp_dir = os.path.join(output_dir, 'compression_temp')
        os.makedirs(compression_temp_dir)

        # Create the temporary directory for decompression.
        decompression_temp_dir = os.path.join(output_dir, 'decompression_temp')
        os.makedirs(decompression_temp_dir)

        # Decompress the files in parallel.
        print('[Build] Start decompressing files.')

        process_manager_context = {
            'processor': processor,
            'compress': compress,
        }

        # Determine the total space for the temporary files.
        total_available_space = shutil.disk_usage(output_dir).free if total_space is None else total_space

        # Display the total available space in GB.
        total_available_space_gb = total_available_space / 1024 / 1024 / 1024
        print('[Build] RDS space limitation:', round(total_available_space_gb, 2), 'GB.')

        process_manager = RDSProcessManager(
            executable=file_processor,
            total_space=total_available_space,
            num_proc=num_proc,
            context=process_manager_context
        )
        for path in zip_file_list:
            process_manager.register(path, output_dir, get_estimated_size(path))

        start_time = time.time()
        process_manager.start()
        end_time = time.time()
        execution_duration = end_time - start_time
        print(f"[Build] Finish decompressing files. (Took {execution_duration:.2f} seconds in total)")

        # Clean up the temporary directory.
        shutil.rmtree(compression_temp_dir)
        shutil.rmtree(decompression_temp_dir)


def file_processor(path: str, output_dir: str, controller: RDSProcessController, context: dict):
    # If the file is not a 7z file, skip it.
    if not path.endswith('.7z'):
        print(f'[Build] >>> Skipped because the file is not a 7z file. ({path})')
        return

    print('[Build] >>> Decompressing:', path)

    # Get the block interior processor from the running context.
    block_interior_processor = context['processor'] if 'processor' in context else DefaultBIP()
    should_compress = context['compress'] if 'compress' in context else True

    # Generate a UUID.
    temp_id = cuid.cuid()

    compression_temp_dir = os.path.join(output_dir, 'compression_temp', temp_id)
    os.makedirs(compression_temp_dir)

    decompression_temp_dir = os.path.join(output_dir, 'decompression_temp', temp_id)
    os.makedirs(decompression_temp_dir)

    # Decompress the file.
    with py7zr.SevenZipFile(path, mode='r') as z:
        start_time = time.time()
        z.extractall(path=decompression_temp_dir)
        end_time = time.time()

        execution_duration = end_time - start_time
        print(f"[Build] >>> Decompression done: {path} -- {execution_duration:.2f} seconds")

    decompressed_files = get_file_list(decompression_temp_dir)
    all_results = []
    for path in decompressed_files:
        results = parse_xml(path=path, processor=block_interior_processor)
        all_results.extend(results)

    # Divide the results into blocks.
    blocks = divide_into_blocks(original_list=all_results, count_per_block=50)

    if should_compress:
        # Store the results to JSONL files.
        json_files = store_to_jsonl(blocks=blocks, output_dir=compression_temp_dir, controller=controller)

        # Compress the files.
        for json_file in json_files:
            compress_file(path=json_file, output_dir=output_dir)
    else:
        # Store the results to JSONL files.
        store_to_jsonl(blocks=blocks, output_dir=output_dir, controller=controller)

    # Delete temporary files.
    shutil.rmtree(compression_temp_dir)
    shutil.rmtree(decompression_temp_dir)

    # Release the RDS process controller for freeing up the disk space constraint.
    controller.release()


def xml_parser_callback(path, item, processor: BlockInteriorProcessor, results: list):
    """
    The callback function for the XML parser.
    :param path: the path of the current item
    :param item: children dictionary
    :param processor: the interior processor for blocks
    :param results: the list of results (to be appended onto)
    :return: True if the parsing should continue, False otherwise
    """
    tag_name = path[-1][0]
    item_type = type(item)

    # If the item is a page, we don't need to process it.
    if tag_name != 'page':
        return True

    # If the item is not a dictionary, it means that the item is a leaf node and we don't expect it to be a block.
    if item_type is not dict:
        return True

    processed_item = processor.parse(tag=tag_name, meta={}, tree=item)

    # If the item is not None, it means that the item is a block and we should append it to the results.
    if processed_item is not None:
        results.append(processed_item)

    return True


def parse_xml(path: str, processor: BlockInteriorProcessor) -> List[dict]:
    """
    Parse the XML file into a list of JSON objects.
    :param path: the path of the XML file
    :param processor: the interior processor for blocks
    :return: the list of parsed results
    """
    results = []

    with open(path, 'rb') as xml_file:
        try:
            xmltodict.parse(
                xml_file,
                item_depth=processor.read_depth,
                item_callback=lambda x, y: xml_parser_callback(x, y, processor, results)
            )
        except xmltodict.ParsingInterrupted as e:
            # We don't need to handle this exception because this should be intended.
            pass

    # Return the results as a list of JSON objects (so that they can be thrown into a JSONL file).
    return results


def divide_into_blocks(original_list: List[dict], count_per_block: int = 50) -> List[List[dict]]:
    """
    Divide the original list into blocks.
    :param original_list: the original list
    :param count_per_block: the number of items per block
    :return: the list of blocks
    """
    # Divide original_list into blocks every count_per_block items.
    return [original_list[i:i + count_per_block] for i in range(0, len(original_list), count_per_block)]


def store_to_jsonl(blocks: List[List[dict]], output_dir: str, controller: RDSProcessController) -> List[str]:
    """
    Store the blocks to a JSONL file.
    :param blocks: the blocks to store
    :param output_dir: the directory to store the JSONL files
    :param controller: the controller for the process
    :return: the list of JSONL file paths
    """
    curr_index = str(controller.declare_index()).zfill(5)

    jsonl_files = []  # Store all the JSONL file paths for future compression.

    for i in range(len(blocks)):
        curr_path = os.path.join(output_dir, f'block_{curr_index}.jsonl')
        with jsonlines.open(curr_path, 'w') as writer:
            writer.write_all(blocks[i])
        jsonl_files.append(curr_path)

    return jsonl_files


# The extension of the compressed file.
compress_extension = '.zst'


def compress_file(path: str, output_dir: str):
    """
    Compress a file.
    :param path: the path of the file to compress
    :param output_dir: the directory to store the compressed file
    :return:
    """
    # If the file is not a JSONL file, skip it.
    if not path.endswith('.jsonl'):
        print(f'[Build] >>> Skipped because the file is not a JSONL file. ({path})')
        return

    output_path = os.path.join(output_dir, os.path.basename(path) + compress_extension)

    print('[Build] >>> Compressing:', path)

    # Compress the file using Zstandard.
    start_time = time.time()
    compress_zstd(input_path=path, output_path=output_path)
    end_time = time.time()
    execution_duration = end_time - start_time

    file_size = os.path.getsize(path)  # Get current file size.
    compressed_file_size = os.path.getsize(output_path)  # Get compressed file size.
    compression_ratio = compressed_file_size / file_size  # Calculate compression ratio.
    print(f"[Build] >>> Compression done: {output_path} -- {execution_duration:.2f} seconds")
    print(f"[Build] >>> >>> Compression ratio: ({compressed_file_size}/{file_size}) = {compression_ratio:.2f}x")
