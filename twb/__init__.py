import os
import shutil
import time
from typing import Tuple, List
import xmltodict
import py7zr
from multiprocessing import Pool
import jsonlines

from .utils import get_file_list, clean_existed_files
from .bip import BlockInteriorProcessor, DefaultBIP


class TemporalWikiBlocks:
    """
    A class to make the blocks of a TemporalWiki page.
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
              num_proc: int = 5):
        """
        Build the blocks.
        :param processor: the interior processor for blocks
        :param output_dir: the output directory
        :param num_proc: the number of processes
        :raise Warning: if there is no file to process
        """
        zip_file_list = self.files

        # If there is no file to process, raise a warning and return.
        if len(zip_file_list) == 0:
            raise Warning('There is no file to process.')

        # If the override flag is set, clean the existed files before processing.
        clean_existed_files(file_list=zip_file_list, output_dir=output_dir)

        # Create the temporary directory for compression.
        compression_temp_dir = os.path.join(output_dir, 'compression_temp')
        if not os.path.exists(compression_temp_dir):
            os.makedirs(compression_temp_dir)

        # Create the temporary directory for decompression.
        decompression_temp_dir = os.path.join(output_dir, 'decompression_temp')
        if not os.path.exists(decompression_temp_dir):
            os.makedirs(decompression_temp_dir)

        # Decompress the files in parallel if the number of processes is greater than 1.
        print('[Build] Start decompressing files.')

        start_time = time.time()

        with Pool(processes=num_proc) as pool:
            pool.map(decompress_file, [(path, decompression_temp_dir) for path in zip_file_list])

        end_time = time.time()
        execution_duration = end_time - start_time
        print(f"[Build] Finish decompressing files. (Took {execution_duration:.2f} seconds in total)")

        all_results = []

        # Get all files in the decompression directory.
        decompressed_file_list = get_file_list(decompression_temp_dir)
        for path in decompressed_file_list:
            results = parse_xml(path, processor)
            all_results.extend(results)

        # Divide the results into blocks.
        blocks = divide_into_blocks(original_list=all_results, count_per_block=50)

        # Save the results to a JSONL file.
        store_to_jsonl(blocks=blocks, compression_temp_dir=compression_temp_dir)

        # List all the files in the compression directory.
        compressed_file_list = get_file_list(compression_temp_dir)
        print(compressed_file_list)

        # Clean up the temporary directory.
        shutil.rmtree(compression_temp_dir)
        shutil.rmtree(decompression_temp_dir)


def decompress_file(config: Tuple[str, str]):
    """
    Decompress a file.
    :param config: the configuration of the decompression in the form of (input_path, output_path)
    :return:
    """
    path, output_path = config

    print('[Build] >>> Decompressing:', path, output_path)

    if not path.endswith('.7z'):
        print(f'[Build] >>> Skipped because the file is not a 7z file. ({path})')
        return

    with py7zr.SevenZipFile(path, mode='r') as z:
        start_time = time.time()
        z.extractall(path=output_path)
        end_time = time.time()

        execution_duration = end_time - start_time
        print(f"[Build] >>> Decompression done: {path} -- {execution_duration:.2f} seconds")


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


def store_to_jsonl(blocks: List[List[dict]], compression_temp_dir: str) -> List[str]:
    """
    Store the blocks to a JSONL file.
    :param blocks: the blocks to store
    :param compression_temp_dir: the directory to store the JSONL files
    :return: the list of JSONL file paths
    """
    jsonl_files = []  # Store all the JSONL file paths for future compression.

    block_range = range(len(blocks))
    num_digits = len(str(len(blocks)))

    for i in block_range:
        curr_path = os.path.join(compression_temp_dir, f'block_{str(i).zfill(num_digits)}.jsonl')
        with jsonlines.open(curr_path, 'w') as writer:
            writer.write_all(blocks[i])
        jsonl_files.append(curr_path)

    return jsonl_files
