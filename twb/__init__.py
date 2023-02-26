import os
import shutil
import time
from typing import Tuple, List
import xmltodict
import py7zr
from multiprocessing import Pool

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
    # TODO: Implement the processor here.

    tag_name = path[-1][0]
    item_type = type(item)

    if tag_name != 'page':
        return True

    if item_type is dict:
        print('item:', item.keys())
    else:
        print('item:', item.strip())

    processed_item = processor.parse(tag=tag_name, meta={}, tree=item)
    results.append(processed_item)

    return True


def parse_xml(path: str, processor: BlockInteriorProcessor) -> List[dict]:
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
