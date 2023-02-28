import os
from typing import Callable, Union

from .parallelization import RDSProcessController, RDSProcessManager
from .utils import get_file_list, decompress_zstd, get_estimated_size, compute_total_available_space


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
        files (list): A list of files to be read.
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

    def decompress(self,
                   output_dir: str,
                   total_space: Union[int, None] = None,
                   num_proc: Union[int, None] = None):
        """
        Decompress the files.
        :param output_dir: the output directory
        :param total_space: the total available space in bytes
        :param num_proc: the number of processes to use
        """

        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # Determine the total space for the temporary files.
        total_available_space = compute_total_available_space(total_space=total_space, output_dir=output_dir)

        process_manager = RDSProcessManager(
            executable=_decompress_processor,
            total_space=total_available_space,
            num_proc=num_proc
        )
        for file in self.files:
            process_manager.register(path=file, output_dir=output_dir, space=get_estimated_size(file))
        process_manager.start()

    def map(self, func: Callable[[dict], None]):
        """
        Map a function to each block.
        :param func: the function to be mapped
        """
        # TODO: Implement this method.
        pass


def _decompress_processor(path: str, output_dir: str, controller: RDSProcessController, context: dict):
    _decompress_file(path, output_dir)
    controller.release()


def _decompress_file(path: str, output_dir: str):
    original_name = os.path.split(path)[1]
    decompressed_name = original_name.replace('.zst', '')
    decompressed_path = os.path.join(output_dir, decompressed_name)
    decompress_zstd(path, decompressed_path)
