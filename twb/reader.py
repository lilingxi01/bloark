from typing import Callable


class BlocksReader:
    def __init__(self, path: str):
        """
        :param path: the path of the blocks
        """
        self.path = path

    def map(self, func: Callable[[dict], None]):
        """
        Map a function to each block.
        :param func: the function to be mapped
        """
        # TODO: Implement this method.
        for block in self:
            func(block)
