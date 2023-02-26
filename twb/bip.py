from abc import ABC, abstractmethod
from typing import Dict, Any, Union


class BlockInteriorProcessor(ABC):
    """
    Block interior processor (BIP) is a class that processes the interior of a block.
    """
    def __init__(self, read_depth: int = 2):
        """
        Create an explainer.
        :param read_depth: the depth of the tree to read (only that level and beyond will be read)
        """
        self.read_depth = read_depth

    @abstractmethod
    def parse(self, tag: str, meta: Dict[str, str], tree: Dict[str, Any]) -> Union[Dict[str, Any], None]:
        pass


class DefaultBIP(BlockInteriorProcessor):
    """
    Default BIP.
    """
    def parse(self, tag: str, meta: Dict[str, str], tree: Dict[str, Any]) -> Union[Dict[str, Any], None]:
        return tree
