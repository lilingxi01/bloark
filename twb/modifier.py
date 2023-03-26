import logging
from abc import ABC, abstractmethod
from typing import Union

_DEFAULT_NUM_PROC = 1
_DEFAULT_LOG_LEVEL = logging.INFO


class Modifier(ABC):
    """
    The core class to define how to modify the JSON content.
    """
    @abstractmethod
    def modify(self, content: dict) -> Union[dict, None]:
        """
        Returns a list of batches of URLs to download.
        :param content: The JSON content to be modified.
        :return: Modified JSON content. Return None if the content should be removed.
        """
        pass
