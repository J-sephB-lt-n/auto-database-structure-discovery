"""
Function src.navigate_data.fetch_from_dict.fetch_from_dict() 
"""

import functools
import operator
from typing import Any


def fetch_from_dict(dct, selectors: list[Any]):
    """Fetch a nested value from a dictionary by specifying the path of nested keys

    Notes:
        I got this function from https://www.30secondsofcode.org/python/s/get/

    Args:
        dct (dict): The dictionary to traverse
        selectors (list): Ordered list of keys leading to the desired nested value to extract

    Example:
        >>> users = {
        ...   'freddy': {
        ... 	'name': {
        ... 	  'first': 'fred',
        ... 	  'last': 'smith'
        ... 	},
        ... 	'postIds': [1, 2, 3]
        ...   }
        ... }
        >>> fetch_from_dict(users, ['freddy', 'name', 'last'])
        smith
        >>> fetch_from_dict(users, ['freddy', 'postIds', 1])
        2
    """
    return functools.reduce(operator.getitem, selectors, dct)
