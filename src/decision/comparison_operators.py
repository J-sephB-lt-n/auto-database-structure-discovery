"""
Defines functions for comparing values
"""


def greater_than(x, y) -> bool:
    if x is None or y is None:
        return False
    return x > y


def less_than(x, y) -> bool:
    if x is None or y is None:
        return False
    return x < y
