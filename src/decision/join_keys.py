"""
Function src.decision.join_keys.join_keys()
"""

import json
from typing import Callable

from src.navigate_data import fetch_from_dict


def greater_than(x, y) -> bool:
    if x is None or y is None:
        return False
    return x > y


def less_than(x, y) -> bool:
    if x is None or y is None:
        return False
    return x < y


def join_keys(
    input_data_filepath: str,
    min_match_criteria: tuple[tuple, ...],
    output_filepath: str
) -> None:
    """Reads in the output of the function
    src.discover.join_keys.join_keys() and decides which
    column pairs show sufficient evidence to be considered
    as joinable ID columns

    Args:
        input_data_filepath (str): TODO
        min_match_criteria (tuple): TODO
        output_filepath (str): TODO

    Returns:
        None: output is exported to a .json file at `output_filepath`, in
                    the format expected by the argument `col_pairs` of function
                     src.dataviz.make_sqlite_skeleton.make_sqlite_skeleton()
    """
    with open(input_data_filepath, "r", encoding="utf-8") as file:
        match_data = json.load(file)

    results: list[tuple] = []

    for match_pair in match_data:
        criterion_violations: int = 0
        for crit in min_match_criteria:
            comparison_func = crit[-1][0]
            threshold = crit[-1][1]
            obs_value = fetch_from_dict(dct=match_pair, selectors=crit[:-1])
            if not comparison_func(obs_value, threshold):
                criterion_violations += 1
                break
        if criterion_violations == 0:
            results.append(
                (
                    (
                        match_pair["sampled_col"]["table_name"],
                        match_pair["sampled_col"]["column_name"],
                    ),
                    (
                        match_pair["lookup_col"]["table_name"],
                        match_pair["lookup_col"]["column_name"],
                    ),
                )
            )

    print(f"Discovered {len(results)} join key pairs")

    with open(output_filepath, "w", encoding="utf-8") as file:
        json.dump(results, file, indent=4)

