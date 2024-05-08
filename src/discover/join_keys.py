"""
Defines function src.discover.join_keys()
"""

import json
import itertools
import random

def join_keys(
    tbl_contents: dict[str, dict],
    n_samples: int = 500,
    output_path: str = "output/discovered_join_keys.json",
    verbose: bool = False,
) -> None:
    """Determines (by sampling and brute force) which columns might be candidates for
    joining between multiple tables (i.e. are ID columns in common)

    Notes:
        - None values and strings containing only whitespace are ignored
        - Exact equality is used for comparison, so columns with the same data stored
            as different data types will be considered as non-matching (e.g. "3" != 3).

    Args:
        tbl_contents (dict): The contents of each table, stored as a dictionary
                                (refer to the example below for guidance on data format)
        n_samples (int): Where a table has more than `n_samples` rows, only a random
                        selection of `n_samples` rows from column 1 will be searched
                        for in comparison column 2
        output_path (str): Local path on filesystem to which the output will be written
        verbose (bool): Print diagnostic information to standard out during script run

    Returns:
        None: output is written to a json file on the local filesystem

    Example:
        >>> table_data = {
        ...     "users_table": {
        ...         "id":        [1,2,3,None,5,6,7,8,9],
        ...         "status_id": [0,1,0,0   ,2,2,1,0,0],
        ...     },
        ...     "user_status_table": {
        ...         "user_id":      [1,2,3],
        ...         "status_desc": ["new", "existing", "closed"],
        ...     },
        ...     "transactions_table": {
        ...         "transaction_id": [1,2,3,4,5,6],
        ...         "person": [1,1,3,3,4,8,8,9,9,9,9],
        ...     }
        ... }
        >>> discover_join_keys(tbl_contents=table_data, verbose=True)
        {
            "sampled_col": {
                "table_name": "users_table",
                "column_name": "id",
                "sample_size": {
                    "n_rows": 9,
                    "n_null": 1,
                    "percent_null": 0.11,
                    "n_unique": 8,
                },
            "lookup_col": {
                "table_name": "user_status_table",
                "column_name": "user_id",
                "size": {
                    "n_rows": 3,
                    "n_null": 0,
                    "percent_null": 0.0,
                    "n_unique": 3,
                },
            "matches": {
                "any_matches_in_lookup": {
                    "n": 3,
                    "percent": 0.33,
                },
                "exactly_1_match_in_lookup": {
                    "n": 3,
                    "percent": 0.33,
                },
            }
        }
        ...
    """
    results: list[dict] = []
    table_names: tuple[str, ...] = tuple(tbl_contents.keys())
    comparison_pairs = itertools.combinations(tbl_contents.items(), 2)
    comparison_counter = itertools.count(1)
    for (sample_tbl_name, sample_tbl_data), (
        lookup_tbl_name,
        lookup_tbl_data,
    ) in comparison_pairs:
        for sample_colname, sample_coldata in sample_tbl_data.items():
            for lookup_colname, lookup_coldata in lookup_tbl_data.items():
                if verbose:
                    print(
                        "sample_tbl_name:",
                        sample_tbl_name,
                        "sample_colname:",
                        sample_colname,
                    )
                    print(
                        "lookup_tbl_name:",
                        lookup_tbl_name,
                        "lookup_colname:",
                        lookup_colname,
                    )
                if len(sample_coldata) > n_samples:
                    sample = random.sample(sample_coldata, k=n_samples)
                else:
                    sample = sample_coldata
                sample_n_null: int = sum([1 if x is None else 0 for x in sample])
                lookup_n_null: int = sum(
                    [1 if x is None else 0 for x in lookup_coldata]
                )
                sample_match_counts = {val: 0 for val in sample if val is not None}
                for val in lookup_coldata:
                    if val in sample_match_counts:
                        sample_match_counts[val] += 1
                n_in_sample_have_any_matches = sum(
                    [1 for x in sample_match_counts.values() if x > 0]
                )
                n_in_sample_have_exactly_1_match = sum(
                    [1 for x in sample_match_counts.values() if x == 1]
                )
                results.append(
                    {
                        "sampled_col": {
                            "table_name": sample_tbl_name,
                            "column_name": sample_colname,
                            "sample_size": {
                                "n_rows": len(sample),
                                "n_null": sample_n_null,
                                "percent_null": round(sample_n_null / len(sample), 2),
                                "n_unique": len(sample_match_counts),
                            },
                        },
                        "lookup_col": {
                            "table_name": lookup_tbl_name,
                            "column_name": lookup_colname,
                            "size": {
                                "n_rows": len(lookup_coldata),
                                "n_null": lookup_n_null,
                                "percent_null": round(
                                    lookup_n_null / len(lookup_coldata)
                                ),
                                "n_unique": len(set(lookup_coldata)),  # expensive?
                            },
                        },
                        "matches": {
                            "any_matches_in_lookup": {
                                "n": n_in_sample_have_any_matches,
                                "percent": round(
                                    n_in_sample_have_any_matches
                                    / len(sample_match_counts),
                                    2,
                                ),
                            },
                            "exactly_1_match_in_lookup": {
                                "n": n_in_sample_have_exactly_1_match,
                                "percent": round(
                                    n_in_sample_have_exactly_1_match
                                    / len(sample_match_counts),
                                    2,
                                ),
                            },
                        },
                    }
                )
                if verbose:
                    print(json.dumps(results[-1], indent=4, default=str))
        print(
            f"Completed comparison {next(comparison_counter):,} of {len(comparison_pairs):,}"
        )
    with open(output_path, "w", encoding="utf-8") as file:
        json.dump(results, file, indent=4)
