"""
Function src.dataviz.join_key_decisions_to_csv.join_key_decisions_to_csv()
"""

import csv
import json


def join_key_decisions_to_csv(
    input_data_filepath: str,
    output_filepath: str,
) -> None:
    """Exporting the discovered join keys as a CSV file

    Args:
        input_data_filepath (str): .json file containing identified join columns
                (this file created by src.decision.join_keys.join_keys())
        output_filepath (str): output .csv file will be written to here

    Returns:
        None: output is written to a .csv file at `output_filepath`
    """
    with open(input_data_filepath, "r", encoding="utf-8") as file:
        col_pairs: list[list[str]] = json.load(file)

    with open(output_filepath, mode="w", encoding="utf-8") as file:
        csv_writer = csv.DictWriter(
            file,
            fieldnames=["src_tbl", "src_col", "dst_tbl", "dst_col"],
            delimiter=",",
            quotechar='"',
            quoting=csv.QUOTE_MINIMAL,
        )
        csv_writer.writeheader()
        for col_pair in col_pairs:
            csv_writer.writerow(
                {
                    "src_tbl": col_pair[0][0],
                    "src_col": col_pair[0][1],
                    "dst_tbl": col_pair[1][0],
                    "dst_col": col_pair[1][1],
                }
            )
