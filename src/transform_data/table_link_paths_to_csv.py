"""
Function src.transform_data.table_link_paths_to_csv.table_link_paths_to_csv()
"""

import csv
import pickle


def table_link_paths_to_csv(input_data_filepath: str, output_filepath: str):
    """Converts the table link pathes database from .pickle to a
    more user-friendly .csv representation
    """
    with open(input_data_filepath, "rb") as file:
        paths_db = pickle.load(file)

    with open(output_filepath, mode="w", encoding="utf-8") as file:
        csv_writer = csv.DictWriter(
            file,
            fieldnames=["source_table", "destination_table", "path"],
            delimiter=",",
            quotechar='"',
            quoting=csv.QUOTE_MINIMAL,
        )
        csv_writer.writeheader()
        for (src_tbl_name, dest_tbl_name), paths in paths_db.items():
            for path in paths:
                csv_writer.writerow(
                    {
                        "source_table": src_tbl_name,
                        "destination_table": dest_tbl_name,
                        "path": path,
                    }
                )
