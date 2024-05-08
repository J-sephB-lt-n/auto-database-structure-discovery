"""
Defines function pivot_jsonl()
"""

import json
import logging

logger = logging.getLogger(__name__)


def pivot_jsonl(input_filepath: str, output_filepath: str) -> None:
    """Converts data stored as newline-delimited JSON (i.e. a
    list of dicts, where each dict is a table row) into a JSON file in
    which the data is stored in columns (i.e. the format
    expected by the function src/discover_join_keys.py)

    Args:
        input_filepath (str): location of input .jsonl file
        output_filepath (str): desired location of output .json file

    Returns:
        None: output is written to a local .json file
    """
    logger.info("Importing file [%s]", input_filepath)
    coldata = dict()
    invalid_data_type_colnames: set[str] = set()
    with open(input_filepath, "r", encoding="utf-8") as file:
        lines: list[dict] = []
        for line in file.readlines():
            linedict = json.loads(line.rstrip())
            lines.append(linedict)
            for key, value in linedict.items():
                if key not in coldata:
                    coldata[key] = []
                if type(value) not in (str, int, float, bool, None):
                    invalid_data_type_colnames.add(key)

    if len(invalid_data_type_colnames) > 0:
        logger.warning(
            "The following columns contain complex or nested data types and have been ommitted from the output:\n%s",
            invalid_data_type_colnames,
        )

    coldata = {
        key: val
        for key, val in coldata.items()
        if key not in invalid_data_type_colnames
    }

    for line in lines:
        for colname in coldata:
            coldata[colname].append(line.get(colname))

    assert (
        len({len(col_contents) for col_contents in coldata.values()}) == 1
    ), "All columns must contain the same number of rows"

    with open(output_filepath, "w", encoding="utf-8") as file:
        json.dump(coldata, file, indent=4)

    logger.info("Exported file [%s]", output_filepath)
