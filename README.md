# database-schema-discovery
Description TODO

```python
import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
```

```python
import pathlib
import src.transform_data

for filepath in pathlib.Path("data_input").iterdir():
    src.transform_data.pivot_jsonl(
        input_filepath=filepath,
        output_filepath=f"temp_storage/{filepath.stem}.json",
    )    
```

```python
import json
import pathlib
import src.discover

table_data = {}

for filepath in pathlib.Path("temp_storage").iterdir():
    if filepath.suffix==".json":
        with open(filepath, "r", encoding="utf-8") as file:
            table_data[filepath.stem] = json.load(file)

src.discover.join_keys(
    tbl_contents=table_data,
    n_samples=1_000,
    output_path="output/discover/join_keys/all_matches.json",
)
```

```python
import src.decision
from src.decision.comparison_operators import greater_than, less_than

join_cols_identified = src.decision.join_keys(
    input_data_filepath="output/discover/join_keys/all_matches.json",
    min_match_criteria=(
        ("matches", "any_matches_in_lookup", "percent", (greater_than, 0.1)),
        ("sampled_col", "sample_size", "percent_null", (less_than, 0.1)),
        ("lookup_col", "size", "percent_null", (less_than, 0.1)),
        ("sampled_col", "sample_size", "n_unique", (greater_than, 3)),
    ),
)

import src.dataviz

src.dataviz.make_sqlite_skeleton(
    col_pairs=join_cols_identified,
    output_db_path="output/dataviz/make_sqlite_skeleton/identified_join_keys.db",
) 
```


