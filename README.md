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
import time
import src.transform_data

start_time = time.perf_counter()
for path in pathlib.Path("data_input").iterdir():
    if path.is_file():
        src.transform_data.pivot_jsonl(
            input_filepath=path,
            output_filepath=f"temp_storage/{path.stem}.json",
    )
    elif path.is_dir():
        print(f"Skipped directory {path}/") 

print(f"Finished pivoting .jsonl files in {(time.perf_counter()-start_time)/60:,.1f} minutes")
```

```python
import json
import pathlib
import time

import src.discover

table_data = {}

start_time = time.perf_counter()
for path in pathlib.Path("temp_storage").iterdir():
    if path.is_file() and path.suffix==".json":
        with open(path, "r", encoding="utf-8") as file:
            table_data[path.stem] = json.load(file)

print(f"Finished reading in data in {(time.perf_counter()-start_time)/60:,.1f} minutes")

start_time = time.perf_counter()
src.discover.join_keys(
    tbl_contents=table_data,
    n_samples=500,
    allowed_key_types=(int, str),
    output_path="output/discover/join_keys/all_matches.json",
)
print(f"Finished discovering join keys in {(time.perf_counter()-start_time)/60:,.1f} minutes")
```

```python
import time

import src.decision
from src.decision.comparison_operators import greater_than, less_than

start_time = time.perf_counter()
src.decision.join_keys(
    input_data_filepath="output/discover/join_keys/all_matches.json",
    min_match_criteria=(
        ("matches", "exactly_1_match_in_lookup", "percent", (greater_than, 0.1)),
        ("sampled_col", "sample_size", "percent_null", (less_than, 0.95)),
        ("sampled_col", "sample_size", "n_unique/n_rows", (greater_than, 0.5)),
        ("lookup_col", "size", "percent_null", (less_than, 0.95)),
        ("sampled_col", "sample_size", "n_unique", (greater_than, 4)),
        ("lookup_col", "size", "n_unique", (greater_than, 4)),
    ),
    output_filepath="output/decision/join_keys/identified_join_keys.json"
)

print(f"Finished making join key decisions in {(time.perf_counter()-start_time)/60:,.1f} minutes")
```

```python
import src.dataviz
src.dataviz.join_key_decisions_to_csv(
    input_data_filepath="output/decision/join_keys/identified_join_keys.json",
    output_filepath="output/dataviz/join_key_decisions_to_csv/identified_join_keys.csv",
)
```

```python
import json
import time

import src.dataviz

start_time = time.perf_counter()
with open("output/decision/join_keys/identified_join_keys.json", "r", encoding="utf-8") as file:
    join_cols_identified = json.load(file)

src.dataviz.make_sqlite_skeleton(
    col_pairs=join_cols_identified,
    output_db_path="output/dataviz/make_sqlite_skeleton/identified_join_keys.db",
) 
print(f"Exported SQLite db skeleton in {(time.perf_counter()-start_time)/60:,.1f} minutes")
```

```python
import src.discover.table_links
import src.transform_data.table_link_paths_to_csv

src.discover.table_links.create_db(
    input_data_filepath="output/decision/join_keys/identified_join_keys.json",
    max_path_len=4,
    output_filepath="output/discover/table_links/create_db/table_link_paths.pickle",
)

src.transform_data.table_link_paths_to_csv(
    input_data_filepath="output/discover/table_links/create_db/table_link_paths.pickle",
    output_filepath="output/transform_data/table_links_to_csv/table_link_paths.csv",
)
```


