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

table_data = dict()

for filepath in pathlib.Path("temp_storage").iterdir():
    if filepath.suffix==".json":
        with open(filepath, "r", encoding="utf-8") as file:
            table_data[filepath.stem] = json.load(file)

src.discover.join_keys(
    tbl_contents=table_data,
    n_samples=1_000,
    output_path="output/discover_join_keys/all_matches.json",
)
```
