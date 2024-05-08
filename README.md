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
        output_filepath=f"temp_storage/{filepath.name}",
    )    
```
