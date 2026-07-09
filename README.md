# xHou Unified Binary Data Format (.xhd)

A unified, robust, and high-performance custom binary archive format designed for active simulations (such as astronomy/physics systems), game save states, and web analytics databases. 

## Features
- **ZIP-Style Central Directory**: Prevents data corruption during continuous streaming and in-place updates.
- **Strict 8-Byte Memory Alignment**: Enables zero-copy memoryview casting for fast array reads.
- **Double-Checksum Validation**: Separate CRC32 integrity checks for the global header and individual chunk payloads.
- **Transactional Crash Safety**: Ignores incomplete/crashed chunks, automatically rolling back the archive to the last stable directory index.
- **Multi-Dataset Support**: Stores multiple datasets (Tabular, NArray, Stream logs) in a single file container.
- **Dynamic Chunk Lengths**: Support for chunks of variable sizes with zero padding or dead space.

---

## File Structure Layout

```text
+------------------------------------+  Offset 0
| Global Header (32 bytes)           |  -> [Magic][Version][Dir Offset][Dir Size][Header CRC]
+------------------------------------+  Offset 32
| Padding / Chunk 0 (Dataset A)      |  -> (8-byte aligned offset)
+------------------------------------+
| Padding / Chunk 1 (Dataset B)      |  -> (8-byte aligned offset)
+------------------------------------+
| ...                                |
+------------------------------------+  dir_offset
| Central Directory Index (JSON)     |  -> [Dataset Catalog & Chunk Map]
+------------------------------------+  EOF
```

---

## API Quick Start

The core library is defined in `xhd_format.py`. The recommended API uses
context managers, readable dataset helpers, and `write`/`read` aliases:

```python
from xhd_format import XhdArchive

stream_spec = {
    "schema": [
        {"name": "interaction_num", "type": "Q"},
        {"name": "particle_id", "type": "I"},
        {"name": "energy", "type": "f"}
    ]
}

with XhdArchive.create("simulation.xhd") as archive:
    archive.create_stream("interactions", stream_spec["schema"], compression="none")
    archive.write("interactions", chunk_id=0, data=[
        {"interaction_num": 1, "particle_id": 42, "energy": 99.8}
    ])

with XhdArchive.open("simulation.xhd") as archive:
    rows = archive.read("interactions", chunk_id=0)
```

The original low-level calls are still supported for compatibility:
`XhdArchive(path, mode="w")`, `create_dataset`, `write_chunk`,
`read_chunk`, `DATASET_STREAM`, and `COMPRESS_NONE`.

Useful helpers:

* `create_stream(name, schema, compression="none")`
* `create_table(name, schema, compression="zlib")`
* `create_array(name, shape=(rows, cols), dtype="d")`
* `list_datasets()`, `list_chunks(dataset)`, `dataset_info(dataset)`, `chunk_info(dataset, chunk)`

## Run the Benchmarks & Tests
Validation and benchmarking scripts are included:

* **Format Library**: `xhd_format.py` (Version 1)
* **API Tests**: `test_api.py` (Tests the simplified public API, validation, array layout, and overwrite checksums).
* **Integrity Stress Tests**: `test_integrity_stress.py` (Builds one mixed archive with generated dict rows, arrays, and streaming logs, then compares roundtrips and attacks cloned copies).
* **Unified Benchmark**: `benchmark_unified.py` (Tests file size, save, and load times against JSON, CSV, and Pickle for 1,000,000 records).
* **Robustness Suite**: `test_robustness.py` (Tests bit corruption, header validation, truncation, and crash recovery).
* **Dynamic Chunks Demo**: `test_dynamic_chunks.py` (Tests streaming 20 dynamic sets totaling 10.5 million records, and selectively loading Set 5).

To execute them:
```bash
python3 -m unittest test_api.py
python3 -m unittest test_integrity_stress.py
python3 benchmark_unified.py
python3 test_robustness.py
python3 test_dynamic_chunks.py
```

To print a dependency-free comparison report against JSON, JSON.gz, Pickle,
and a CSV directory:

```bash
python3 test_integrity_stress.py --compare-formats
```
