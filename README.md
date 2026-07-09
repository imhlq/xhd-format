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

## Getting Started

### 1. Unified Format API
The core library is defined in `xhd_format.py`. You can open an archive in write (`w`), read (`r`), or update (`r+`) modes:

```python
from xhd_format import XhdArchive, DATASET_STREAM, COMPRESS_NONE

archive = XhdArchive("simulation.xhd", mode='w')

# Create a dataset
stream_spec = {
    "schema": [
        {"name": "interaction_num", "type": "Q"},
        {"name": "particle_id", "type": "I"},
        {"name": "energy", "type": "f"}
    ]
}
archive.create_dataset("interactions", DATASET_STREAM, stream_spec, compression=COMPRESS_NONE)

# Write chunk 0
archive.write_chunk("interactions", chunk_id=0, data_payload=[
    {"interaction_num": 1, "particle_id": 42, "energy": 99.8}
])
archive.close()
```

### 2. Run the Benchmarks & Tests
Four validation and benchmarking scripts are included:

* **Format Library**: `xhd_format.py` (Version 7)
* **Unified Benchmark**: `benchmark_unified.py` (Tests file size, save, and load times against JSON, CSV, and Pickle for 1,000,000 records).
* **Robustness Suite**: `test_robustness.py` (Tests bit corruption, header validation, truncation, and crash recovery).
* **Dynamic Chunks Demo**: `test_dynamic_chunks.py` (Tests streaming 20 dynamic sets totaling 10.5 million records, and selectively loading Set 5).

To execute them:
```bash
python3 benchmark_unified.py
python3 test_robustness.py
python3 test_dynamic_chunks.py
```
