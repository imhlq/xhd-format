"""Read and write xHou Data Format (.xhd) archives.

The module keeps the original integer constants for compatibility, while the
new enum and helper methods make the public API easier to read:

    with XhdArchive.create("run.xhd") as archive:
        archive.create_stream("events", schema)
        archive.write("events", 0, rows)
"""

from __future__ import annotations

import json
import os
import struct
import zlib
from enum import IntEnum
from pathlib import Path


MAGIC_BYTES = b"xHOU"
VERSION = 1
__version__ = "1.0.0"


class DatasetType(IntEnum):
    """Dataset storage layouts supported by XHD."""

    TABULAR = 1
    NARRAY = 2
    STREAM = 3


class Compression(IntEnum):
    """Payload compression algorithms supported by XHD."""

    NONE = 0
    ZLIB = 1


# Backwards-compatible constants.
DATASET_TABULAR = DatasetType.TABULAR
DATASET_NARRAY = DatasetType.NARRAY
DATASET_STREAM = DatasetType.STREAM
COMPRESS_NONE = Compression.NONE
COMPRESS_ZLIB = Compression.ZLIB

__all__ = [
    "Compression",
    "COMPRESS_NONE",
    "COMPRESS_ZLIB",
    "CorruptedFileError",
    "DATASET_NARRAY",
    "DATASET_STREAM",
    "DATASET_TABULAR",
    "DatasetType",
    "MAGIC_BYTES",
    "VERSION",
    "XhdArchive",
    "__version__",
    "open_archive",
]


class CorruptedFileError(Exception):
    """Raised when file corruption or incomplete writes are detected."""


class XhdArchive:
    """Read, write, and stream multiple datasets inside one ``.xhd`` archive."""

    HEADER_FORMAT = "<4sHQII10x"
    HEADER_PREFIX_FORMAT = "<4sHQI"
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
    WRITABLE_MODES = {"w", "r+"}

    def __init__(self, filepath, mode="r"):
        self.filepath = str(filepath)
        self.mode = mode
        self.file = None
        self.directory = {}
        self.header_format = self.HEADER_FORMAT
        self.header_size = self.HEADER_SIZE

        if mode == "w":
            self.file = open(self.filepath, "wb+")
            try:
                self._write_directory_at_end()
            except Exception:
                self.close()
                raise
        elif mode in ("r", "r+"):
            if not os.path.exists(self.filepath):
                raise FileNotFoundError(f"Archive '{self.filepath}' not found.")
            self.file = open(self.filepath, "rb+" if mode == "r+" else "rb")
            try:
                self._read_directory()
            except Exception:
                self.close()
                raise
        else:
            raise ValueError("Mode must be 'r', 'w', or 'r+'.")

    @classmethod
    def create(cls, filepath):
        """Create or replace an archive and open it for writing."""

        return cls(filepath, mode="w")

    @classmethod
    def open(cls, filepath, writable=False):
        """Open an existing archive.

        Pass ``writable=True`` to add datasets or chunks to an existing archive.
        """

        return cls(filepath, mode="r+" if writable else "r")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False

    @property
    def closed(self):
        return self.file is None or self.file.closed

    def create_dataset(self, name, dataset_type, spec, compression=Compression.NONE):
        """Define a dataset.

        ``dataset_type`` and ``compression`` may be enum values, old integer
        constants, or readable strings such as ``"stream"`` and ``"zlib"``.
        """

        self._require_writable()
        self._validate_name(name)
        dataset_type = self._normalize_dataset_type(dataset_type)
        compression = self._normalize_compression(compression)
        self._validate_spec(dataset_type, spec)

        if name in self.directory:
            raise ValueError(f"Dataset '{name}' already exists.")

        self.directory[name] = {
            "type": int(dataset_type),
            "compression": int(compression),
            "spec": spec,
            "chunks": {},
        }
        self._write_directory_at_end()

    def create_stream(self, name, schema, compression=Compression.NONE):
        """Create an append-friendly row dataset."""

        self.create_dataset(name, DatasetType.STREAM, {"schema": schema}, compression)

    def create_table(self, name, schema, compression=Compression.NONE):
        """Create a tabular row dataset."""

        self.create_dataset(name, DatasetType.TABULAR, {"schema": schema}, compression)

    def create_array(self, name, shape, dtype, compression=Compression.NONE):
        """Create a two-dimensional numeric array dataset."""

        self.create_dataset(
            name,
            DatasetType.NARRAY,
            {"shape": list(shape), "dtype": dtype},
            compression,
        )

    def write(self, dataset_name, chunk_id, data):
        """Write a chunk. Alias for ``write_chunk``."""

        self.write_chunk(dataset_name, chunk_id, data)

    def write_chunk(self, dataset_name, chunk_id, data_payload):
        """Pack, compress, validate, and write a data chunk into the archive."""

        self._require_writable()
        dataset = self._get_dataset(dataset_name)
        chunk_str_id = str(chunk_id)

        data_bytes, record_count = self._pack_payload(dataset, data_payload)
        payload_bytes = self._compress(data_bytes, dataset["compression"])
        packed_len = len(payload_bytes)
        checksum = zlib.crc32(payload_bytes) & 0xFFFFFFFF

        if chunk_str_id in dataset["chunks"]:
            chunk_info = dataset["chunks"][chunk_str_id]
            if chunk_info["length"] != packed_len:
                raise ValueError(
                    "Updated chunk size does not match existing chunk size. "
                    "Use a new chunk id for variable-size updates."
                )
            offset = chunk_info["offset"]
            self.file.seek(offset)
            self.file.write(payload_bytes)
            chunk_info["checksum"] = checksum
            chunk_info["record_count"] = record_count
        else:
            self.file.seek(0, os.SEEK_END)
            offset = self._align_file_to_eight_bytes()
            self.file.seek(offset)
            self.file.write(payload_bytes)
            dataset["chunks"][chunk_str_id] = {
                "offset": offset,
                "length": packed_len,
                "checksum": checksum,
                "record_count": record_count,
            }

        self._write_directory_at_end()

    def read(self, dataset_name, chunk_id):
        """Read a chunk. Alias for ``read_chunk``."""

        return self.read_chunk(dataset_name, chunk_id)

    def read_chunk(self, dataset_name, chunk_id):
        """Read, validate, decompress, and unpack a chunk from disk."""

        self._require_open()
        dataset = self._get_dataset(dataset_name)
        chunk_str_id = str(chunk_id)

        if chunk_str_id not in dataset["chunks"]:
            raise IndexError(f"Chunk {chunk_id} does not exist in dataset '{dataset_name}'.")

        chunk_info = dataset["chunks"][chunk_str_id]
        offset = chunk_info["offset"]
        length = chunk_info["length"]
        expected_checksum = chunk_info["checksum"]
        self._validate_chunk_bounds(offset, length, dataset_name, chunk_id)

        self.file.seek(offset)
        payload_bytes = self.file.read(length)
        if len(payload_bytes) != length:
            raise CorruptedFileError(
                f"Chunk '{dataset_name}/{chunk_id}' is incomplete: "
                f"expected {length} bytes, got {len(payload_bytes)}."
            )

        actual_checksum = zlib.crc32(payload_bytes) & 0xFFFFFFFF
        if actual_checksum != expected_checksum:
            raise CorruptedFileError(
                f"Data corruption detected in dataset '{dataset_name}', chunk {chunk_id}. "
                "CRC32 checksum mismatch."
            )

        data_bytes = self._decompress(payload_bytes, dataset["compression"], dataset_name, chunk_id)
        return self._unpack_payload(dataset, data_bytes, dataset_name, chunk_id)

    def list_datasets(self):
        """Return dataset names in directory order."""

        self._require_open()
        return list(self.directory)

    def has_dataset(self, name):
        self._require_open()
        return name in self.directory

    def dataset_info(self, name):
        """Return a shallow copy of a dataset's metadata."""

        dataset = self._get_dataset(name)
        return {
            "type": DatasetType(dataset["type"]).name.lower(),
            "compression": Compression(dataset["compression"]).name.lower(),
            "spec": dict(dataset["spec"]),
            "chunks": dict(dataset["chunks"]),
        }

    def list_chunks(self, dataset_name):
        """Return chunk ids for a dataset as strings."""

        dataset = self._get_dataset(dataset_name)
        return list(dataset["chunks"])

    def has_chunk(self, dataset_name, chunk_id):
        dataset = self._get_dataset(dataset_name)
        return str(chunk_id) in dataset["chunks"]

    def chunk_info(self, dataset_name, chunk_id):
        dataset = self._get_dataset(dataset_name)
        chunk_str_id = str(chunk_id)
        if chunk_str_id not in dataset["chunks"]:
            raise IndexError(f"Chunk {chunk_id} does not exist in dataset '{dataset_name}'.")
        return dict(dataset["chunks"][chunk_str_id])

    def close(self):
        if self.file and not self.file.closed:
            self.file.flush()
            self.file.close()

    def _read_directory(self):
        self._require_open()
        self.file.seek(0)
        header_bytes = self.file.read(self.HEADER_SIZE)
        if len(header_bytes) < self.HEADER_SIZE:
            raise CorruptedFileError("File header is cut short or incomplete.")

        magic, version, dir_offset, dir_size, header_checksum = struct.unpack(
            self.HEADER_FORMAT, header_bytes
        )

        if magic != MAGIC_BYTES:
            raise CorruptedFileError("Invalid magic signature. Not a valid XHD file.")
        if version != VERSION:
            raise CorruptedFileError(f"Unsupported format version: {version}. Required: {VERSION}.")

        header_pre = struct.pack(self.HEADER_PREFIX_FORMAT, magic, version, dir_offset, dir_size)
        actual_header_checksum = zlib.crc32(header_pre) & 0xFFFFFFFF
        if actual_header_checksum != header_checksum:
            raise CorruptedFileError("Header corruption detected. CRC32 checksum mismatch.")

        self.file.seek(0, os.SEEK_END)
        file_size = self.file.tell()
        if dir_offset < self.HEADER_SIZE:
            raise CorruptedFileError(f"Directory index starts inside the header at offset {dir_offset}.")
        if dir_offset + dir_size > file_size:
            raise CorruptedFileError(
                f"Directory index pointer ({dir_offset} + {dir_size}) exceeds "
                f"actual file size ({file_size})."
            )

        self.file.seek(dir_offset)
        dir_bytes = self.file.read(dir_size)
        if len(dir_bytes) != dir_size:
            raise CorruptedFileError(
                f"Directory index is incomplete: expected {dir_size} bytes, got {len(dir_bytes)}."
            )

        try:
            directory = json.loads(dir_bytes.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise CorruptedFileError(f"Directory index block is corrupted: {exc}") from exc

        self._validate_directory(directory)
        self.directory = directory

    def _write_directory_at_end(self):
        self._require_open()
        dir_bytes = json.dumps(self.directory, separators=(",", ":")).encode("utf-8")
        dir_size = len(dir_bytes)

        self.file.seek(0, os.SEEK_END)
        dir_offset = max(self.file.tell(), self.HEADER_SIZE)

        self.file.seek(dir_offset)
        self.file.write(dir_bytes)
        self.file.truncate()

        header_pre = struct.pack(self.HEADER_PREFIX_FORMAT, MAGIC_BYTES, VERSION, dir_offset, dir_size)
        header_checksum = zlib.crc32(header_pre) & 0xFFFFFFFF
        header_bytes = struct.pack(
            self.HEADER_FORMAT,
            MAGIC_BYTES,
            VERSION,
            dir_offset,
            dir_size,
            header_checksum,
        )
        self.file.seek(0)
        self.file.write(header_bytes)
        self.file.flush()

    def _pack_payload(self, dataset, data_payload):
        dtype = DatasetType(dataset["type"])
        if dtype in (DatasetType.TABULAR, DatasetType.STREAM):
            return self._pack_rows(dataset["spec"], data_payload)
        if dtype == DatasetType.NARRAY:
            return self._pack_array(dataset["spec"], data_payload)
        raise CorruptedFileError(f"Unknown dataset type in directory: {dataset['type']}.")

    def _unpack_payload(self, dataset, data_bytes, dataset_name, chunk_id):
        dtype = DatasetType(dataset["type"])
        if dtype in (DatasetType.TABULAR, DatasetType.STREAM):
            return self._unpack_rows(dataset["spec"], data_bytes, dataset_name, chunk_id)
        if dtype == DatasetType.NARRAY:
            return self._unpack_array(dataset["spec"], data_bytes, dataset_name, chunk_id)
        raise CorruptedFileError(f"Unknown dataset type in directory: {dataset['type']}.")

    def _pack_rows(self, spec, rows):
        row_struct = self._row_struct(spec)
        active_cols = self._active_columns(spec)
        data_bytes = bytearray()
        record_count = 0

        for row in rows:
            row_values = []
            for col in active_cols:
                try:
                    value = row[col["name"]]
                except KeyError as exc:
                    raise ValueError(f"Missing field '{col['name']}' in row {record_count}.") from exc
                row_values.append(self._encode_value(col["type"], value))
            try:
                data_bytes.extend(row_struct.pack(*row_values))
            except struct.error as exc:
                raise ValueError(f"Row {record_count} does not match schema: {exc}") from exc
            record_count += 1

        return bytes(data_bytes), record_count

    def _unpack_rows(self, spec, data_bytes, dataset_name, chunk_id):
        row_struct = self._row_struct(spec)
        row_size = row_struct.size
        if row_size == 0 or len(data_bytes) % row_size != 0:
            raise CorruptedFileError(
                f"Chunk '{dataset_name}/{chunk_id}' length is not a multiple of row size {row_size}."
            )

        active_cols = self._active_columns(spec)
        rows = []
        mv = memoryview(data_bytes)
        for start in range(0, len(mv), row_size):
            unpacked = row_struct.unpack(mv[start : start + row_size])
            row = {}
            for idx, col in enumerate(active_cols):
                value = unpacked[idx]
                if isinstance(value, bytes):
                    value = value.decode("utf-8").rstrip("\x00")
                row[col["name"]] = value
            rows.append(row)
        return rows

    def _pack_array(self, spec, data_payload):
        dtype = spec["dtype"]
        rows, cols = spec["shape"]
        flat_data = self._flatten_array_payload(data_payload)
        expected = rows * cols
        if len(flat_data) != expected:
            raise ValueError(
                f"Array payload has {len(flat_data)} values, but shape {spec['shape']} "
                f"requires {expected}."
            )

        try:
            return struct.pack(f"<{len(flat_data)}{dtype}", *flat_data), rows
        except struct.error as exc:
            raise ValueError(f"Array payload does not match dtype '{dtype}': {exc}") from exc

    def _unpack_array(self, spec, data_bytes, dataset_name, chunk_id):
        dtype = spec["dtype"]
        rows, cols = spec["shape"]
        expected_values = rows * cols
        value_size = struct.calcsize(f"<{dtype}")
        expected_bytes = expected_values * value_size
        if len(data_bytes) != expected_bytes:
            raise CorruptedFileError(
                f"Chunk '{dataset_name}/{chunk_id}' has {len(data_bytes)} bytes, "
                f"but array shape {spec['shape']} with dtype '{dtype}' requires {expected_bytes}."
            )

        try:
            flat_values = struct.unpack(f"<{expected_values}{dtype}", data_bytes)
        except struct.error as exc:
            raise CorruptedFileError(f"Array chunk '{dataset_name}/{chunk_id}' is invalid: {exc}") from exc

        return [
            list(flat_values[row_start : row_start + cols])
            for row_start in range(0, expected_values, cols)
        ]

    def _compress(self, data_bytes, compression):
        compression = Compression(compression)
        if compression == Compression.NONE:
            return data_bytes
        if compression == Compression.ZLIB:
            return zlib.compress(data_bytes, level=6)
        raise ValueError(f"Unsupported compression: {compression}.")

    def _decompress(self, payload_bytes, compression, dataset_name, chunk_id):
        compression = Compression(compression)
        if compression == Compression.NONE:
            return payload_bytes
        if compression == Compression.ZLIB:
            try:
                return zlib.decompress(payload_bytes)
            except zlib.error as exc:
                raise CorruptedFileError(
                    f"Compressed chunk '{dataset_name}/{chunk_id}' could not be decompressed: {exc}"
                ) from exc
        raise CorruptedFileError(f"Unknown compression in directory: {compression}.")

    def _align_file_to_eight_bytes(self):
        offset = self.file.tell()
        padding = (8 - (offset % 8)) % 8
        if padding:
            self.file.write(b"\x00" * padding)
            offset += padding
        return offset

    def _get_dataset(self, name):
        self._require_open()
        if name not in self.directory:
            raise ValueError(f"Dataset '{name}' not found.")
        return self.directory[name]

    def _validate_chunk_bounds(self, offset, length, dataset_name, chunk_id):
        if offset < self.HEADER_SIZE or length < 0:
            raise CorruptedFileError(
                f"Chunk '{dataset_name}/{chunk_id}' has invalid offset/length: {offset}/{length}."
            )

        self.file.seek(0, os.SEEK_END)
        file_size = self.file.tell()
        if offset + length > file_size:
            raise CorruptedFileError(
                f"Data chunk offset ({offset} + {length}) exceeds actual file size ({file_size})."
            )

    def _validate_directory(self, directory):
        if not isinstance(directory, dict):
            raise CorruptedFileError("Directory index must be a JSON object.")

        for name, dataset in directory.items():
            try:
                self._validate_name(name)
                dataset_type = self._normalize_dataset_type(dataset["type"])
                compression = self._normalize_compression(dataset["compression"])
                self._validate_spec(dataset_type, dataset["spec"])
                chunks = dataset["chunks"]
            except (KeyError, TypeError, ValueError) as exc:
                raise CorruptedFileError(f"Dataset metadata for '{name}' is invalid: {exc}") from exc

            if not isinstance(chunks, dict):
                raise CorruptedFileError(f"Chunk map for dataset '{name}' must be an object.")

            for chunk_id, chunk_info in chunks.items():
                if not isinstance(chunk_info, dict):
                    raise CorruptedFileError(f"Chunk '{name}/{chunk_id}' metadata must be an object.")
                for field in ("offset", "length", "checksum", "record_count"):
                    if not isinstance(chunk_info.get(field), int) or chunk_info[field] < 0:
                        raise CorruptedFileError(
                            f"Chunk '{name}/{chunk_id}' has invalid '{field}' metadata."
                        )
            dataset["type"] = int(dataset_type)
            dataset["compression"] = int(compression)

    def _validate_spec(self, dataset_type, spec):
        if not isinstance(spec, dict):
            raise ValueError("Dataset spec must be a dictionary.")

        dataset_type = DatasetType(dataset_type)
        if dataset_type in (DatasetType.TABULAR, DatasetType.STREAM):
            schema = spec.get("schema")
            if not isinstance(schema, list) or not schema:
                raise ValueError("Row dataset spec requires a non-empty 'schema' list.")
            seen_names = set()
            for idx, col in enumerate(schema):
                if not isinstance(col, dict):
                    raise ValueError(f"Schema column {idx} must be a dictionary.")
                col_type = col.get("type")
                if not isinstance(col_type, str) or not col_type:
                    raise ValueError(f"Schema column {idx} requires a struct 'type'.")
                if not self._is_padding_column(col_type):
                    name = col.get("name")
                    self._validate_name(name)
                    if name in seen_names:
                        raise ValueError(f"Duplicate schema field '{name}'.")
                    seen_names.add(name)
            self._row_struct(spec)
        elif dataset_type == DatasetType.NARRAY:
            shape = spec.get("shape")
            dtype = spec.get("dtype")
            if (
                not isinstance(shape, (list, tuple))
                or len(shape) != 2
                or not all(isinstance(value, int) and value > 0 for value in shape)
            ):
                raise ValueError("Array dataset spec requires a two-item positive integer 'shape'.")
            if not isinstance(dtype, str) or not dtype:
                raise ValueError("Array dataset spec requires a struct 'dtype'.")
            struct.calcsize(f"<{dtype}")

    def _row_struct(self, spec):
        try:
            return struct.Struct("<" + "".join(col["type"] for col in spec["schema"]))
        except (KeyError, TypeError, struct.error) as exc:
            raise ValueError(f"Invalid row schema: {exc}") from exc

    def _active_columns(self, spec):
        return [col for col in spec["schema"] if not self._is_padding_column(col["type"])]

    @staticmethod
    def _is_padding_column(type_code):
        return type_code.endswith("x")

    @staticmethod
    def _encode_value(type_code, value):
        if type_code.endswith("s"):
            str_len = int(type_code[:-1] or "1")
            if isinstance(value, bytes):
                value_bytes = value[:str_len]
            else:
                value_bytes = str(value).encode("utf-8")[:str_len]
            return value_bytes.ljust(str_len, b"\x00")
        return value

    @staticmethod
    def _flatten_array_payload(data_payload):
        if isinstance(data_payload, (str, bytes)):
            raise ValueError("Array payload must be numeric data, not text or bytes.")

        values = list(data_payload)
        if values and isinstance(values[0], (list, tuple)):
            flat = []
            for row in values:
                flat.extend(row)
            return flat
        return values

    @staticmethod
    def _normalize_dataset_type(dataset_type):
        if isinstance(dataset_type, str):
            value = dataset_type.strip().lower()
            aliases = {
                "tabular": DatasetType.TABULAR,
                "table": DatasetType.TABULAR,
                "narray": DatasetType.NARRAY,
                "array": DatasetType.NARRAY,
                "stream": DatasetType.STREAM,
            }
            if value in aliases:
                return aliases[value]
            raise ValueError(f"Unknown dataset type '{dataset_type}'.")
        return DatasetType(dataset_type)

    @staticmethod
    def _normalize_compression(compression):
        if isinstance(compression, str):
            value = compression.strip().lower()
            aliases = {
                "none": Compression.NONE,
                "raw": Compression.NONE,
                "zlib": Compression.ZLIB,
            }
            if value in aliases:
                return aliases[value]
            raise ValueError(f"Unknown compression '{compression}'.")
        return Compression(compression)

    @staticmethod
    def _validate_name(name):
        if not isinstance(name, str) or not name:
            raise ValueError("Names must be non-empty strings.")

    def _require_writable(self):
        self._require_open()
        if self.mode not in self.WRITABLE_MODES:
            raise OSError("Archive is read-only.")

    def _require_open(self):
        if self.closed:
            raise ValueError("Archive is closed.")


def open_archive(filepath, mode="r"):
    """Open an XHD archive.

    This small function mirrors Python's built-in ``open`` style and is handy
    for callers that prefer functions over constructors.
    """

    return XhdArchive(Path(filepath), mode=mode)
