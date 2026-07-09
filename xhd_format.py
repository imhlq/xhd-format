import struct
import os
import json
import zlib
import time

# ==============================================================================
# The Unified & Robust xHou Data Format (.xhd) Specification - Version 7 (Safe)
# ==============================================================================
#
# Robustness Enhancements:
# 1. Header Integrity Checksum:
#    - The header contains a dedicated CRC32 checksum protecting the 
#      vital metadata pointers. Any corruption in the header will be 
#      detected immediately upon opening the archive.
# 2. Strict Boundary Checking:
#    - Validates file boundaries and sizes before reading chunk offsets or 
#      seeking. Prevents out-of-bounds pointer crashes.
# 3. Crash Recovery (Commit Pointer Pattern / Central Directory Append):
#    - During streaming writes, new chunks are appended *after* the previous 
#      directory catalog. The old catalog is never overwritten.
#    - If a crash occurs during a write, the file remains in its last stable 
#      state because the header pointer still points to the old, intact catalog.
# 4. Custom Error Reporting:
#    - All file failures raise a structured `CorruptedFileError` exception.
# ==============================================================================

MAGIC_BYTES = b'xHOU'
VERSION = 7

# Dataset Types
DATASET_TABULAR = 1
DATASET_NARRAY = 2
DATASET_STREAM = 3

# Compression Types
COMPRESS_NONE = 0
COMPRESS_ZLIB = 1


class CorruptedFileError(Exception):
    """Raised when file corruption or incomplete writes are detected."""
    pass


class XhdArchive:
    """Robust API for reading, writing, and streaming multiple datasets inside .xhd files."""

    def __init__(self, filepath, mode='r'):
        self.filepath = filepath
        self.mode = mode
        self.file = None
        self.directory = {}
        # Header Layout: 4s (magic), H (version), Q (dir_offset), I (dir_size), I (header_crc), 10x (padding)
        self.header_format = '<4sH QI I 10x'
        self.header_size = struct.calcsize(self.header_format)

        if mode == 'w':
            self.file = open(filepath, 'wb+')
            self._write_directory_at_end()
        elif mode in ('r', 'r+'):
            if not os.path.exists(filepath):
                raise FileNotFoundError(f"Archive '{filepath}' not found.")
            self.file = open(filepath, 'rb+' if mode == 'r+' else 'rb')
            self._read_directory()
        else:
            raise ValueError("Mode must be 'r', 'w', or 'r+'")

    def _read_directory(self):
        """Reads, checksums, and validates the central directory index catalog."""
        self.file.seek(0)
        header_bytes = self.file.read(self.header_size)
        if len(header_bytes) < self.header_size:
            raise CorruptedFileError("File header is cut short/incomplete.")

        magic, version, dir_offset, dir_size, header_checksum = struct.unpack(self.header_format, header_bytes)

        # 1. Basic Header Validation
        if magic != MAGIC_BYTES:
            raise CorruptedFileError("Invalid magic signature. Not a valid XHD file.")
        if version != VERSION:
            raise CorruptedFileError(f"Unsupported format version: {version}. Required: {VERSION}")

        # 2. Header Checksum Validation
        header_pre = struct.pack('<4sHQI', magic, version, dir_offset, dir_size)
        actual_header_checksum = zlib.crc32(header_pre) & 0xFFFFFFFF
        if actual_header_checksum != header_checksum:
            raise CorruptedFileError("Header corruption detected! (CRC32 Checksum Mismatch)")

        # 3. Directory Offset Bounds Validation
        self.file.seek(0, os.SEEK_END)
        file_size = self.file.tell()
        if dir_offset + dir_size > file_size:
            raise CorruptedFileError(f"Directory index pointer ({dir_offset} + {dir_size}) exceeds actual file size ({file_size}).")

        # 4. Read Index Catalog
        self.file.seek(dir_offset)
        dir_bytes = self.file.read(dir_size)
        try:
            self.directory = json.loads(dir_bytes.decode('utf-8'))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            raise CorruptedFileError(f"Directory index block is corrupted: {str(e)}")

    def _write_directory_at_end(self):
        """Writes directory index at the current file end and updates the header with checksums."""
        dir_bytes = json.dumps(self.directory).encode('utf-8')
        dir_size = len(dir_bytes)

        # Always append directory to the end of the file
        self.file.seek(0, os.SEEK_END)
        dir_offset = self.file.tell()
        
        if dir_offset < self.header_size:
            dir_offset = self.header_size

        # 1. Write Central Directory Index Catalog
        self.file.seek(dir_offset)
        self.file.write(dir_bytes)
        self.file.truncate()

        # 2. Compute Header Checksum
        header_pre = struct.pack('<4sHQI', MAGIC_BYTES, VERSION, dir_offset, dir_size)
        header_checksum = zlib.crc32(header_pre) & 0xFFFFFFFF

        # 3. Rewrite Header at start
        header_bytes = struct.pack(self.header_format, MAGIC_BYTES, VERSION, dir_offset, dir_size, header_checksum)
        self.file.seek(0)
        self.file.write(header_bytes)
        self.file.flush()

    def create_dataset(self, name, dataset_type, spec, compression=COMPRESS_NONE):
        """Defines a new dataset metadata configuration."""
        if self.mode not in ('w', 'r+'):
            raise IOError("Archive is read-only.")
        if name in self.directory:
            raise ValueError(f"Dataset '{name}' already exists.")

        self.directory[name] = {
            "type": dataset_type,
            "compression": compression,
            "spec": spec,
            "chunks": {}
        }
        self._write_directory_at_end()

    def write_chunk(self, dataset_name, chunk_id, data_payload):
        """Packs, compresses, validates, and writes a data chunk into the archive."""
        if self.mode not in ('w', 'r+'):
            raise IOError("Archive is read-only.")
        if dataset_name not in self.directory:
            raise ValueError(f"Dataset '{dataset_name}' not defined.")

        dataset = self.directory[dataset_name]
        dtype = dataset["type"]
        spec = dataset["spec"]
        compression = dataset["compression"]
        chunk_str_id = str(chunk_id)

        # 1. Pack the payload to raw bytes
        data_bytes = bytearray()

        if dtype == DATASET_TABULAR or dtype == DATASET_STREAM:
            format_str = '<' + ''.join(col['type'] for col in spec['schema'])
            row_struct = struct.Struct(format_str)

            for row in data_payload:
                row_values = []
                for col in spec['schema']:
                    if 'x' in col['type']:
                        continue
                    val = row[col['name']]
                    if 's' in col['type']:
                        str_len = int(col['type'].replace('s', ''))
                        val_bytes = val.encode('utf-8')[:str_len]
                        val = val_bytes.ljust(str_len, b'\x00')
                    row_values.append(val)
                data_bytes.extend(row_struct.pack(*row_values))

        elif dtype == DATASET_NARRAY:
            arr_dtype = spec['dtype']
            flat_data = []
            if isinstance(data_payload[0], list):
                for col in data_payload:
                    flat_data.extend(col)
            else:
                flat_data = data_payload

            format_str = f'<{len(flat_data)}{arr_dtype}'
            data_bytes = struct.pack(format_str, *flat_data)

        # 2. Compress payload if compression is enabled
        if compression == COMPRESS_ZLIB:
            payload_bytes = zlib.compress(data_bytes, level=6)
        else:
            payload_bytes = data_bytes

        packed_len = len(payload_bytes)
        checksum = zlib.crc32(payload_bytes) & 0xFFFFFFFF

        # 3. Determine write offset
        if chunk_str_id in dataset["chunks"]:
            # OVERWRITE IN-PLACE
            chunk_info = dataset["chunks"][chunk_str_id]
            if chunk_info["length"] != packed_len:
                raise ValueError("Updated chunk size does not match existing chunk size!")
            offset = chunk_info["offset"]
            
            self.file.seek(offset)
            self.file.write(payload_bytes)
        else:
            # APPEND NEW CHUNK (Append to the end of the file, after old directory index)
            self.file.seek(0, os.SEEK_END)
            offset = self.file.tell()
            
            align_padding = (8 - (offset % 8)) % 8
            if align_padding > 0:
                self.file.write(b'\x00' * align_padding)
                offset += align_padding

            dataset["chunks"][chunk_str_id] = {
                "offset": offset,
                "length": packed_len,
                "checksum": checksum,
                "record_count": len(data_payload)
            }
            
            self.file.seek(offset)
            self.file.write(payload_bytes)

        # Update Directory
        self._write_directory_at_end()

    def read_chunk(self, dataset_name, chunk_id):
        """Reads, validates, decompresses, and unpacks a chunk of data from disk."""
        if dataset_name not in self.directory:
            raise ValueError(f"Dataset '{dataset_name}' not found.")

        dataset = self.directory[dataset_name]
        dtype_code = dataset["type"]
        spec = dataset["spec"]
        compression = dataset["compression"]
        chunk_str_id = str(chunk_id)

        if chunk_str_id not in dataset["chunks"]:
            raise IndexError(f"Chunk {chunk_id} does not exist in dataset '{dataset_name}'.")

        chunk_info = dataset["chunks"][chunk_str_id]
        offset = chunk_info["offset"]
        length = chunk_info["length"]
        expected_checksum = chunk_info["checksum"]

        # Check bounds before seeking or reading data chunk
        self.file.seek(0, os.SEEK_END)
        file_size = self.file.tell()
        if offset + length > file_size:
            raise CorruptedFileError(f"Data chunk offset ({offset} + {length}) exceeds actual file size ({file_size}).")

        # Read the chunk payload from disk
        self.file.seek(offset)
        payload_bytes = self.file.read(length)

        # CRC32 Checksum Validation
        actual_checksum = zlib.crc32(payload_bytes) & 0xFFFFFFFF
        if actual_checksum != expected_checksum:
            raise CorruptedFileError(f"Data corruption detected in dataset '{dataset_name}', chunk {chunk_id}! (CRC32 Checksum Mismatch)")

        # Decompress if compressed
        if compression == COMPRESS_ZLIB:
            data_bytes = zlib.decompress(payload_bytes)
        else:
            data_bytes = payload_bytes

        # 4. Unpack binary bytes
        if dtype_code == DATASET_TABULAR or dtype_code == DATASET_STREAM:
            format_str = '<' + ''.join(col['type'] for col in spec['schema'])
            row_struct = struct.Struct(format_str)
            row_size = row_struct.size
            rows = []
            
            active_cols = [col for col in spec['schema'] if 'x' not in col['type']]
            
            mv = memoryview(data_bytes)
            for i in range(0, len(mv), row_size):
                row_chunk = mv[i:i+row_size]
                unpacked = row_struct.unpack(row_chunk)
                row_dict = {}
                for idx, col in enumerate(active_cols):
                    val = unpacked[idx]
                    if isinstance(val, bytes):
                        val = val.decode('utf-8').rstrip('\x00')
                    row_dict[col['name']] = val
                rows.append(row_dict)
            return rows

        elif dtype_code == DATASET_NARRAY:
            arr_dtype = spec['dtype']
            shape = spec['shape']
            num_rows, num_cols = shape
            
            flat_view = memoryview(data_bytes).cast(arr_dtype)
            cols = []
            for col_idx in range(num_cols):
                start = col_idx * num_rows
                end = start + num_rows
                cols.append(flat_view[start:end])
                
            return [list(row) for row in zip(*cols)]

    def close(self):
        if self.file:
            self.file.close()
