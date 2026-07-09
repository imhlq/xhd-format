import argparse
import csv
import gzip
import json
import os
import pickle
import shutil
import struct
import sys
import tempfile
import time
import unittest
import zlib

from xhd_format import MAGIC_BYTES, VERSION, CorruptedFileError, XhdArchive


EVENT_SCHEMA = [
    {"name": "event_id", "type": "Q"},
    {"name": "iteration", "type": "I"},
    {"name": "particle_id", "type": "I"},
    {"name": "energy", "type": "f"},
    {"name": "state", "type": "B"},
    {"name": "tag", "type": "12s"},
    {"name": "padding", "type": "3x"},
]

METRIC_SCHEMA = [
    {"name": "step", "type": "I"},
    {"name": "temperature", "type": "d"},
    {"name": "pressure", "type": "f"},
    {"name": "status", "type": "8s"},
]

EVENT_CHUNK_COUNTS = [800, 950, 1100, 1250, 1400]
METRIC_COUNT = 2400
POSITION_SHAPE = (4096, 3)
ID_SHAPE = (2048, 4)


class MixedArchiveIntegrityStressTests(unittest.TestCase):
    """End-to-end archive comparison plus attacks against the same mixed file."""

    maxDiff = None

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.archive_path = os.path.join(self.tmpdir.name, "mixed_integrity_stress.xhd")
        self.expected = self.build_mixed_archive(self.archive_path)

    def test_mixed_archive_roundtrip_and_catalog(self):
        with XhdArchive.open(self.archive_path) as archive:
            self.assertEqual(
                set(archive.list_datasets()),
                {"events", "metrics", "positions", "ids"},
            )

            self.assertEqual(archive.list_chunks("events"), ["0", "1", "2", "3", "4"])
            self.assertEqual(archive.chunk_info("events", 4)["record_count"], 1400)
            self.assertRowsAlmostEqual(archive.read("metrics", 0), self.expected["metrics"])
            self.assertEqual(archive.read("ids", 0), self.expected["ids"])
            self.assertMatrixAlmostEqual(archive.read("positions", 0), self.expected["positions"])

            for chunk_id, expected_rows in self.expected["events"].items():
                self.assertRowsAlmostEqual(archive.read("events", chunk_id), expected_rows)

    def test_attack_header_corruption_is_rejected_on_open(self):
        attacked = self.copy_archive("header_flip.xhd")

        with open(attacked, "r+b") as handle:
            handle.seek(10)
            handle.write(b"\xFF")

        with self.assertRaisesRegex(CorruptedFileError, "Header corruption"):
            XhdArchive.open(attacked)

    def test_attack_directory_truncation_is_rejected_on_open(self):
        attacked = self.copy_archive("directory_truncated.xhd")

        with open(attacked, "r+b") as handle:
            handle.truncate(os.path.getsize(attacked) - 17)

        with self.assertRaisesRegex(CorruptedFileError, "Directory index pointer"):
            XhdArchive.open(attacked)

    def test_attack_payload_bit_flip_is_rejected_on_read(self):
        attacked = self.copy_archive("payload_bit_flip.xhd")

        with XhdArchive.open(attacked) as archive:
            offset = archive.chunk_info("events", 3)["offset"]

        self.flip_byte(attacked, offset + 23)

        with XhdArchive.open(attacked) as archive:
            with self.assertRaisesRegex(CorruptedFileError, "CRC32"):
                archive.read("events", 3)

            self.assertRowsAlmostEqual(archive.read("metrics", 0), self.expected["metrics"])

    def test_attack_compressed_payload_bit_flip_is_rejected_on_read(self):
        attacked = self.copy_archive("compressed_payload_bit_flip.xhd")

        with XhdArchive.open(attacked) as archive:
            offset = archive.chunk_info("ids", 0)["offset"]

        self.flip_byte(attacked, offset + 5)

        with XhdArchive.open(attacked) as archive:
            with self.assertRaisesRegex(CorruptedFileError, "CRC32"):
                archive.read("ids", 0)

    def test_attack_directory_metadata_bounds_are_rejected_on_read(self):
        attacked = self.copy_archive("metadata_bounds.xhd")
        directory = self.read_directory(attacked)
        directory["positions"]["chunks"]["0"]["offset"] = os.path.getsize(attacked) + 4096
        self.rewrite_directory(attacked, directory)

        with XhdArchive.open(attacked) as archive:
            with self.assertRaisesRegex(CorruptedFileError, "exceeds actual file size"):
                archive.read("positions", 0)

    def test_attack_directory_metadata_type_is_rejected_on_open(self):
        attacked = self.copy_archive("metadata_type.xhd")
        directory = self.read_directory(attacked)
        directory["events"]["chunks"]["2"]["length"] = "not-an-int"
        self.rewrite_directory(attacked, directory)

        with self.assertRaisesRegex(CorruptedFileError, "invalid 'length' metadata"):
            XhdArchive.open(attacked)

    def test_attack_appended_crash_junk_is_ignored(self):
        attacked = self.copy_archive("append_junk.xhd")

        with open(attacked, "ab") as handle:
            handle.write(b"unfinished-new-chunk" * 32)

        with XhdArchive.open(attacked) as archive:
            self.assertRowsAlmostEqual(archive.read("events", 4), self.expected["events"][4])
            self.assertRowsAlmostEqual(archive.read("metrics", 0), self.expected["metrics"])
            with self.assertRaises(IndexError):
                archive.read("events", 99)

    @staticmethod
    def build_mixed_archive(path):
        expected = {
            "events": {},
            "metrics": generate_metric_rows(METRIC_COUNT),
            "positions": generate_position_matrix(*POSITION_SHAPE),
            "ids": generate_id_matrix(*ID_SHAPE),
        }

        with XhdArchive.create(path) as archive:
            archive.create_stream("events", EVENT_SCHEMA, compression="none")
            archive.create_table("metrics", METRIC_SCHEMA, compression="none")
            archive.create_array("positions", shape=POSITION_SHAPE, dtype="d", compression="zlib")
            archive.create_array("ids", shape=ID_SHAPE, dtype="I", compression="zlib")

            for chunk_id, count in enumerate(EVENT_CHUNK_COUNTS):
                rows = generate_event_rows(chunk_id, count)
                expected["events"][chunk_id] = rows
                archive.write("events", chunk_id, rows)

            archive.write("metrics", 0, expected["metrics"])
            archive.write("positions", 0, expected["positions"])
            archive.write("ids", 0, expected["ids"])

        return expected

    def copy_archive(self, filename):
        target = os.path.join(self.tmpdir.name, filename)
        shutil.copyfile(self.archive_path, target)
        return target

    @staticmethod
    def flip_byte(path, offset):
        with open(path, "r+b") as handle:
            handle.seek(offset)
            original = handle.read(1)
            if not original:
                raise AssertionError(f"No byte available to flip at offset {offset}.")
            handle.seek(offset)
            handle.write(bytes([original[0] ^ 0xFF]))

    @staticmethod
    def read_directory(path):
        with open(path, "rb") as handle:
            header = handle.read(XhdArchive.HEADER_SIZE)
            magic, version, directory_offset, directory_size, checksum = struct.unpack(
                XhdArchive.HEADER_FORMAT,
                header,
            )
            if magic != MAGIC_BYTES or version != VERSION:
                raise AssertionError("Test helper opened an invalid archive header.")
            expected_checksum = zlib.crc32(
                struct.pack(
                    XhdArchive.HEADER_PREFIX_FORMAT,
                    magic,
                    version,
                    directory_offset,
                    directory_size,
                )
            ) & 0xFFFFFFFF
            if checksum != expected_checksum:
                raise AssertionError("Test helper opened an archive with a bad header checksum.")

            handle.seek(directory_offset)
            return json.loads(handle.read(directory_size).decode("utf-8"))

    @staticmethod
    def rewrite_directory(path, directory):
        directory_bytes = json.dumps(directory, separators=(",", ":")).encode("utf-8")

        with open(path, "r+b") as handle:
            handle.seek(0, os.SEEK_END)
            directory_offset = handle.tell()
            handle.write(directory_bytes)
            handle.truncate()

            header_pre = struct.pack(
                XhdArchive.HEADER_PREFIX_FORMAT,
                MAGIC_BYTES,
                VERSION,
                directory_offset,
                len(directory_bytes),
            )
            checksum = zlib.crc32(header_pre) & 0xFFFFFFFF
            header = struct.pack(
                XhdArchive.HEADER_FORMAT,
                MAGIC_BYTES,
                VERSION,
                directory_offset,
                len(directory_bytes),
                checksum,
            )
            handle.seek(0)
            handle.write(header)

    def assertRowsAlmostEqual(self, actual, expected):
        self.assertEqual(len(actual), len(expected))
        for row_index, (actual_row, expected_row) in enumerate(zip(actual, expected)):
            self.assertEqual(actual_row.keys(), expected_row.keys())
            for key, expected_value in expected_row.items():
                actual_value = actual_row[key]
                if isinstance(expected_value, float):
                    self.assertAlmostEqual(
                        actual_value,
                        expected_value,
                        places=5,
                        msg=f"row {row_index}, key {key}",
                    )
                else:
                    self.assertEqual(actual_value, expected_value, f"row {row_index}, key {key}")

    def assertMatrixAlmostEqual(self, actual, expected):
        self.assertEqual(len(actual), len(expected))
        for row_index, (actual_row, expected_row) in enumerate(zip(actual, expected)):
            self.assertEqual(len(actual_row), len(expected_row))
            for col_index, (actual_value, expected_value) in enumerate(zip(actual_row, expected_row)):
                if isinstance(expected_value, float):
                    self.assertAlmostEqual(
                        actual_value,
                        expected_value,
                        places=9,
                        msg=f"row {row_index}, col {col_index}",
                    )
                else:
                    self.assertEqual(actual_value, expected_value)


def generate_event_rows(chunk_id, count):
    rows = []
    base = chunk_id * 1_000_000
    for index in range(count):
        rows.append(
            {
                "event_id": base + index,
                "iteration": chunk_id * 100 + index // 7,
                "particle_id": (index * 37 + chunk_id * 11) % 500_000,
                "energy": round(((index * 0.03125) + chunk_id * 1.75) % 900.0, 5),
                "state": (index + chunk_id) % 8,
                "tag": f"ev{chunk_id}_{index % 97}",
            }
        )
    return rows


def generate_metric_rows(count):
    rows = []
    for step in range(count):
        rows.append(
            {
                "step": step,
                "temperature": 273.15 + (step * 0.125) + ((step % 9) * 0.01),
                "pressure": round(1.0 + (step % 31) * 0.0075, 6),
                "status": "ok" if step % 17 else "warn",
            }
        )
    return rows


def generate_position_matrix(rows, cols):
    return [
        [
            (row * 0.5) + (col * 0.125) + ((row % 13) * 0.001)
            for col in range(cols)
        ]
        for row in range(rows)
    ]


def generate_id_matrix(rows, cols):
    return [
        [
            (row * 97 + col * 13) % 2_000_000
            for col in range(cols)
        ]
        for row in range(rows)
    ]


def generate_mixed_dataset():
    return {
        "events": {
            chunk_id: generate_event_rows(chunk_id, count)
            for chunk_id, count in enumerate(EVENT_CHUNK_COUNTS)
        },
        "metrics": generate_metric_rows(METRIC_COUNT),
        "positions": generate_position_matrix(*POSITION_SHAPE),
        "ids": generate_id_matrix(*ID_SHAPE),
    }


def write_mixed_xhd(path, data):
    with XhdArchive.create(path) as archive:
        archive.create_stream("events", EVENT_SCHEMA, compression="none")
        archive.create_table("metrics", METRIC_SCHEMA, compression="none")
        archive.create_array("positions", shape=POSITION_SHAPE, dtype="d", compression="zlib")
        archive.create_array("ids", shape=ID_SHAPE, dtype="I", compression="zlib")

        for chunk_id, rows in data["events"].items():
            archive.write("events", chunk_id, rows)
        archive.write("metrics", 0, data["metrics"])
        archive.write("positions", 0, data["positions"])
        archive.write("ids", 0, data["ids"])


def read_xhd_all(path):
    with XhdArchive.open(path) as archive:
        return {
            "events": {
                int(chunk_id): archive.read("events", chunk_id)
                for chunk_id in archive.list_chunks("events")
            },
            "metrics": archive.read("metrics", 0),
            "positions": archive.read("positions", 0),
            "ids": archive.read("ids", 0),
        }


def read_xhd_event_chunk(path, chunk_id):
    with XhdArchive.open(path) as archive:
        return archive.read("events", chunk_id)


def write_json(path, data):
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(data, handle, separators=(",", ":"))


def read_json_all(path):
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def read_json_event_chunk(path, chunk_id):
    with open(path, "r", encoding="utf-8") as handle:
        data = json.load(handle)
    return data["events"][str(chunk_id)]


def write_json_gz(path, data):
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        json.dump(data, handle, separators=(",", ":"))


def read_json_gz_all(path):
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        return json.load(handle)


def read_json_gz_event_chunk(path, chunk_id):
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        data = json.load(handle)
    return data["events"][str(chunk_id)]


def write_pickle(path, data):
    with open(path, "wb") as handle:
        pickle.dump(data, handle, protocol=pickle.HIGHEST_PROTOCOL)


def read_pickle_all(path):
    with open(path, "rb") as handle:
        return pickle.load(handle)


def read_pickle_event_chunk(path, chunk_id):
    with open(path, "rb") as handle:
        data = pickle.load(handle)
    return data["events"][chunk_id]


def write_csv_directory(path, data):
    os.makedirs(path, exist_ok=True)

    for chunk_id, rows in data["events"].items():
        write_dict_rows_csv(os.path.join(path, f"events_{chunk_id}.csv"), rows)
    write_dict_rows_csv(os.path.join(path, "metrics.csv"), data["metrics"])
    write_matrix_csv(os.path.join(path, "positions.csv"), data["positions"])
    write_matrix_csv(os.path.join(path, "ids.csv"), data["ids"])


def read_csv_directory_all(path):
    return {
        "events": {
            chunk_id: read_event_rows_csv(os.path.join(path, f"events_{chunk_id}.csv"))
            for chunk_id in range(len(EVENT_CHUNK_COUNTS))
        },
        "metrics": read_metric_rows_csv(os.path.join(path, "metrics.csv")),
        "positions": read_float_matrix_csv(os.path.join(path, "positions.csv")),
        "ids": read_int_matrix_csv(os.path.join(path, "ids.csv")),
    }


def read_csv_event_chunk(path, chunk_id):
    return read_event_rows_csv(os.path.join(path, f"events_{chunk_id}.csv"))


def write_dict_rows_csv(path, rows):
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def write_matrix_csv(path, matrix):
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerows(matrix)


def read_event_rows_csv(path):
    rows = []
    with open(path, "r", newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            rows.append(
                {
                    "event_id": int(row["event_id"]),
                    "iteration": int(row["iteration"]),
                    "particle_id": int(row["particle_id"]),
                    "energy": float(row["energy"]),
                    "state": int(row["state"]),
                    "tag": row["tag"],
                }
            )
    return rows


def read_metric_rows_csv(path):
    rows = []
    with open(path, "r", newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            rows.append(
                {
                    "step": int(row["step"]),
                    "temperature": float(row["temperature"]),
                    "pressure": float(row["pressure"]),
                    "status": row["status"],
                }
            )
    return rows


def read_float_matrix_csv(path):
    with open(path, "r", newline="", encoding="utf-8") as handle:
        return [[float(value) for value in row] for row in csv.reader(handle)]


def read_int_matrix_csv(path):
    with open(path, "r", newline="", encoding="utf-8") as handle:
        return [[int(value) for value in row] for row in csv.reader(handle)]


def path_size(path):
    if os.path.isfile(path):
        return os.path.getsize(path)

    total = 0
    for root, _dirs, files in os.walk(path):
        for filename in files:
            total += os.path.getsize(os.path.join(root, filename))
    return total


def timed(callable_obj, *args):
    start = time.perf_counter()
    result = callable_obj(*args)
    return result, time.perf_counter() - start


def count_records(data):
    return {
        "event_rows": sum(len(rows) for rows in data["events"].values()),
        "metric_rows": len(data["metrics"]),
        "position_values": len(data["positions"]) * len(data["positions"][0]),
        "id_values": len(data["ids"]) * len(data["ids"][0]),
    }


def run_format_comparison_report():
    data = generate_mixed_dataset()
    selected_chunk = 3
    selected_count = len(data["events"][selected_chunk])

    formats = [
        {
            "name": "XHD",
            "target": "mixed.xhd",
            "write": write_mixed_xhd,
            "read_all": read_xhd_all,
            "read_subset": read_xhd_event_chunk,
            "notes": "Binary, central directory, per-chunk CRC32, direct chunk reads.",
        },
        {
            "name": "JSON",
            "target": "mixed.json",
            "write": write_json,
            "read_all": read_json_all,
            "read_subset": read_json_event_chunk,
            "notes": "Portable text, large numeric overhead, whole-file parse for subset.",
        },
        {
            "name": "JSON.gz",
            "target": "mixed.json.gz",
            "write": write_json_gz,
            "read_all": read_json_gz_all,
            "read_subset": read_json_gz_event_chunk,
            "notes": "Small portable text, decompresses and parses whole file for subset.",
        },
        {
            "name": "Pickle",
            "target": "mixed.pkl",
            "write": write_pickle,
            "read_all": read_pickle_all,
            "read_subset": read_pickle_event_chunk,
            "notes": "Compact Python object graph, Python-only, unsafe for untrusted input.",
        },
        {
            "name": "CSV dir",
            "target": "csv",
            "write": write_csv_directory,
            "read_all": read_csv_directory_all,
            "read_subset": read_csv_event_chunk,
            "notes": "Human-readable tables split by dataset/chunk, no embedded schema/checksums.",
        },
    ]

    with tempfile.TemporaryDirectory() as tmpdir:
        rows = []
        for fmt in formats:
            target = os.path.join(tmpdir, fmt["target"])
            _unused, write_seconds = timed(fmt["write"], target, data)
            size_bytes = path_size(target)
            all_data, read_all_seconds = timed(fmt["read_all"], target)
            subset_data, subset_seconds = timed(fmt["read_subset"], target, selected_chunk)
            rows.append(
                {
                    "format": fmt["name"],
                    "size_bytes": size_bytes,
                    "write_seconds": write_seconds,
                    "read_all_seconds": read_all_seconds,
                    "subset_seconds": subset_seconds,
                    "all_rows": count_records(normalize_loaded_data(all_data))["event_rows"],
                    "subset_rows": len(subset_data),
                    "notes": fmt["notes"],
                }
            )

    xhd_size = rows[0]["size_bytes"]
    print("\nXHD mixed-data comparison report")
    print("=" * 96)
    print("Generated data:")
    counts = count_records(data)
    print(f"  Event stream rows : {counts['event_rows']:,} across {len(EVENT_CHUNK_COUNTS)} chunks")
    print(f"  Metric table rows : {counts['metric_rows']:,}")
    print(f"  Position values   : {counts['position_values']:,} float64 values")
    print(f"  ID values         : {counts['id_values']:,} uint32 values")
    print(f"  Subset read target: events chunk {selected_chunk} ({selected_count:,} rows)")
    print()
    print(
        f"{'Format':<10} {'Size MB':>9} {'vs XHD':>8} {'Write ms':>10} "
        f"{'Read all ms':>12} {'Subset ms':>10} {'Rows':>8} {'Subset':>8}"
    )
    print("-" * 96)
    for row in rows:
        size_ratio = row["size_bytes"] / xhd_size if xhd_size else 0
        print(
            f"{row['format']:<10} "
            f"{row['size_bytes'] / (1024 * 1024):>9.3f} "
            f"{size_ratio:>8.2f}x "
            f"{row['write_seconds'] * 1000:>10.2f} "
            f"{row['read_all_seconds'] * 1000:>12.2f} "
            f"{row['subset_seconds'] * 1000:>10.2f} "
            f"{row['all_rows']:>8,} "
            f"{row['subset_rows']:>8,}"
        )

    print("\nFormat notes")
    print("-" * 96)
    for row in rows:
        print(f"{row['format']:<10} {row['notes']}")


def normalize_loaded_data(data):
    events = data["events"]
    if events and isinstance(next(iter(events.keys())), str):
        events = {int(chunk_id): rows for chunk_id, rows in events.items()}
    return {
        "events": events,
        "metrics": data["metrics"],
        "positions": data["positions"],
        "ids": data["ids"],
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="XHD integrity stress tests and format comparison.")
    parser.add_argument(
        "--compare-formats",
        action="store_true",
        help="Print a mixed-data comparison against JSON, JSON.gz, Pickle, and CSV.",
    )
    args, unittest_args = parser.parse_known_args()
    if args.compare_formats:
        run_format_comparison_report()
    else:
        unittest.main(argv=[sys.argv[0]] + unittest_args)
