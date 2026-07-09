import os
import struct
import tempfile
import unittest
import zlib

from xhd_format import Compression, CorruptedFileError, DatasetType, MAGIC_BYTES, VERSION, XhdArchive


class XhdApiTests(unittest.TestCase):
    def temp_path(self):
        handle = tempfile.NamedTemporaryFile(suffix=".xhd", delete=False)
        handle.close()
        os.remove(handle.name)
        self.addCleanup(lambda: os.path.exists(handle.name) and os.remove(handle.name))
        return handle.name

    def test_context_manager_and_string_options(self):
        path = self.temp_path()
        schema = [
            {"name": "id", "type": "I"},
            {"name": "label", "type": "8s"},
        ]

        with XhdArchive.create(path) as archive:
            archive.create_dataset("events", "stream", {"schema": schema}, compression="none")
            archive.write("events", 0, [{"id": 7, "label": "alpha"}])

        with XhdArchive.open(path) as archive:
            self.assertEqual(archive.list_datasets(), ["events"])
            self.assertEqual(archive.list_chunks("events"), ["0"])
            self.assertEqual(archive.read("events", 0), [{"id": 7, "label": "alpha"}])
            self.assertEqual(archive.dataset_info("events")["type"], "stream")

    def test_same_size_overwrite_updates_checksum(self):
        path = self.temp_path()

        with XhdArchive.create(path) as archive:
            archive.create_stream("events", [{"name": "value", "type": "I"}])
            archive.write_chunk("events", 0, [{"value": 1}])
            archive.write_chunk("events", 0, [{"value": 2}])

        with XhdArchive.open(path) as archive:
            self.assertEqual(archive.read_chunk("events", 0), [{"value": 2}])

    def test_array_roundtrip_is_row_major(self):
        path = self.temp_path()

        with XhdArchive.create(path) as archive:
            archive.create_array("positions", shape=(2, 3), dtype="d")
            archive.write("positions", 0, [1.0, 2.0, 3.0, 4.0, 5.0, 6.0])

        with XhdArchive.open(path) as archive:
            self.assertEqual(
                archive.read("positions", 0),
                [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]],
            )

    def test_validation_rejects_bad_schema_and_payload(self):
        path = self.temp_path()

        with XhdArchive.create(path) as archive:
            with self.assertRaises(ValueError):
                archive.create_dataset("bad", DatasetType.STREAM, {"schema": []})

            archive.create_array("positions", [2, 3], "d", compression=Compression.ZLIB)
            with self.assertRaises(ValueError):
                archive.write("positions", 0, [1.0, 2.0])

    def test_corrupted_directory_shape_is_reported(self):
        path = self.temp_path()

        with XhdArchive.create(path) as archive:
            archive.create_stream("events", [{"name": "value", "type": "I"}])

        dir_offset = XhdArchive.HEADER_SIZE
        dir_bytes = b"[]"
        header_pre = struct.pack(
            XhdArchive.HEADER_PREFIX_FORMAT,
            MAGIC_BYTES,
            VERSION,
            dir_offset,
            len(dir_bytes),
        )
        checksum = zlib.crc32(header_pre) & 0xFFFFFFFF
        header = struct.pack(
            XhdArchive.HEADER_FORMAT,
            MAGIC_BYTES,
            VERSION,
            dir_offset,
            len(dir_bytes),
            checksum,
        )

        with open(path, "r+b") as handle:
            handle.seek(0)
            handle.write(header)
            handle.seek(dir_offset)
            handle.write(b"[]")
            handle.truncate(dir_offset + len(dir_bytes))

        with self.assertRaises(CorruptedFileError):
            XhdArchive.open(path)


if __name__ == "__main__":
    unittest.main()
