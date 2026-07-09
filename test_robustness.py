import os
import zlib
import struct
from xhd_format import XhdArchive, DATASET_NARRAY, COMPRESS_NONE, CorruptedFileError

# ==============================================================================
# Robustness Validation Suite for .xhd (xHou Data)
# ==============================================================================
# This script injects errors, corrupts bytes, and truncates files to verify 
# that the XHD format detects failures cleanly rather than crashing silently.
# ==============================================================================

def create_healthy_file(filename):
    """Creates a baseline valid XHD file with coordinate datasets."""
    archive = XhdArchive(filename, mode='w')
    archive.create_dataset("positions", DATASET_NARRAY, {"shape": [2, 3], "dtype": "d"}, compression=COMPRESS_NONE)
    
    # 2 bodies, 3 coords (2 * 3 * 8 = 48 bytes payload)
    archive.write_chunk("positions", chunk_id=0, data_payload=[1.0, 2.0, 3.0, 4.0, 5.0, 6.0])
    archive.close()


def test_random_bit_corruption(filename):
    """Flips bytes in the raw data chunk to verify CRC32 catches it."""
    print("\n--- Test 1: Random Bit Corruption ---")
    create_healthy_file(filename)

    # 1. Open archive to retrieve the active chunk's exact offset on disk
    archive = XhdArchive(filename, mode='r')
    chunk_offset = archive.directory["positions"]["chunks"]["0"]["offset"]
    archive.close()

    # 2. Corrupt a byte in the exact active chunk data payload
    with open(filename, "r+b") as f:
        f.seek(chunk_offset)  
        original_byte = f.read(1)
        f.seek(chunk_offset)
        corrupted_byte = bytes([original_byte[0] ^ 0xFF])
        f.write(corrupted_byte)

    # 3. Try reading it back
    archive = XhdArchive(filename, mode='r')
    try:
        data = archive.read_chunk("positions", chunk_id=0)
        print("[-] Test Failed: Reader loaded corrupted data without warning.")
    except CorruptedFileError as e:
        print(f"[+] Test Passed! Reader successfully caught corruption: {e}")
    finally:
        archive.close()


def test_header_corruption(filename):
    """Flips bytes in the header block to verify header checksum validation."""
    print("\n--- Test 2: Header Block Corruption ---")
    create_healthy_file(filename)

    # Corrupt a byte in the file header (offsets 0 to 32)
    with open(filename, "r+b") as f:
        f.seek(10)  # Seek to directory offset pointer
        # Write 0xFF to ensure the header bytes change and fail CRC32 Checksum
        f.write(b'\xFF')

    # Try opening the archive
    try:
        archive = XhdArchive(filename, mode='r')
        print("[-] Test Failed: Reader opened file with corrupted header pointers.")
        archive.close()
    except CorruptedFileError as e:
        print(f"[+] Test Passed! Reader caught corrupted header pointers: {e}")


def test_file_truncation(filename):
    """Truncates the file to simulate sudden stop while writing."""
    print("\n--- Test 3: Truncation / Sudden Write Stop ---")
    create_healthy_file(filename)

    # Truncate the file, cutting off the last 10 bytes (where the directory is)
    file_size = os.path.getsize(filename)
    with open(filename, "r+b") as f:
        f.truncate(file_size - 10)

    # Try opening it
    try:
        archive = XhdArchive(filename, mode='r')
        print("[-] Test Failed: Reader opened a truncated file.")
        archive.close()
    except CorruptedFileError as e:
        print(f"[+] Test Passed! Reader caught truncated index directory: {e}")


def test_crash_recovery_during_append(filename):
    """Simulates a crash during append, verifying that XHD rolls back to the last stable state."""
    print("\n--- Test 4: Crash Recovery During Chunk Append ---")
    
    # 1. Create a stable file with chunk 0
    archive = XhdArchive(filename, mode='w')
    archive.create_dataset("positions", DATASET_NARRAY, {"shape": [1, 3], "dtype": "d"}, compression=COMPRESS_NONE)
    archive.write_chunk("positions", chunk_id=0, data_payload=[1.0, 1.0, 1.0])
    archive.close()
    
    # 2. Simulate appending a new chunk 1, but crash (close stream) BEFORE directory catalog updates
    # We append raw bytes to the end of the file, simulating an unfinished file write.
    with open(filename, "ab") as f:
        f.write(struct.pack('<3d', 9.9, 9.9, 9.9))
    
    # 3. Re-open archive. It should load the last stable state (chunk 0) and ignore the junk appended bytes
    # because the header's directory pointer still points to the old directory, which is fully intact!
    archive = XhdArchive(filename, mode='r')
    try:
        # Check if chunk 0 is still valid and readable
        val = archive.read_chunk("positions", chunk_id=0)
        print(f"[+] Chunk 0 is valid: {val}")
        
        # Verify chunk 1 is NOT visible because it crashed before directory indexing
        try:
            archive.read_chunk("positions", chunk_id=1)
            print("[-] Test Failed: Incomplete chunk was indexed.")
        except IndexError:
            print("[+] Test Passed! Incomplete chunk 1 was ignored, file recovered to last stable state.")
            
    except CorruptedFileError as e:
        print(f"[-] Test Failed: Stable recovery failed: {e}")
    finally:
        archive.close()


if __name__ == '__main__':
    filename = "test_robustness.xhd"
    
    test_random_bit_corruption(filename)
    test_header_corruption(filename)
    test_file_truncation(filename)
    test_crash_recovery_during_append(filename)
    
    # Clean up test file
    try:
        os.remove(filename)
    except FileNotFoundError:
        pass
