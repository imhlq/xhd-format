import os
import time
import json
import csv
import pickle
import struct
import zlib
from xhd_format import XhdArchive, DATASET_STREAM, COMPRESS_NONE, COMPRESS_ZLIB

# ==============================================================================
# Unified xHou Data Format (.xhd) Benchmark Suite
# ==============================================================================
# Focuses on evaluating the storage size and performance of logging 
# 1,000,000 high-frequency particle interaction events in an astronomy simulation.
#
# Interaction Record Schema:
# - Interaction Number: uint64 (8 bytes)
# - Iteration Number: uint32 (4 bytes)
# - Particle ID: uint32 (4 bytes)
# - Particle Energy: float (4 bytes)
# - Interaction Type: uint8 (1 byte)
# - Padding: 3 bytes (to align to 8-byte boundaries)
#
# Struct Format: '<QIIfB3x' (Total size: 24 bytes)
# ==============================================================================

RECORD_FORMAT = '<QIIfB3x'
RECORD_SIZE = struct.calcsize(RECORD_FORMAT)


def generate_benchmark_events(count):
    """Generates a list of dummy simulation interaction events."""
    print(f"[*] Generating {count:,} simulation events in memory...")
    events = []
    for i in range(count):
        events.append({
            "interaction_num": i,
            "iteration": i // 100,  # e.g., 100 interactions per simulation step
            "particle_id": (i * 7) % 50000,
            "energy": (i * 0.125) % 250.0,
            "type": (i % 5) + 1
        })
    return events


def run_comprehensive_benchmark():
    event_count = 1000000
    events = generate_benchmark_events(event_count)
    results = {}

    print(f"[+] Generation complete. Record size: {RECORD_SIZE} bytes.")
    print("\n" + "=" * 70)
    print(f"STARTING BENCHMARK - 1,000,000 RECORDS")
    print("=" * 70)

    # --------------------------------------------------------------------------
    # 1. Custom XHD Format - Uncompressed Stream (Hot Path Active Logging)
    # --------------------------------------------------------------------------
    stream_spec = {
        "schema": [
            {"name": "interaction_num", "type": "Q"},
            {"name": "iteration", "type": "I"},
            {"name": "particle_id", "type": "I"},
            {"name": "energy", "type": "f"},
            {"name": "type", "type": "B"},
            {"name": "padding", "type": "3x"}
        ]
    }

    t0 = time.time()
    archive = XhdArchive("bench_raw.xhd", mode='w')
    archive.create_dataset("events", DATASET_STREAM, stream_spec, compression=COMPRESS_NONE)
    # Flush in bulk to simulate buffered writing
    archive.write_chunk("events", chunk_id=0, data_payload=events)
    archive.close()
    t_save = time.time() - t0

    t0 = time.time()
    archive = XhdArchive("bench_raw.xhd", mode='r')
    xhd_parsed = archive.read_chunk("events", chunk_id=0)
    archive.close()
    t_load = time.time() - t0

    results["XHD (Uncompressed)"] = {
        "size_mb": os.path.getsize("bench_raw.xhd") / (1024 * 1024),
        "save_time": t_save,
        "load_time": t_load,
        "notes": "O(1) in-place rewrite support, aligned"
    }

    # --------------------------------------------------------------------------
    # 2. Custom XHD Format - Compressed Archive (Cold Path Archival)
    # --------------------------------------------------------------------------
    t0 = time.time()
    archive = XhdArchive("bench_zip.xhd", mode='w')
    archive.create_dataset("events", DATASET_STREAM, stream_spec, compression=COMPRESS_ZLIB)
    archive.write_chunk("events", chunk_id=0, data_payload=events)
    archive.close()
    t_save = time.time() - t0

    t0 = time.time()
    archive = XhdArchive("bench_zip.xhd", mode='r')
    xhd_zip_parsed = archive.read_chunk("events", chunk_id=0)
    archive.close()
    t_load = time.time() - t0

    results["XHD (Compressed)"] = {
        "size_mb": os.path.getsize("bench_zip.xhd") / (1024 * 1024),
        "save_time": t_save,
        "load_time": t_load,
        "notes": "Optimal size, no random write support"
    }

    # --------------------------------------------------------------------------
    # 3. JSON Format
    # --------------------------------------------------------------------------
    t0 = time.time()
    with open("bench.json", "w") as f:
        json.dump(events, f)
    t_save = time.time() - t0

    t0 = time.time()
    with open("bench.json", "r") as f:
        json_parsed = json.load(f)
    t_load = time.time() - t0

    results["JSON"] = {
        "size_mb": os.path.getsize("bench.json") / (1024 * 1024),
        "save_time": t_save,
        "load_time": t_load,
        "notes": "Highly redundant metadata, text parsing"
    }

    # --------------------------------------------------------------------------
    # 4. CSV Format
    # --------------------------------------------------------------------------
    t0 = time.time()
    with open("bench.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["interaction_num", "iteration", "particle_id", "energy", "type"])
        for e in events:
            writer.writerow([e["interaction_num"], e["iteration"], e["particle_id"], e["energy"], e["type"]])
    t_save = time.time() - t0

    t0 = time.time()
    csv_parsed = []
    with open("bench.csv", "r") as f:
        reader = csv.reader(f)
        header = next(reader)
        for r in reader:
            csv_parsed.append({
                "interaction_num": int(r[0]),
                "iteration": int(r[1]),
                "particle_id": int(r[2]),
                "energy": float(r[3]),
                "type": int(r[4])
            })
    t_load = time.time() - t0

    results["CSV"] = {
        "size_mb": os.path.getsize("bench.csv") / (1024 * 1024),
        "save_time": t_save,
        "load_time": t_load,
        "notes": "Compact text, no schemas, slow string parse"
    }

    # --------------------------------------------------------------------------
    # 5. Pickle Format
    # --------------------------------------------------------------------------
    t0 = time.time()
    with open("bench.pkl", "wb") as f:
        pickle.dump(events, f, protocol=pickle.HIGHEST_PROTOCOL)
    t_save = time.time() - t0

    t0 = time.time()
    with open("bench.pkl", "rb") as f:
        pkl_parsed = pickle.load(f)
    t_load = time.time() - t0

    results["Pickle"] = {
        "size_mb": os.path.getsize("bench.pkl") / (1024 * 1024),
        "save_time": t_save,
        "load_time": t_load,
        "notes": "Python-only object graph serialization"
    }

    # Clean up test files to avoid cluttering workspace
    for filename in ["bench_raw.xhd", "bench_zip.xhd", "bench.json", "bench.csv", "bench.pkl"]:
        try:
            os.remove(filename)
        except FileNotFoundError:
            pass

    # Print Results Table
    print("\n" + "=" * 70)
    print("BENCHMARK RESULTS TABLE (1,000,000 EVENTS)")
    print("=" * 70)
    print(f"{'Format':<20} | {'File Size (MB)':<15} | {'Save Time (s)':<15} | {'Load Time (s)':<15}")
    print("-" * 70)
    for format_name, metrics in results.items():
        print(f"{format_name:<20} | {metrics['size_mb']:<15.2f} | {metrics['save_time']:<15.4f} | {metrics['load_time']:<15.4f}")
    print("=" * 70)

    # Print detailed commentary
    print("\n--- Key Interpretations ---")
    print("1. File Size:")
    print(f"   - JSON is massive ({results['JSON']['size_mb']:.2f} MB) because it stores floats/ints as text characters and duplicates key names.")
    print(f"   - XHD Uncompressed ({results['XHD (Uncompressed)']['size_mb']:.2f} MB) fits exactly in its physical mathematical format (24 bytes * 1M events).")
    print(f"   - XHD Compressed ({results['XHD (Compressed)']['size_mb']:.2f} MB) compresses numerical structures extremely well, reducing size to only {results['XHD (Compressed)']['size_mb']/results['XHD (Uncompressed)']['size_mb']*100:.1f}% of raw.")
    
    print("\n2. Speed & Use Case Recommendations:")
    print("   - Active Simulation Logging: Use XHD (Uncompressed). Save speed is highly optimized, and you get in-place overwrite capability.")
    print("   - Post-Simulation Storage: Convert/compact the data into XHD (Compressed). This achieves the smallest footprint for sharing.")


if __name__ == '__main__':
    run_comprehensive_benchmark()
