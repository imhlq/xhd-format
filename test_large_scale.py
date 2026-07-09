import os
import time
from xhd_format import XhdArchive, DATASET_STREAM, COMPRESS_NONE

# ==============================================================================
# Large-Scale Simulation Streaming Benchmark (10,000,000+ Events)
# ==============================================================================
# Tests the performance of streaming 10 million interaction records across 
# multiple dynamic chunks to simulate a real-world scientific simulation run.
#
# Optimization Strategy for Memory:
# - Instead of generating 10 million records in one massive list (which would 
#   exhaust Python RAM), we generate and stream 10 chunks of 1,000,000 events 
#   each, flushing memory on each step.
# ==============================================================================

def generate_chunk_data(chunk_id, count):
    """Generates 1 million dummy simulation records in memory."""
    events = []
    # Pre-allocating to avoid array resizing overhead
    base_idx = chunk_id * count
    for i in range(count):
        events.append({
            "interaction_num": base_idx + i,
            "iteration": (base_idx + i) // 100,
            "particle_id": (i * 13) % 100000,
            "energy": (i * 0.05) % 500.0,
            "type": (i % 5) + 1
        })
    return events


def run_large_scale_benchmark():
    filename = "large_scale_simulation.xhd"
    num_chunks = 10
    records_per_chunk = 1000000
    total_records = num_chunks * records_per_chunk

    print("=" * 70)
    print(f"LAUNCHING LARGE-SCALE STREAMING BENCHMARK ({total_records:,} EVENTS)")
    print("=" * 70)

    # 1. Define dataset schema
    stream_spec = {
        "schema": [
            {"name": "interaction_num", "type": "Q"},
            {"name": "iteration", "type": "I"},
            {"name": "particle_id", "type": "I"},
            {"name": "energy", "type": "f"},
            {"name": "type", "type": "B"},
            {"name": "padding", "type": "3x"}  # 8-byte aligned structure
        ]
    }

    # 2. Open archive and write chunks sequentially (hot-path streaming)
    t_start = time.time()
    archive = XhdArchive(filename, mode='w')
    archive.create_dataset("collisions", DATASET_STREAM, stream_spec, compression=COMPRESS_NONE)

    total_write_time = 0.0

    for chunk_idx in range(num_chunks):
        t_gen_start = time.time()
        # Generate 1M records in RAM
        chunk_data = generate_chunk_data(chunk_idx, records_per_chunk)
        t_gen = time.time() - t_gen_start

        # Time only the file-writing and catalog-updating section
        t_write_start = time.time()
        archive.write_chunk("collisions", chunk_id=chunk_idx, data_payload=chunk_data)
        t_write = time.time() - t_write_start
        total_write_time += t_write

        print(f"[*] Streamed Chunk {chunk_idx}: 1,000,000 events [Gen: {t_gen:.2f}s | Write: {t_write:.2f}s]")
        
        # Free memory immediately
        del chunk_data

    archive.close()
    total_elapsed = time.time() - t_start

    file_size_mb = os.path.getsize(filename) / (1024 * 1024)
    print("\n" + "=" * 70)
    print("WRITE BENCHMARK COMPLETE")
    print("=" * 70)
    print(f"    - Total Records Written : {total_records:,}")
    print(f"    - File Size on Disk     : {file_size_mb:.2f} MB")
    print(f"    - Total Disk Write Time : {total_write_time:.4f} seconds")
    print(f"    - Overall Elapsed Time  : {total_elapsed:.4f} seconds (inc. generation)")
    print(f"    - Average Write Speed   : {total_records / total_write_time:,.0f} records/sec")
    print("=" * 70)

    # --------------------------------------------------------------------------
    # 3. Read specific chunks back (simulating a visualization loading Set 5)
    # --------------------------------------------------------------------------
    print("\n[*] Re-opening unified archive to simulate selective loading...")
    
    t_open_start = time.time()
    archive = XhdArchive(filename, mode='r')
    t_open = time.time() - t_open_start
    print(f"[+] Archive metadata loaded in {t_open:.6f} seconds.")

    # Get dynamic chunk list
    chunk_list = list(archive.directory["collisions"]["chunks"].keys())
    print(f"    - Catalog detected {len(chunk_list)} dynamic sets: {chunk_list}")

    # Load only Chunk 5 (Set 5 - holding events 5,000,000 to 6,000,000)
    target_chunk = 5
    t_read_start = time.time()
    events_set_5 = archive.read_chunk("collisions", chunk_id=target_chunk)
    t_read = time.time() - t_read_start

    print(f"\n[+] Loaded Chunk {target_chunk} (1,000,000 events) in {t_read:.4f} seconds!")
    print(f"    - First Event: Int#={events_set_5[0]['interaction_num']} | Energy={events_set_5[0]['energy']:.2f}")
    print(f"    - Last Event : Int#={events_set_5[-1]['interaction_num']} | Energy={events_set_5[-1]['energy']:.2f}")

    archive.close()

    # Clean up test file to reclaim disk space
    try:
        os.remove(filename)
    except FileNotFoundError:
        pass


if __name__ == '__main__':
    run_large_scale_benchmark()
