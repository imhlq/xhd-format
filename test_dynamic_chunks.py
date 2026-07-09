import os
import time
from xhd_format import XhdArchive, DATASET_STREAM, COMPRESS_NONE

# ==============================================================================
# Dynamic Length Chunk Streaming Test
# ==============================================================================
# Simulates a simulation run of 20 ticks (sets), each outputting a different 
# number of interaction records based on collision frequency:
#
# Chunk Sizes:
# - Set 0: 50,000 events
# - Set 1: 100,000 events
# - ...
# - Set 5: 300,000 events
# - Set 19: 1,000,000 events
# - Total Events: 10.5 Million
# ==============================================================================

def generate_dynamic_chunk(chunk_id, count):
    """Generates a dynamic number of collision events."""
    events = []
    base_idx = chunk_id * 10000000  # Enforce unique event IDs
    for i in range(count):
        events.append({
            "interaction_num": base_idx + i,
            "iteration": chunk_id,
            "particle_id": (i * 17) % 80000,
            "energy": (i * 0.08) % 150.0,
            "type": (i % 4) + 1
        })
    return events


def run_dynamic_chunks_demo():
    filename = "dynamic_simulation.xhd"
    num_chunks = 20
    
    print("=" * 75)
    print(f"STREAMING 20 DYNAMIC SETS (Total: 10.5 Million Events)")
    print("=" * 75)

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

    # 1. Start writing dynamic chunks to archive
    t_start = time.time()
    archive = XhdArchive(filename, mode='w')
    archive.create_dataset("collisions", DATASET_STREAM, stream_spec, compression=COMPRESS_NONE)

    total_records = 0
    for chunk_idx in range(num_chunks):
        # Calculate dynamic count for this chunk: 50,000 * (chunk_idx + 1)
        chunk_count = 50000 * (chunk_idx + 1)
        total_records += chunk_count
        
        t0 = time.time()
        chunk_data = generate_dynamic_chunk(chunk_idx, chunk_count)
        archive.write_chunk("collisions", chunk_id=chunk_idx, data_payload=chunk_data)
        
        print(f"[*] Streamed Set {chunk_idx:<2} : {chunk_count:>9,} events | Write: {time.time() - t0:.4f}s")
        del chunk_data

    archive.close()
    total_elapsed = time.time() - t_start

    file_size_mb = os.path.getsize(filename) / (1024 * 1024)
    print("\n" + "=" * 75)
    print("WRITE COMPLETE")
    print("=" * 75)
    print(f"    - Total Events Streamed : {total_records:,}")
    print(f"    - File Size on Disk     : {file_size_mb:.2f} MB")
    print(f"    - Overall Writing Time  : {total_elapsed:.4f} seconds")
    print("=" * 75)

    # --------------------------------------------------------------------------
    # 2. Re-open and load Set 5 (dynamic chunk 5)
    # --------------------------------------------------------------------------
    print("\n[*] Re-opening archive to stream Set 5...")
    archive = XhdArchive(filename, mode='r')

    # Load chunk catalog metadata to display different lengths
    print("\n--- Catalog Details (Subset of sets for display) ---")
    chunks_meta = archive.directory["collisions"]["chunks"]
    for idx in [0, 5, 10, 19]:
        meta = chunks_meta[str(idx)]
        print(f"    - Set {idx:<2}: Offset = {meta['offset']:<12} | Length = {meta['length']:<10} bytes | Records = {meta['record_count']:,}")

    # Load Set 5 (dynamic size = 300,000 events)
    target_set = 5
    t_read_start = time.time()
    set_5_events = archive.read_chunk("collisions", chunk_id=target_set)
    t_read = time.time() - t_read_start

    print(f"\n[+] Loaded Set {target_set} directly from disk in {t_read:.4f} seconds!")
    print(f"    - Recieved Record Count: {len(set_5_events):,} events (Expected: 300,000)")
    print(f"    - First Event: Int#={set_5_events[0]['interaction_num']} | Iteration={set_5_events[0]['iteration']} | Energy={set_5_events[0]['energy']:.2f}")
    print(f"    - Last Event : Int#={set_5_events[-1]['interaction_num']} | Iteration={set_5_events[-1]['iteration']} | Energy={set_5_events[-1]['energy']:.2f}")

    archive.close()

    # Clean up test file to reclaim disk space
    try:
        os.remove(filename)
    except FileNotFoundError:
        pass


if __name__ == '__main__':
    run_dynamic_chunks_demo()
