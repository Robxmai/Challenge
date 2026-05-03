"""
Pipeline entry point. Orchestrates the medallion stages in order:

  1. Ingest  — raw source files => Bronze layer Delta tables
  2. Transform — Bronze => Silver layer Delta tables
  3. Provision — Silver => Gold layer Delta tables
  4. Stream   — (Stage 3) Poll /data/stream/ for micro-batch JSONL files

The scoring system invokes this directly:
  docker run ... candidate-submission:latest python pipeline/run_all.py

No interactive input. Exits 0 on success, non-zero on failure.

Stage 3: The streaming path runs AFTER the batch pipeline. All 12 stream
files are pre-staged at /data/stream/. The polling loop processes them in
filename order and exits when all files are consumed (quiescence).
"""

import os
import sys
import time

from pipeline.ingest import run_ingestion
from pipeline.transform import run_transformation
from pipeline.provision import run_provisioning
from pipeline.stream_ingest import run_stream_ingestion


def is_stream_data_available():
    """Check if Stage 3 streaming data is present at /data/stream/."""
    stream_path = "/data/stream"
    if not os.path.isdir(stream_path):
        return False
    try:
        files = [f for f in os.listdir(stream_path) if f.endswith(".jsonl")]
        return len(files) > 0
    except (OSError, PermissionError):
        return False


if __name__ == "__main__":
    pipeline_start = time.time()

    t0 = time.time()
    run_ingestion()
    t1 = time.time()
    elapsed = t1 - t0
    print(f"[Ingest] Completed in {int(elapsed//60)}m {elapsed%60:.1f}s")

    t0 = time.time()
    run_transformation()
    t1 = time.time()
    elapsed = t1 - t0
    print(f"[Transform] Completed in {int(elapsed//60)}m {elapsed%60:.1f}s")

    t0 = time.time()
    run_provisioning()
    t1 = time.time()
    elapsed = t1 - t0
    print(f"[Provision] Completed in {int(elapsed//60)}m {elapsed%60:.1f}s")

    if is_stream_data_available():
        t0 = time.time()
        run_stream_ingestion()
        t1 = time.time()
        elapsed = t1 - t0
        print(f"[Stream] Completed in {int(elapsed//60)}m {elapsed%60:.1f}s")

    total_seconds = round(time.time() - pipeline_start, 1)
    total_min = int(total_seconds // 60)
    total_sec = total_seconds % 60
    print(f"\nPipeline completed in {total_min}m {total_sec:.1f}s (exit code 0)")

    sys.exit(0)