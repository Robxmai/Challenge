# Test_Deployment — Nedbank DE Challenge

Copy this folder to any server with Docker, then run:

```bash
chmod +x run.sh
./run.sh
```

## Prerequisites

- **Docker** installed and running
- **Base image** `nedbank-de-challenge/base:1.0` loaded

If you don't have the base image, load it from the provided archive:
```bash
docker load < nedbank-de-challenge-base.tar
```
Or build from `Dockerfile.base` (in the `infrastructure/` folder of the challenge pack).

## What it does

1. Builds the pipeline Docker image (offline — no network)
2. Creates test data at `/tmp/de-challenge-test-data/`
3. Runs the pipeline with scoring-equivalent constraints (2 GB RAM, 2 vCPU, no network)
4. Verifies all 9 Delta tables exist in Bronze / Silver / Gold
5. Reports duration and exit code

## Files

| File | Purpose |
|---|---|
| `run.sh` | Test runner — build, run, verify |
| `Dockerfile` | Extends base image with pipeline code |
| `requirements.txt` | Extra dependencies (empty — base image has everything) |
| `pipeline/` | Medallion pipeline code (ingest → transform → provision) |
| `config/` | Pipeline config + DQ rules (externalised) |
| `sample_data/` | Small cross-referenced dataset for testing |

## Sample data

5 customers, 5 accounts, 8 transactions — all properly cross-referenced for referential integrity. Sized to run in under 60 seconds.
