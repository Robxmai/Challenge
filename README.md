# Pipeline — DE Challenge Submission

Copy this folder to any server with Docker, then run:

```bash
chmod +x run.sh
./run.sh
```

## Prerequisites

- **Docker** installed and running

## Files

| File | Purpose |
|------|---------|
| `Dockerfile` | Extends base image with pipeline code |
| `requirements.txt` | Extra dependencies (empty — base image has everything) |
| `pipeline/` | Medallion pipeline code (ingest → transform → provision) |
| `config/` | Pipeline config + DQ rules (externalised) |

## AI Usage Disclosure

This code was created with the assistance of AI.
