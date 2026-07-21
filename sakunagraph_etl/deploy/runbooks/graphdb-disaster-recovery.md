# GraphDB backup, restore, and disaster recovery

These procedures target GraphDB 11's recovery REST API. Run backup and restore
commands with an administrator account and treat archives as sensitive because
system-data backups may contain user configuration.

## Backup and verification

```bash
sakuna-etl graphdb-admin --host https://graphdb.example \
  --repo sakunagraph backup --out /backups/sakunagraph-2026-07-18.tar

sakuna-etl graphdb-admin verify /backups/sakunagraph-2026-07-18.tar
sakuna-etl graphdb-admin --host https://graphdb.example status
```

The response streams to a temporary file before the backup is committed
write-once and receives a SHA-256 metadata sidecar. An identical retry is
accepted. When its TAR compression is inspectable, the command also requires
GraphDB's `.success` marker. Copy the archive and sidecar to the
retention-managed backup prefix, then perform a restore drill at least
quarterly.

## Restore drill and recovery

1. Provision an isolated GraphDB instance running a backup-compatible version.
2. Copy the archive and sidecar to the drill worker and verify them.
3. Restore into the isolated instance. The confirmation must match the target
   repository exactly:

   ```bash
   sakuna-etl graphdb-admin --host https://graphdb-drill.example \
     --repo sakunagraph restore /backups/sakunagraph-2026-07-18.tar \
     --confirm-repository sakunagraph
   ```

4. Run repository health checks, triple counts, competency queries, and a
   canonical event sample. Record achieved RTO/RPO and the GraphDB versions.
5. For production recovery, restore into a replacement instance when possible,
   validate it, then switch traffic. Avoid `--remove-stale-repositories` unless
   the incident commander explicitly requires a full-instance replacement.

## Full rebuild from RDF manifests

If no compatible binary backup exists, rebuild a new maintenance repository
from verified Stage 6/7 manifests:

```bash
sakuna-etl graphdb-admin --host https://graphdb-rebuild.example \
  --profile onprem full-rebuild \
  --target-repository sakunagraph-rebuild \
  --confirm-target sakunagraph-rebuild \
  --manifest /artifacts/runs/psgc/RUN/manifest.json \
  --manifest /artifacts/runs/emdat/RUN/manifest.json \
  --manifest /artifacts/runs/gda/RUN/manifest.json \
  --manifest /artifacts/runs/ndrrmc/RUN/manifest.json \
  --manifest /artifacts/runs/dromic/RUN/manifest.json \
  --manifest /artifacts/runs/alignment/RUN/manifest.json
```

All manifests and Turtle files are checksum/parsing preflighted before the
target is cleared. The first manifest clears the maintenance repository; later
contexts use atomic named-graph replacement. Never point this operation at the
active production repository during a drill.

## Tested recovery controls

CI unit tests exercise backup checksum/marker verification, corrupted-backup
rejection, restore multipart parameters, manifest preflight, and the explicit
repository confirmations. A real quarterly drill remains required because
license, dataset size, network throughput, and GraphDB version compatibility
cannot be simulated by unit tests.
