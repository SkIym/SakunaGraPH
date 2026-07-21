# ADR 0001: Stage 6 artifact boundary

Status: Accepted

SakunaGraPH uses content-derived run IDs and write-once artifacts. A run
manifest is committed only after every input and output object has been stored
and verified. Validation failures use the same format below `quarantine/`.

Local files are the reference storage behavior. The on-premise adapter adds
cross-process locks for a shared filesystem; the cloud adapter uses conditional
S3-compatible object creation. The three adapters expose the same keys and
manifest schema, so a run can be materialized without pipeline-specific storage
logic.

DROMIC's JSON event manifest is authoritative. Producer-specific records are
merged under one lock, and the legacy text files are derived views during the
deprecation window.
