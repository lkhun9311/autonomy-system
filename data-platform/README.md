# data-platform

**A lakehouse and data-quality platform for multimodal autonomous-driving sensor logs.**

## Current scope

Ingesting nuScenes scene/sample/sensor metadata into Iceberg tables, cataloguing the external
sensor blobs those rows reference, and proving — with injected faults — that the quality gates
actually fire.

That is the whole of it right now.

Simulation, robot runtime, perception models, GPU workloads, visualization, and a public SDK are
**not implemented** and are not on this project's path.

## The boundary this project is about

Getting this wrong in either direction is expensive: reimplementing snapshots is waste, while
assuming the table format covers blob integrity is a correctness bug.

| Concern | Iceberg provides | This project implements |
|---|---|---|
| Atomicity | snapshot-level atomic commit | grouping N sensors of one scene into a single domain commit |
| Versioning | snapshot history, time travel, branch/tag | dataset *release* semantics — approval, deprecation, training eligibility |
| Schema | schema evolution | nuScenes normalisation, per-sensor field model, compatibility policy |
| Slicing | SQL predicates over rows | a slice preserving a consistent sensor set, calibration version, missing-frame policy, blob existence |
| Catalog | table identifier → metadata location | the domain catalog: sensor, sample, blob, owner, provenance, release state |
| Quality | — | referential integrity, blob existence/size/checksum, sensor coverage, timestamp contracts |
| Orchestration | — | retry, idempotency, backfill, repair, gating, alerting |
| Blob lifecycle | tracks its own data files | external JPEG/`.bin` upload, checksum, orphan detection, retention, deletion |

**The blob row is the load-bearing decision.** Sensor payloads do not go into a `binary` column.
Metadata rows carry `blob_uri`, `byte_size` and `checksum`; payloads live in object storage. The
consequence has to be said plainly: **an Iceberg snapshot does not protect the contents of the
object `blob_uri` points at.** Overwrite or delete that object and the table is silently wrong.
Content-addressed keys, immutable object policy, checksum verification and orphan reconciliation
are ours to build.

## Milestones

Ordered by dependency, not by calendar. Each is gated on the previous one being *correct*.

**M1 — correctness slice.** nuScenes `v1.0-mini`. Normalise `scene`/`sample`/`sample_data` into a
manifest, establish the blob contract, write one Iceberg snapshot, reconcile source ↔ table ↔ blob.
Deliberately excluded: throughput headlines, multi-table partition design, S3, EKS. Reconciliation
has to be right before a rate means anything.

**M2 — backfill, orchestration, infrastructure.** `trainval` metadata and blob catalog backfill.
Spark locally. Schema and partition evolution. A DAG running backfill → quality gate → publish, with
an injected task failure repaired idempotently. S3, IAM/IRSA, lifecycle, encryption, `terraform plan`
in CI, and one small smoke run on a cluster — proving deployment and reproducibility, not performance.

**M3 — streaming and cost.** A replay producer and consumer with event-time and ingest-time held
apart. Deduplication and idempotency. Micro-batch commits. Late-arrival repair. Query and file-count
compared across compaction. The same manifest reproduced locally and remotely.

## Measurement rules

Fixed before any measurement, because these are what make a later number mean something.

- **`freshness` does not exist until M3.** A static dataset has no freshness. When replay lands it is
  defined as *snapshot-queryable time − replay producer emit time*, and labelled a replayed
  event-time workload. Measuring against the original sensor timestamp would measure how old
  nuScenes is.
- **`completeness` on undamaged input is 100%.** Anything less is data loss, not an achievement.
  Fault-detection recall is a different number on a different line; the two are never merged.
- **`$/TB` needs an honest denominator.** Structured bytes, blob bytes read for hashing, bytes
  written, and total corpus referenced are counted separately. Reading 1 GB of JSON while
  *referencing* 350 GB of blobs is not 0.35 TB of processing. A single point is not extrapolated.
- **A gate that cannot be shown to fail has not been shown to run.** Disabling one must turn its
  test red. A check that cannot distinguish "did not fire" from "passed" is worse than none,
  because it manufactures confidence.
- **Negative results stay.** A run that fails its pre-registered checks is published as one.

## Status

Pre-registration stage. No measurements have been taken.
