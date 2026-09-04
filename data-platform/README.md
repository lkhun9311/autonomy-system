# data-platform

**A lakehouse and data-quality platform for multimodal autonomous-driving sensor logs.**

The question this answers: **as sensor logs accumulate, can a training dataset be produced from
them reproducibly?** Reproducibly means three things at once — the same snapshot yields the same
dataset, corruption and gaps cannot pass silently, and a failing slice can be found and promoted
into the next dataset version. Close all three and it is a platform. Close only the first and it is
an ETL demo.

## Why this stack

Chosen from the frequency of requirements across 20 Korean data-platform postings, not from taste.

| Requirement | Postings | Where it lands |
|---|---:|---|
| Spark / PySpark | 18/20 | M1 |
| Airflow | 18/20 | M2 |
| Python | 16/20 | M1 |
| Kafka | 14/20 | M4, envelopes only |
| Kubernetes | 10/20 | already held |
| SQL / data modelling | 10/20 | M1 |
| Trino / Presto | 5/20 | M4 |
| Iceberg / Delta / Hudi | 4/20 | M1 and M4 |
| MongoDB | 1/20 | **not used** |

Deliberately excluded: Flink (9/20 is not low, but learning a fourth engine while Spark, Kafka and
Airflow are all at zero lines means none of them gets deep), managed warehouses (they take `$/TB`
out of our hands), dbt (this is a platform, not analytics engineering), and MongoDB — one posting
in twenty, and in that one it appears as an example of database experience rather than as a
requirement. Adding it without a question it answers would be a list of technologies.

Iceberg over Delta because the Korean sample mentions Iceberg twice against Delta once; Delta
returns in M4 as a comparison rather than a replacement, since the autonomous-driving postings that
name a format name Delta.

## The boundary

Getting this wrong in either direction is expensive. Reimplementing snapshots is waste; assuming
the table format covers blob integrity is a correctness bug.

| Concern | The table format provides | This project implements |
|---|---|---|
| Atomicity | snapshot-level atomic commit | grouping N sensors of one scene into a single domain commit |
| Versioning | snapshot history, time travel | dataset *release* semantics — approval, deprecation, training eligibility |
| Schema | schema evolution | nuScenes normalisation, per-sensor field model, compatibility policy |
| Slicing | SQL predicates over rows | a slice preserving a consistent sensor set, calibration version, missing-frame policy, blob existence |
| Catalog | table identifier → metadata location | the domain catalog: sensor, sample, blob, provenance, release state |
| Quality | — | referential integrity, blob existence/size/checksum, sensor coverage, timestamp contracts |
| Orchestration | — | retry, idempotency, backfill, repair, gating, alerting |
| Blob lifecycle | tracks its own data files | external JPEG/`.bin` upload, checksum, orphan detection, retention |

**The blob row is the load-bearing decision.** Sensor payloads do not go into a `binary` column.
Metadata rows carry `blob_uri`, `byte_size` and `checksum`; payloads live in object storage. Said
plainly: **a snapshot does not protect the contents of the object `blob_uri` points at.** Overwrite
or delete that object and the table is silently wrong. Content-addressed keys, immutable object
policy, checksum verification and orphan reconciliation are ours to build.

## Milestones

Ordered by dependency, not by calendar. Throughput measured before reconciliation is proven
measures nothing, and embeddings over a corpus that cannot be searched cannot be evaluated.

**M1 — canonical lakehouse.** PySpark normalises `scene`/`sample`/`sample_data` into one scene
commit; the blob contract is established; clock skew, missing sensors, duplicates and out-of-order
frames are detected; failures land in a quarantine table rather than in the release.
*Excluded here: throughput headlines, S3, clusters, the Delta comparison.*

**M2 — reproducible training supply.** Airflow runs backfill → quality gate → publish, with an
injected task failure repaired idempotently and partial backfill by scene, sensor, date or quality
slice. S3, IAM/IRSA and `terraform plan` in CI, plus one small cluster run that proves deployment
and permissions — not performance. A PyTorch dataloader measures what actually reaches training.

**M3 — temporal search and evaluation.** OpenCLIP produces segment embeddings keyed by
`scene_id · camera · start_ts · end_ts · embedding_version`; text and image queries return time
ranges, not files; Recall@K and latency say whether the search is right rather than whether it
exists. Query set and ground truth are fixed *before* the embeddings are built.

**M4 — event layer and format comparison.** Kafka carries envelopes — `uri`, `checksum`,
`schema_version`, `start/end_ts` — never sensor payloads. Iceberg and Delta meet on one shared
late-arriving backfill workload rather than a feature checklist. Trino provides the SQL layer.

**M5 — failure mining and promotion.** Three to five corner-case queries (excessive clock skew,
camera blackout, sparse LiDAR, hard braking, annotation disagreement) produce slices that get
promoted into an evaluation set and a new dataset version, closing the loop back to M1. An optional
MCAP/rosbag2 adapter covers the interchange concept that US robotics postings ask for by
description rather than by product name.

## Measurement rules

Fixed before any measurement, because these are what make a later number mean something.

- **`completeness` on undamaged input is 100%.** Anything less is data loss, not an achievement.
  Fault-detection recall is a different number on a different line; the two are never merged, and
  recall is reported per fault type rather than averaged.
- **`freshness` does not exist until M4.** A static dataset has none. Once events flow it is
  defined as *snapshot-queryable time − emit time* and labelled a replayed event-time workload.
  Measured against the original sensor timestamp it would only measure how old nuScenes is.
- **`Recall@K` needs its ground truth first.** Choosing the correct answers after seeing the search
  results is not evaluation, it is justification.
- **`$/TB` needs an honest denominator.** Structured bytes, blob bytes read for hashing, bytes
  written and total corpus referenced are counted separately. Reading 1 GB of JSON while
  *referencing* 350 GB of blobs is not 0.35 TB of processing.
- **A gate that cannot be shown to fail has not been shown to run.** Disabling one must turn its
  test red. A check that cannot tell "did not fire" from "passed" is worse than none, because it
  manufactures confidence.
- **Negative results stay.** A run that fails its pre-registered checks is published as one.

## Where this runs

Measured on the development machine: 32 cores, 59 GiB RAM, 1.2 TB free, NVMe at 18 GB/s read,
sha256 at 0.5 GiB/s per core. No local NVIDIA GPU.

M1, M4 and M5 run entirely locally at no cloud cost. M2 needs a small amount of AWS to prove the
IaC and deployment path; M3 can embed on CPU in a couple of hours or on a rented GPU in about ten
minutes. Source data stays local and only a slice is uploaded — storing the full corpus in object
storage would consume roughly a third of the monthly budget every month, which is a design
decision rather than a saving.

## Status

Pre-registration stage. No measurements have been taken. When numbers exist they appear here with
the baseline, the workload, the hardware, the commit hash and the repetition count — or they do
not appear at all.
