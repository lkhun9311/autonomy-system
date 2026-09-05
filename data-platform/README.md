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
| Kafka | 14/20 | M5, replayed event-time workload |
| Kubernetes | 10/20 | already held |
| SQL / data modelling | 10/20 | M1 |
| Trino / Presto | 5/20 | M1 and M5 |
| Iceberg / Delta / Hudi | 4/20 | M1 |
| MongoDB | 1/20 | **not used** |

Deliberately excluded: Flink (9/20 is not low, but learning a fourth engine while Spark, Kafka and
Airflow are all at zero lines means none of them gets deep), managed warehouses (they take `$/TB`
out of our hands), dbt (this is a platform, not analytics engineering), and MongoDB — one posting
in twenty, and in that one it appears as an example of database experience rather than as a
requirement. Adding it without a question it answers would be a list of technologies.

Iceberg over Delta because the Korean sample mentions Iceberg twice against Delta once. The
autonomous-driving postings that name a format name Delta, so the reason for the choice is written
down here rather than settled by a benchmark: a head-to-head run designed by the person who already
picked the winner is easy to arrange and answers no decision anyone is making. One format is
carried the whole way through instead.

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

The streaming layer comes last, and the reason is not that the dataset is static. Replaying
nuScenes at its recorded timestamps is a real event-time workload — the sensors run at different
rates, so out-of-order arrival and skew between event time and arrival time are properties of the
data rather than noise added to make the exercise look harder. What is missing early is not the
workload but the verdict. Exactly-once is demonstrated by showing that the table built by streaming
matches the table built by batch, row for row, over the same scenes; **the batch pipeline is the
oracle for the streaming one**, so it has to be right first. M5 could be built at any point. It
could not be believed until M1 exists.

**M1 — canonical lakehouse.** PySpark normalises `scene`/`sample`/`sample_data` into one scene
commit; the blob contract is established; clock skew, missing sensors, duplicates and out-of-order
frames are detected; failures land in a quarantine table rather than in the release. Trino reads
the result, snapshot time travel included.
*Excluded here: throughput headlines, S3, clusters, streaming.*

**M2 — reproducible training supply.** Airflow runs backfill → quality gate → publish, with an
injected task failure repaired idempotently and partial backfill by scene, sensor, date or quality
slice. S3, IAM/IRSA and `terraform plan` in CI, plus one small cluster run that proves deployment
and permissions — not performance. A PyTorch dataloader measures what actually reaches training.

**M3 — temporal search and evaluation.** OpenCLIP produces segment embeddings keyed by
`scene_id · camera · start_ts · end_ts · embedding_version`; text and image queries return time
ranges, not files; Recall@K and latency say whether the search is right rather than whether it
exists. Query set and ground truth are fixed *before* the embeddings are built.

**M4 — failure mining and promotion.** Three to five corner-case queries (excessive clock skew,
camera blackout, sparse LiDAR, hard braking, annotation disagreement) produce slices that get
promoted into an evaluation set and a new dataset version, closing the loop back to M1. An optional
MCAP/rosbag2 adapter covers the interchange concept that US robotics postings ask for by
description rather than by product name.

**M5 — streaming ingest and the equivalence proof.** Kafka replays scenes at their recorded
timestamps, at the rates the nuScenes specification states — six cameras at 12 Hz, `LIDAR_TOP` at
20 Hz, five radars at 13 Hz, keyframes at 2 Hz — which M1 has already checked against the data
rather than taken on trust. Different rates mean arrival order and event-time order genuinely
disagree, so watermarks and late-arrival handling have something to do. Kafka carries envelopes —
`uri`, `checksum`, `schema_version`, `start/end_ts` — never sensor payloads; the schema and the
target table are M1's, unchanged. Duplicate events are injected and consumers are killed mid-stream
to make the sink defend the handover between Kafka offset and Iceberg commit. The claim reduces to
one line: **the streaming table equals the batch table, row for row, on the same scenes**, and
Trino runs that diff. Without it the milestone would show only that Kafka was installed.

## Measurement rules

Fixed before any measurement, because these are what make a later number mean something.

- **`completeness` on undamaged input is 100%.** Anything less is data loss, not an achievement.
  Fault-detection recall is a different number on a different line; the two are never merged, and
  recall is reported per fault type rather than averaged.
- **`freshness` does not exist until M5.** A static dataset has none. Once events flow it is
  defined as *snapshot-queryable time − emit time* and labelled a replayed event-time workload,
  with the replay speed stated. Measured against the original sensor timestamp it would only
  measure how old nuScenes is, and presented as production freshness it would be a false claim.
- **Exactly-once is a comparison, not a configuration.** It is claimed only as row-level equality
  between the streaming table and the batch table over the same scenes, with duplicate injection
  and forced consumer restarts inside the run being compared. Quoting a delivery-guarantee setting
  out of a config file is not evidence that the setting held.
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
