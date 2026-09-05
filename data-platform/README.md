# data-platform

**A lakehouse and data-quality platform for multimodal autonomous-driving sensor logs.**

The question this answers: **as sensor logs accumulate, can a training dataset be produced from
them reproducibly?** Reproducibly means three things at once — the same snapshot yields the same
dataset, corruption and gaps cannot pass silently, and a failing slice can be found and promoted
into the next dataset version. Close all three and it is a platform. Close only the first and it is
an ETL demo.

## Architecture

```
  source                  nuScenes — JSON metadata + camera / LiDAR / radar blobs, local disk
                          │
                          ├──────────────────────────────┐
                          │                              │
  ingest               M1 batch path                  M5 replay path
                          PySpark normalise              one producer per sensor stream
                          one scene commit               stated + seeded arrival model
                          quality gate ─▶ quarantine     Kafka — envelopes only
                          │                              streaming sink
                          │                              Kafka offset ↔ Iceberg commit
                          │                              │
                          ├──────────────────────────────┘
                          │
  store                   Iceberg — one schema, one set of tables
                          scene · sample · sample_data · blob_ref
                          ├──────────────────────────────┐
                          │                              │
                          release pin                    blob store
                          snapshot ids · blob hashes     content-addressed · immutable
                          transform + policy version     checksum · orphan sweep
                          │
                          ├────────────────┬────────────────┬──────────────────┐
  consumers            M1 Trino         M2 dataloader    M3 OpenCLIP        M4 corner cases
                          SQL, time        PyTorch,         embeddings,        skew · blackout ·
                          travel, and      what reaches     text / image →     sparse LiDAR →
                          the M5 diff      training         time ranges        new dataset
                                                                               version → M1
```

Two ingest paths, one schema, one set of tables. **M2** wraps the batch path in Airflow — backfill
→ gate → publish, repaired idempotently — and proves one deployment path on S3. **M5**'s claim is
the join at the centre: the released streaming table equals the released batch table over the same
scenes. What that check does and does not establish is stated in M5 rather than left implied.

## Skillset

| Technology | What it does here | Milestone | Postings † |
|---|---|---|---:|
| PySpark | nuScenes → canonical scene commit; the batch path the replay path is later checked against | M1 | 18/20 |
| Python | the language everything above the SQL layer is written in | M1 | 16/20 |
| Iceberg | snapshot atomicity, schema evolution and time travel under both ingest paths | M1 | 4/20 |
| SQL / data modelling | the sensor · sample · blob · provenance · release model itself | M1 | 10/20 |
| Trino | SQL and time travel over the canonical tables, and the row-level diff in M5 | M1, M5 | 5/20 |
| Airflow | backfill → quality gate → publish, with an injected failure repaired idempotently | M2 | 18/20 |
| Terraform · S3 · IRSA | one deployment and permission path proved off the laptop, not performance | M2 | — |
| Kubernetes | already held; carries the single small cluster run in M2 | M2 | 10/20 |
| PyTorch dataloader | what actually reaches training, measured rather than assumed | M2 | — |
| OpenCLIP + FAISS | segment embeddings; text and image queries returning time ranges | M3 | — |
| Kafka | replayed event-time ingest of the same scenes into the same tables | M5 | 14/20 |
| MongoDB | **not used** — one posting in twenty, and there as an example rather than a requirement | — | 1/20 |

† Frequency across 20 Korean data-platform postings collected in September 2026. The list, the
collection date and the coding rules live outside this repository, per the documentation note in
`CONTRIBUTING.md`, so **no reader can recompute this column** — treat it as an unverified personal
survey that explains how the shortlist was drawn, not as evidence for it. Differences of one or two
postings are inside its noise and nothing here rests on them.

Deliberately excluded: Flink (9/20 is not low, but learning a fourth engine while Spark, Kafka and
Airflow are all at zero lines means none of them gets deep), managed warehouses (they take `$/TB`
out of our hands), and dbt (this is a platform, not analytics engineering).

Iceberg rather than Delta — and the honest reason is not the 2-to-1 count in a survey nobody can
recompute, since one posting either way flips that. It is that one format has to be carried the
whole way through for the M5 comparison to mean anything, and Iceberg is the one whose catalog and
Trino path are already reachable here. A head-to-head benchmark was planned and has been dropped:
the criterion that would have flipped the choice was never written down first, so the result could
not have changed anything. If the roles being targeted genuinely require Delta, the direct evidence
is writing the same scene commit against Delta once — a compatibility exercise, not a contest, and
smaller than a milestone.

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

**What a release pins is the other one.** "One scene commit" and "dataset release" are the two
phrases this project promises reproducibility with, and neither means anything until its boundary is
written down. A release names the snapshot id of every table it spans, the content hash of every
blob it references, and the version of the transform and the quality policy that produced it;
approval publishes those together or publishes nothing. Leave that undefined and a quality gate can
pass, a referenced blob can be deleted before publication, and a dataloader can read a different
snapshot per sensor — while the row-level check in M5 still comes back equal and the training data
still is not reproducible.

## Milestones

Ordered by dependency where one exists and by priority where it does not, and the two are labelled
rather than blurred. Throughput measured before reconciliation is proven measures nothing, and
embeddings over a corpus that cannot be searched cannot be evaluated — those are dependencies. M5's
only hard dependency is M1. It is last by priority: the reference has to exist before the comparison
is worth running, and it is the piece the rest of the platform does not need.

The streaming layer being last is not because the dataset is static. Replaying nuScenes at recorded
timestamps is a genuine event-time workload once each sensor stream is its own producer, because the
consumer's merged view is then out of order by construction. What it is not is free evidence:
nuScenes records sensor timestamps, not arrival times, so the arrival model is synthetic and M5
states its partitioning, keys, delay distribution and seed and runs an order-preserving control
beside it. Claiming that differing sensor rates by themselves produce out-of-order arrival would be
false — merge-sort every sensor into one partition and the order never inverts.

What M5 buys is a reference to check against. **The batch pipeline is the reference for the
streaming one** — with the caveat that both share the normalisation code, so agreement between them
cannot detect a bug they hold in common. M1 therefore produces the independent half, and agreement
with the batch table is the second check rather than the first.

**M1 — canonical lakehouse.** PySpark normalises `scene`/`sample`/`sample_data` into one scene
commit; the blob contract and the release pin above are established; clock skew, missing sensors,
duplicates and out-of-order frames are detected; failures land in a quarantine table rather than in
the release. Two artefacts are built here for later use: per-sensor event counts derived from the
source metadata *without* going through the transform, and a hand-checked fixture of a few scenes.
A local single-node Trino reads two fixed snapshots and returns their expected rows — an
interoperability and time-travel check, not a second implementation; if it starts to need catalog or
storage configuration beyond that, it moves to M2.
*Excluded here: throughput headlines, S3, clusters, streaming.*

**M2 — reproducible training supply.** Airflow runs backfill → quality gate → publish, with an
injected task failure repaired idempotently and partial backfill by scene, sensor, date or quality
slice. S3, IAM/IRSA and `terraform plan` in CI, plus one small cluster run that proves deployment
and permissions — not performance. A PyTorch dataloader measures what actually reaches training,
and publication latency — input ready to release published — is first measurable here.

**M3 — temporal search and evaluation.** OpenCLIP produces segment embeddings keyed by
`scene_id · camera · start_ts · end_ts · embedding_version`; text and image queries return time
ranges, not files; Recall@K and latency say whether the search is right rather than whether it
exists. Query set and ground truth are fixed *before* the embeddings are built.

**M4 — failure mining and promotion.** Three to five corner-case queries (excessive clock skew,
camera blackout, sparse LiDAR, hard braking, annotation disagreement) produce slices that get
promoted into an evaluation set and a new dataset version, closing the loop back to M1. Most of
these are SQL quality queries and do not depend on M3; the ones that need embeddings say so. An
optional MCAP/rosbag2 adapter covers the interchange concept that US robotics postings ask for by
description rather than by product name.

**M5 — streaming ingest and the equivalence check.** Kafka replays the same scenes at their
recorded timestamps, one producer per sensor stream, at the rates the nuScenes specification states
— six cameras at 12 Hz, `LIDAR_TOP` at 20 Hz, five radars at 13 Hz, keyframes at 2 Hz. Those are
capture rates, not a promise that every interval is exactly `1/Hz`, and M1 has already checked them
against the data. The arrival model is stated and seeded, and an order-preserving control run says
how much of the outcome the disorder is responsible for. Kafka carries envelopes — `uri`,
`checksum`, `schema_version`, `start/end_ts` — never sensor payloads; the schema and the target
table are M1's, unchanged. Duplicate events are injected and consumers are killed mid-stream to make
the sink defend the handover between Kafka offset and Iceberg commit.

The claim is one line and deliberately narrow: **the released streaming table equals the released
batch table over the same scenes, row for row and duplicate for duplicate.** Set comparison would
not do — it calls the batch table's `[a]` equal to a streaming table's `[a, a]`. The comparison is
fixed before the run: which columns are in scope (`ingested_at` and run identifiers are not), which
input range, what happens to an event arriving past the watermark on one path but not the other, and
what condition means the stream has finished emitting. Trino runs the diff.

What the check does *not* establish is published with it. Equality of the released tables is
evidence about the final state, not about every state the pipeline passed through: a sink that
commits to Iceberg, dies before recording its checkpoint, re-appends on restart and deduplicates at
release time yields an equal final table while any reader of the intermediate snapshot saw
duplicates. The guarantee claimed here is therefore release-level equivalence under injected
duplicates and forced restarts, scoped to the local configuration below. Exactly-once as a property
of the delivery path is a larger claim — it needs the intermediate states and the external effects —
and this milestone does not make it.

## Measurement rules

Fixed before any measurement, because these are what make a later number mean something.

- **`completeness` on undamaged input is 100%.** Anything less is data loss, not an achievement.
  Fault-detection recall is a different number on a different line; the two are never merged, and
  recall is reported per fault type rather than averaged.
- **Agreement between two paths that share code is the second check, not the first.** If the batch
  and streaming paths share a normalisation that drops a radar channel, they agree on the wrong
  answer. The first check is a value derived without the transform — the M1 per-sensor counts and
  the hand-checked fixture. `completeness` takes its denominator from those, never from the output.
- **Release equivalence is a comparison; exactly-once is a larger claim.** Equivalence is reported
  as row-level equality with duplicate multiplicity preserved, over a column set, input range and
  late-arrival policy fixed before the run, with duplicate injection and forced consumer restarts
  inside it. It says the released tables agree. It does not say every event took effect exactly
  once, and quoting a delivery-guarantee setting out of a config file says less than either.
- **`freshness` is a stream metric and waits for M5.** Publication latency is a different number and
  is measurable from M2. Stream freshness is *snapshot-queryable time − emit time*, labelled a
  replayed event-time workload with its replay speed stated, and reported next to producer lag,
  since `queryable − emit` hides a producer that emitted late against its own schedule. Measured
  against the original sensor timestamp it would only measure how old nuScenes is; presented as
  production freshness it would be a false claim.
- **`Recall@K` needs its ground truth first.** Choosing the correct answers after seeing the search
  results is not evaluation, it is justification.
- **`$/TB` needs an honest denominator.** Structured bytes, blob bytes read for hashing, bytes
  written and total corpus referenced are counted separately. Reading 1 GB of JSON while
  *referencing* 350 GB of blobs is not 0.35 TB of processing.
- **A gate that cannot be shown to fail has not been shown to run.** Disabling one must turn its
  test red. A check that cannot tell "did not fire" from "passed" is worse than none, because it
  manufactures confidence.
- **Estimates are labelled as estimates.** A number off a stopwatch, a number derived by arithmetic
  and a number off a vendor page are marked as which. A result is also scoped to the configuration
  it ran in; a local one does not describe the S3 path until it has been run there.
- **Negative results stay.** A run that fails its pre-registered checks is published as one.

## Where this runs

Measured on the development machine: 32 cores, 59 GiB RAM, 1.2 TB free, NVMe at 18 GB/s read,
sha256 at 0.5 GiB/s per core. Those are stopwatch numbers about the hardware. Everything else in
this section is arithmetic from them, and is an estimate.

M1, M4 and M5 run entirely locally at no cloud cost. M2 needs a small amount of AWS to prove the IaC
and deployment path; M3 should embed on CPU in a couple of hours, or in about ten minutes on a
rented GPU. Source data stays local and only a slice is uploaded — storing the full corpus in object
storage is estimated at roughly a third of the monthly budget, every month, which is a design
decision rather than a saving.

M5 runs against local storage and a local catalog, so its conclusions are about that configuration.
Extending them to the S3 path built in M2 would mean running the same duplicate injection and
restart scenarios there. Until that happens, the streaming result is not a statement about the cloud
deployment.

## Status

Pre-registration stage. Nothing in the system has been measured, because nothing in it has been
built — the hardware figures above are the exception and are marked as such. When system numbers
exist they appear here with the baseline, the workload, the hardware, the commit hash and the
repetition count, or they do not appear at all.
