# data-platform

**A lakehouse and data-quality platform for multimodal autonomous-driving sensor logs.**

The question this answers: **as sensor logs accumulate, can a training dataset be produced from
them reproducibly?** Reproducibly means three things at once — the same snapshot yields the same
dataset, corruption and gaps cannot pass silently, and a failing slice can be found and promoted
into the next dataset version. Close all three and it is a platform. Close only the first and it is
an ETL demo.

## How this is judged

Every number a platform reports about itself is a number it also defined. That is the failure this
project is built against, so three of its results are computed by someone else.

| Instrument | What it scores | Where |
|---|---|---|
| **MLPerf Storage v3.0** | how fast the storage path feeds training | M2 |
| **Croissant 1.1** | whether release metadata is a standard, not an invention | M2 |
| **RefAV · EvalAI 2469** | whether scenario retrieval is right, not merely self-consistent | M5 |

MLPerf Storage is the primary one, because it measures this job. It emulates accelerators with
sleep timers and reads real data through PyTorch at the intensity of a real training run, so it
needs no GPU: the arithmetic is skipped and the whole path from storage through client memory is
not. Two of its workloads are this corpus almost exactly — `RetinaNet` is millions of small random
reads, `3D U-Net` is large sequential reads — and its **open division** admits alternative data
formats and access methods, which is where Iceberg-plus-external-blobs meets Lance on a workload
neither of them chose.

Running the suite and reporting against the published v3.0 results is what M2 commits to. A
reviewed submission to MLCommons is a separate process with its own cycle and is a stretch goal,
recorded here as one so that the two are never conflated later.

## Architecture

```
  source            Argoverse 2 Sensor · Map Change (TbV) · nuScenes         published 3D tracks
                    raw logs + blobs, local NVMe                             external input
                        │                              │                             │
                        ├──────────────────────────────┤                             │
                        │                              │                             │
  ingest           M1 batch path                  M4 event backbone                  │
                      PySpark normalise              topics partitioned by log_id    │
                      one log commit                   blob.arrived   ~11 M events   │
                      sensor-level gates               sweep.quality  ~0.7 M         │
                      │        └─▶ quarantine          release.cdc    Debezium       │
                      │                                replay.sensor  event-time     │
                      │                              Schema Registry · Avro · compat │
                      │                              Kafka Connect ─▶ Iceberg sink   │
                      │                              DLQ ─▶ quarantine               │
                      │                              │                               │
                      ├──────────────────────────────┘                               │
                      │  equivalence — batch table == stream table, row for row      │
  store           Iceberg — one schema, one set of tables  ◀─────────────────────────┘
                  log · sample · sensor_data · blob_ref · sweep_stat · track · event
                      ├──────────────────────────────┬───────────────────────────┐
                      │                              │                           │
                  release pin                    blob store                  Lance table
                  snapshot ids · blob hashes     content-addressed           embeddings and
                  transform + policy version     checksum · orphan sweep     random access
                  Croissant 1.1 · PROV-O                                          │
                      │                                                           │
  measure           ┌─┴──────────────────────┐                                    │
                 M2 MLPerf Storage v3.0       M2 format comparison ◀──────────────┘
                    RetinaNet · 3D U-Net         open division: Iceberg + blobs
                    VectorDB · checkpoint        against Lance, same workload
                      │                              │
  prove             └──────────────┬───────────────┘
                                   │
                 M5 TbV map change            M5 RefAV → EvalAI 2469
                    LiDAR × camera × HD map      HOTA-Temporal
                    labelled discrepancies       baseline 50.1 · winner 53.4
                      │                              │
  serve          M3 PyTorch dataloader        M5 corner case → new dataset version → back to M1
```

```
  developer          GitHub                             AWS  ap-northeast-2
  ─────────          ───────────────────────────        ──────────────────────────────────────

  feat/*              Actions · CI   (on PR)             GitHub OIDC provider
    │                 ├ ruff · pytest                      │   no long-lived access keys
    ▼                 ├ data contract tests                ▼
   dev ──────────────▶├ quality-rule tests               IAM role  gha-plan   read + plan
    │                 ├ terraform fmt · validate         IAM role  gha-apply  main branch only
    │                 └ terraform plan → PR comment        │
    │                        │                             ▼
    ▼                        │                           S3    raw-slice · warehouse · artifacts
   main ─────────────▶ Actions · CD  (on merge)          Glue or Nessie   Iceberg REST catalog
                       ├ build images → ECR              ECR   spark · airflow · trino
                       ├ terraform apply                   │
                       └ bump image tag in deploy/          ▼
                              │                           EKS   one small node group
                       deploy/  ← git is desired state      ├ Spark on K8s  ─┐
                              │                             ├ Airflow        ├ IRSA → S3
                       Argo CD ─────── sync ───────────────▶└ Trino         ─┘
                              ▲                                 no node keys, no secrets in env
                              └ drift detection
                                what runs == what is in git

  local only          Kafka · MinIO · single-node Trino · the whole corpus on NVMe
  on AWS              one slice · terraform plan on every PR · apply once, in M3
```

The branch flow is the permission boundary: a pull request can only assume a role that plans, and
apply opens on `main` alone. Argo CD is here for drift rather than for deployment — whether what is
running is what is in git is a question a system should answer, not a person.

## The substrate

Argoverse 2 is open autonomous-driving data and HD maps from six U.S. cities, released by Argo AI
under CC BY-NC-SA 4.0. One name covers four datasets, and they are taken in the order of the
*problem* each one adds rather than the volume it brings.

| Dataset | On disk | What it adds that the others do not | Phase |
|---|---:|---|---|
| **Sensor** | ~1 TB | the multimodal blob contract; 1,000 logs, 9 cameras at 20 fps, two 32-beam LiDARs at 10 Hz merged into one sweep, 30-class cuboids at 10 Hz | **1** |
| **Map Change (TbV)** | 922 GB | **reference drift with ground truth** — 1,043 logs, 559,440 sweeps, 7,837,614 images, and labelled HD-map discrepancies | **2** |
| Motion Forecasting | 58 GB | row cardinality without blob volume — 250,000 scenarios | 3 |
| Lidar | not published | volume and unlabelled data — 20,000 thirty-second sequences. Derived from TbV's ratio at roughly 1.5 TB, which is an estimate and is measured before it is committed to | if needed |

Tabular data is Apache Feather throughout: sweeps, poses, calibration, annotations. A sweep carries
`x`, `y`, `z`, `intensity`, `laser_number` and `offset_ns`, and the last two are what make
sensor-level quality checkable rather than assertable.

**Map Change is the one this project needs most and the one portfolios ignore.** Its paper is
*Trust, but Verify: Cross-Modality Fusion for HD Map Change Detection* — the map is wrong on
purpose, and finding out requires putting LiDAR, camera and map into the same frame. For a platform
whose claim is that corruption cannot pass silently, that is a quality problem with an answer key,
which is rarer than a dataset.

### What nuScenes is for

It is not a second pile of data. It is the falsification test for M1's central claim.

M1 says it builds a *normalisation contract*. One dataset cannot distinguish a contract from a
parser. nuScenes differs on every axis that matters, so passing both through one schema and one
gate is the evidence:

| | Argoverse 2 Sensor | nuScenes |
|---|---|---|
| cameras | 9 at 20 fps | 6 at 12 Hz |
| LiDAR | 2 × 32-beam at 10 Hz, merged | 1 × 32-beam at 20 Hz |
| radar | none | 5 at 13 Hz |
| annotation | 30 classes at 10 Hz | 23 classes at 2 Hz keyframes |
| tabular format | Apache Feather | JSON |
| log duration | 15 s | 20 s |

An MCAP/rosbag2 or LeRobot adapter is the same socket, used a third time, and is what connects this
to the robotics postings that ask for pipelines standardised across teleoperation and simulation
data rather than across driving logs.

## Why these datasets and not others

Chosen from the frequency of requirements across 20 Korean data-platform postings collected in
September 2026. The list, the collection date and the coding rules live outside this repository per
the documentation note in `CONTRIBUTING.md`, so **no reader can recompute the column** — treat it as
an unverified personal survey explaining how the shortlist was drawn, not as evidence for it.
Differences of one or two postings sit inside its noise and nothing here rests on them.

| Technology | What it does here | Milestone | Postings |
|---|---|---|---:|
| PySpark | Feather and JSON → one canonical log commit; the batch path the replay path is checked against | M1 | 18/20 |
| Python | everything above the SQL layer | M1 | 16/20 |
| Iceberg | snapshot atomicity, schema evolution, time travel under both ingest paths | M1 | 4/20 |
| SQL / data modelling | the sensor · sample · blob · sweep · track · release model itself | M1 | 10/20 |
| Trino | SQL and time travel over the canonical tables; the predicate side of scenario mining; the row-level diff in M4 | M1, M4, M5 | 5/20 |
| Lance | embeddings and random access, measured against Iceberg-plus-blobs on the MLPerf workload | M2 | — |
| Airflow | backfill → quality gate → publish, with an injected failure repaired idempotently | M3 | 18/20 |
| Terraform · S3 · IRSA | one deployment and permission path proved off the laptop — not performance | M3 | — |
| GitHub Actions | contract and quality-rule tests, plan on every PR, apply only from `main` | M3 | — |
| Argo CD | drift — whether what runs is what is in git | M3 | — |
| Kubernetes | already held; carries the single small cluster run in M3 | M3 | 10/20 |
| PyTorch dataloader | what actually reaches training, measured rather than assumed | M3 | — |
| Prometheus · Grafana | gate evaluation counters, data SLIs and table health as time series; red when a gate is disabled | M1, M3 | — |
| Loki · OpenTelemetry | structured logs keyed by release, collected through the standard instrumentation path rather than a bespoke one | M3 | — |
| Kafka | the ingest control plane — four topics partitioned by `log_id`, consumer groups, offsets as resumability, DLQ into quarantine | M4 | 14/20 |
| Schema Registry | Avro envelope subjects with a stated compatibility policy, tested against the Iceberg table schema | M4 | — |
| Kafka Connect | the Iceberg sink, and the connector/offset/snapshot operation the postings ask about by name | M4 | — |
| Debezium | change data capture from the release-state Postgres, so release history is a table rather than a log | M4 | — |
| OpenCLIP | segment embeddings; the vector side of scenario mining | M5 | — |
| MongoDB | **not used** — one posting in twenty, and there as an example rather than a requirement | — | 1/20 |

Deliberately excluded: Flink (9/20 is not low, but learning a fourth engine while Spark, Kafka and
Airflow are all at zero lines means none of them gets deep), managed warehouses (they take `$/TB`
out of our hands), and dbt (this is a platform, not analytics engineering).

That exclusion is about breadth, and it is why Kafka Connect, Schema Registry and Debezium are *not*
excluded by it. They are Kafka's own components rather than a fourth engine, and they are the
components the postings name — connectors, snapshots, offsets, replica lag, subject compatibility.
Adding them deepens the one streaming system instead of starting a second.

Iceberg rather than Delta, and the honest reason is not a 2-to-1 count in a survey nobody can
recompute. It is that one format has to be carried the whole way through for the M4 comparison to
mean anything, and Iceberg is the one whose catalog and Trino path are already reachable here. The
format question does not disappear — it moves to M2, where Lance meets it on a workload defined by
MLCommons rather than by the person who picked the winner.

## The boundary

Getting this wrong in either direction is expensive. Reimplementing snapshots is waste; assuming
the table format covers blob integrity or sensor health is a correctness bug.

| Concern | The table format provides | This project implements |
|---|---|---|
| Atomicity | snapshot-level atomic commit | grouping N sensors of one log into a single domain commit |
| Versioning | snapshot history, time travel | dataset *release* semantics — approval, deprecation, training eligibility |
| Schema | schema evolution | AV2 and nuScenes normalisation, per-sensor field model, compatibility policy |
| Slicing | SQL predicates over rows | a slice preserving a consistent sensor set, calibration version, missing-frame policy, blob existence |
| Catalog | table identifier → metadata location | the domain catalog: sensor, sample, blob, provenance, release state |
| Quality | — | referential integrity, blob existence/size/checksum, and the sensor-level checks below |
| Orchestration | — | retry, idempotency, backfill, repair, gating, alerting |
| Blob lifecycle | tracks its own data files | external JPEG/Feather upload, checksum, orphan detection, retention |

**The blob row is the load-bearing decision.** Sensor payloads do not go into a `binary` column.
Metadata rows carry `blob_uri`, `byte_size` and `checksum`; payloads live in object storage. Said
plainly: **a snapshot does not protect the contents of the object `blob_uri` points at.** Overwrite
or delete that object and the table is silently wrong. Content-addressed keys, immutable object
policy, checksum verification and orphan reconciliation are ours to build.

**What a release pins is the other one.** A release names the snapshot id of every table it spans,
the content hash of every blob it references, and the version of the transform and the quality
policy that produced it; approval publishes those together or publishes nothing. Leave that
undefined and a gate can pass, a referenced blob can be deleted before publication, and a dataloader
can read a different snapshot per sensor — while a row-level check still comes back equal and the
training data still is not reproducible. The pin is emitted as **Croissant 1.1**, whose PROV-O
provenance model already says what this project would otherwise have invented.

### Sensor-level quality is not metadata quality

Referential integrity and checksums say the rows point at objects that exist. They say nothing about
whether a LiDAR had a dead laser that afternoon. These checks live in M1 and each writes a row to
`sweep_stat` rather than a boolean:

- **per-laser return counts** — `laser_number` spans 64 lasers across the two units; a laser whose
  return count collapses against its own history is degraded hardware, not a bad log.
- **sweep point count and range distribution** — sparse sweeps, rain and occlusion look different
  from each other and from a dropped sensor.
- **intra-sweep timing** — `offset_ns` must span one 10 Hz revolution and advance monotonically per
  laser; violations are a sync fault the row count cannot see.
- **cross-sensor skew** — every sweep against the nearest frame of each of the nine cameras, kept as
  a distribution per camera rather than a single mean.
- **ego-pose continuity** — gaps and jumps in the 6-DOF pose stream break motion compensation
  downstream of anything that consumes them.
- **calibration residual** — LiDAR ground returns against the HD map's 30 cm ground-height raster.
  This is the check TbV turns from an assertion into a measurement, because there the answer is
  known.

### Observability is how the gate rule is enforced at runtime

"A gate that cannot be shown to fail has not been shown to run" is a CI rule, and CI only sees the
gates it was asked about. At runtime the same failure comes back wearing different clothes: the gate
did not fire, nothing went red, and the release shipped. So every gate emits four counters rather
than a boolean, and the fourth is the one that matters:

```
gate_evaluated_total{gate, dataset, release}
gate_passed_total
gate_failed_total
gate_skipped_total      ← alert on this, and on evaluated staying flat while ingest advances
```

Most systems count passes and failures. Counting *evaluations* and *skips* is what lets a dashboard
distinguish "the data was clean" from "nobody looked", which is the same distinction the whole
project is built on.

Data signals sit in the same place as machine signals, because on this platform they are the
interesting ones: freshness p50/p95 beside producer lag, completeness per release, quarantine depth
by reason, the `sweep_stat` distributions, blob orphan and checksum-mismatch counts, consumer lag
per partition, DLQ depth, schema-compatibility outcomes, and the Iceberg table-health numbers —
snapshot count, small-file count, orphan files — that a lakehouse quietly rots without.

**Prometheus, Loki, Grafana and Alertmanager, instrumented through OpenTelemetry. Not ELK**, for two
reasons. The signal here is time series and structured events keyed by `release_id`, `log_id` and
`snapshot_id`, not full-text search over unstructured logs, which is Elasticsearch's strength and
its cost. And more specifically: **M2 measures the storage path, and Elasticsearch would compete for
the disk being measured.** Loki indexes labels rather than log content and is roughly an order of
magnitude lighter for this. Every benchmark run records what else was running on the machine.

Stderr is never discarded. A failure whose cause was routed to `/dev/null` cannot be diagnosed, and
a check that silently produced nothing is indistinguishable from one that passed — the same bug in a
different layer.

**The dashboard is not a deliverable.** A screenshot of a green board proves nothing. What counts is
a board that goes red when a gate is disabled and green again when it is restored, which is the same
evidence the CI rule demands, taken at runtime. The instrumentation contract is defined in M1 where
the gates are born, the stack is stood up in M3 beside Airflow, and M4 adds the streaming signals.

### The event backbone is not a demonstration

M1's batch path is deliberately free of Kafka, because it is the reference the streaming path is
later checked against. That is the *only* reason Kafka is absent from M1, and it says nothing about
how much of the platform runs on events. Four topics do, each partitioned by `log_id` so that
per-log ordering is a property of the layout rather than a hope:

| Topic | Volume | The question it exists to answer |
|---|---:|---|
| `blob.arrived` | ~11 M | when eleven million objects land, how does the platform know what to process, once each, and resume from where it stopped? |
| `sweep.quality` | ~0.7 M | how does a failed sensor check become a new dataset version without a human polling a table? |
| `release.cdc` | low | is the release state machine's history queryable as data, or only as application logs? |
| `replay.sensor` | ~0.7 M | does event time hold up when arrival order and event order disagree? |

Consumer groups supply the parallelism, offsets supply the resumability, and the dead-letter queue
lands in the quarantine table that already exists — a poison envelope and a failed quality gate
belong in the same place, and putting them there is the argument for having one.

**Two schema systems now have to agree, and that is the interesting part.** Envelope subjects live
in the Schema Registry as Avro with a stated compatibility policy; the target tables evolve under
Iceberg's own rules. The gate is that a wire-schema change the table cannot accept fails CI before
it reaches a topic — and like every gate here, it has to be shown to fail before it is believed.
Nothing about a `binary` column changes: envelopes carry `uri`, `checksum`, `schema_version` and
`start/end_ts`, never sensor payloads.

Debezium supplies `release.cdc` from the Postgres that Airflow needs anyway, which is what turns
the draft → gated → approved → deprecated state machine from application state into a table with a
history. Kafka Connect runs the Iceberg sink. Neither is a fourth engine; both are the components
the postings name.

## Milestones

Ordered by dependency where one exists and by priority where it does not, and the two are labelled
rather than blurred. M2 and M3 depend on M1 and on nothing else. M4 depends on M1, because the batch
table is what its equivalence check compares against. M5 depends on M4, because its promotion loop
consumes `sweep.quality`, and on M1, because the checks that fill that topic live there.

**M1 — canonical lakehouse and sensor-level quality.** PySpark normalises Argoverse 2 Sensor into
one log commit; the blob contract and the release pin are established; the sensor-level checks above
run and land in `sweep_stat`; failures land in a quarantine table rather than in the release. Two
artefacts are built here for later use: per-sensor event counts derived from the source metadata
*without* going through the transform, and a hand-checked fixture of a few logs. A local
single-node Trino reads two fixed snapshots and returns their expected rows — an interoperability
and time-travel check, not a second implementation.
*Excluded here: throughput headlines, S3, clusters, streaming.*

**M2 — the measured data path.** MLPerf Storage v3.0 runs against this corpus on this hardware:
`RetinaNet` for the small-random-read path, `3D U-Net` for the large-sequential one, `VectorDB` for
the embedding index, and checkpointing for the write path. The open division carries the format
comparison — Iceberg with external blobs against Lance — on a workload neither of them designed.
Every release emits Croissant 1.1 metadata with PROV-O provenance and passes the validator.
*A reviewed MLCommons submission is a stretch goal and is recorded as one.*

**M3 — reproducible supply.** Airflow runs backfill → quality gate → publish, with an injected task
failure repaired idempotently and partial backfill by log, sensor, date or quality slice. Terraform,
S3, IAM/IRSA and `terraform plan` in CI, Argo CD reconciling `deploy/`, and one small cluster run
that proves deployment and permissions — not performance. A PyTorch dataloader measures what
actually reaches training; publication latency is first measurable here.

**M4 — the event backbone and its equivalence check.** The four topics above go up on a local
cluster: `blob.arrived` drives checksum verification and blob registration for the whole corpus,
`sweep.quality` carries every sensor-check result, `release.cdc` arrives by Debezium, and Kafka
Connect sinks into the same Iceberg tables M1 writes. Envelope subjects are registered with a
compatibility policy, and a wire-schema change the table cannot accept fails CI. Consumer groups,
partition assignment, offset management and DLQ routing are operated rather than described, because
that is the difference the postings are asking about.

Then the same logs are replayed on `replay.sensor` at their recorded timestamps, one producer per
sensor stream. Rates are Argoverse 2's — nine cameras at 20 fps, the merged LiDAR sweep at 10 Hz —
which are capture rates rather than a promise that every interval is exactly `1/Hz`, and which M1
has already checked against the data. The arrival model is stated and seeded, and an
order-preserving control run says how much of the outcome the disorder is responsible for. Duplicate
events are injected and consumers are killed mid-stream to make the sink defend the handover between
Kafka offset and Iceberg commit.

The claim is one line and deliberately narrow: **the released streaming table equals the released
batch table over the same logs, row for row and duplicate for duplicate.** Set comparison would not
do — it calls the batch table's `[a]` equal to a streaming table's `[a, a]`. The comparison is fixed
before the run: which columns are in scope (`ingested_at` and run identifiers are not), which input
range, what happens to an event arriving past the watermark on one path but not the other, and what
condition means the stream has finished emitting. Trino runs the diff.

What the check does *not* establish is published with it. Equality of the released tables is
evidence about the final state, not about every state the pipeline passed through: a sink that
commits to Iceberg, dies before recording its checkpoint, re-appends on restart and deduplicates at
release time yields an equal final table while any reader of the intermediate snapshot saw
duplicates. The guarantee claimed here is release-level equivalence under injected duplicates and
forced restarts, scoped to the local configuration. Exactly-once as a property of the delivery path
is a larger claim — it needs the intermediate states and the external effects — and this milestone
does not make it.

**M5 — external ground truth.** Three outside opinions on three different claims.
*Map change:* TbV's labelled HD-map discrepancies score the calibration-residual check against an
answer key, which is the only way the sensor-level gates become a measurement rather than an
assertion. *Scenario mining:* RefAV's 10,000 natural-language queries over Argoverse 2 logs,
answered twice — once by compiling queries to predicates over the `track` table in Trino, once by
OpenCLIP embeddings over Lance — with both scored by HOTA-Temporal on EvalAI challenge 2469 rather
than by a query set of our own. *Contract:* nuScenes through the same schema and the same gate,
because one dataset cannot tell a contract from a parser. Failing slices from all three arrive on
`sweep.quality` and are promoted into an evaluation set and a new dataset version, closing the loop
back to M1. *This is the heaviest milestone and splits if it has to.*

## Measurement rules

Fixed before any measurement, because these are what make a later number mean something.

- **`completeness` on undamaged input is 100%.** Anything less is data loss, not an achievement.
  Fault-detection recall is a different number on a different line; the two are never merged, and
  recall is reported per fault type rather than averaged.
- **Agreement between two paths that share code is the second check, not the first.** If the batch
  and streaming paths share a normalisation that drops a laser, they agree on the wrong answer. The
  first check is a value derived without the transform — the M1 per-sensor counts and the
  hand-checked fixture. `completeness` takes its denominator from those, never from the output.
- **A number computed here is labelled as such.** The three external instruments are named with
  their version, their division and the date they were run, and their results are never averaged
  with internal ones. An MLPerf Storage result run locally is not a submitted result and says so.
- **Release equivalence is a comparison; exactly-once is a larger claim.** Equivalence is reported
  as row-level equality with duplicate multiplicity preserved, over a column set, input range and
  late-arrival policy fixed before the run, with duplicate injection and forced consumer restarts
  inside it. It says the released tables agree. It does not say every event took effect exactly
  once, and quoting a delivery-guarantee setting out of a config file says less than either.
- **`freshness` is a stream metric and waits for M4.** Publication latency is a different number and
  is measurable from M3. Stream freshness is *snapshot-queryable time − emit time*, labelled a
  replayed event-time workload with its replay speed stated, and reported next to producer lag,
  since `queryable − emit` hides a producer that emitted late against its own schedule.
- **Retrieval accuracy is EvalAI's number, not ours.** HOTA-Temporal, HOTA-Track, Timestamp BA and
  Log BA are reported as returned, beside the published baseline of 50.1 and challenge best of 53.4.
  A query set of our own scoring itself would be justification rather than evaluation.
- **`$/TB` needs an honest denominator.** Structured bytes, blob bytes read for hashing, bytes
  written and total corpus referenced are counted separately. Reading 1 GB of Feather while
  *referencing* 900 GB of blobs is not 0.9 TB of processing.
- **A gate that cannot be shown to fail has not been shown to run.** Disabling one must turn its
  test red. A check that cannot tell "did not fire" from "passed" is worse than none, because it
  manufactures confidence. The schema-compatibility gate is held to this too: a wire schema the
  table cannot accept is committed on a branch, and CI going green is the bug.
- **Estimates are labelled as estimates.** A number off a stopwatch, a number derived by arithmetic
  and a number off a vendor page are marked as which. A result is also scoped to the configuration
  it ran in; a local one does not describe the S3 path until it has been run there.
- **A benchmark run records what else was running.** The observability stack shares this machine
  with the thing it observes, and M2 measures a disk that Prometheus and Loki also write to. Every
  storage result carries the co-tenants of its run, and a result taken with an unrecorded background
  load is a result that cannot be repeated.
- **Negative results stay.** A run that fails its pre-registered checks is published as one.

## Where this runs

Measured on the development machine: 32 cores, 59 GiB RAM, NVMe at 18 GB/s read, sha256 at
0.5 GiB/s per core. No local NVIDIA GPU. Those are stopwatch numbers about the hardware; everything
else in this section is arithmetic from them or from published rates, and is an estimate.

Storage is 2 TB internal with 1.2 TB free, plus a 4 TB NVMe drive added for this work — about 5.2 TB
of working space. The corpus stays on it:

| | On disk |
|---|---:|
| Argoverse 2 Sensor | ~1 TB |
| Argoverse 2 Map Change (TbV) | 922 GB |
| Argoverse 2 Motion Forecasting | 58 GB |
| nuScenes | ~350 GB |
| Iceberg warehouse, Lance tables, derived releases | ~0.5–1 TB |
| **committed total** | **~3 TB** |

Argoverse 2 Lidar would add roughly another 1.5 TB by the estimate above and is not committed.

**The corpus is local on purpose, and MLPerf Storage is why.** That benchmark measures the path from
storage through client memory; moving the corpus to object storage would remove the thing being
measured. A year of 1 TB in S3 Standard costs about what the drive did, and at the end of the year
the drive is still here.

S3's role is to prove the cloud path — IaC, IRSA, catalog, one slice — not to hold the corpus. Two
facts decide how archival is done, both of which apply directly to this data:

- **Glacier Deep Archive bills per object**, adding 32 KB of archive index plus 8 KB of
  Standard-rate metadata to each. TbV alone is 7.84 million images; at 40 KB apiece that is over
  300 GB of pure overhead.
- **Lifecycle transitions bill per request.** Moving millions of individual files into Glacier costs
  more than storing them there.

So archival applies to *derived releases, aggregated into few large objects*, after a milestone
closes — never to the working set, and never file-by-file. Egress is the other trap: pulling a
terabyte back out to this machine costs roughly a hundred times a month of storing it, while
processing it in-region costs nothing.

M1, M2, M4 and M5 run entirely locally at no cloud cost. M3 needs a small amount of AWS to prove the
IaC and deployment path, applied once with its logs and cost recorded and then destroyed. M5's
embeddings run on CPU in a couple of hours or on a rented GPU in about ten minutes.

## Status

Pre-registration stage. Nothing in the system has been measured, because nothing in it has been
built — the hardware figures above are the exception and are marked as such. Dataset sizes are
published specifications except where marked as estimates, and are verified against the actual
download in M1. When system numbers exist they appear here with the baseline, the workload, the
hardware, the commit hash and the repetition count, or they do not appear at all.
