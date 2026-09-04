# Autonomy System

Infrastructure for data-driven development of autonomous machines, built one piece at a time.

## What is here

| | |
|---|---|
| [`data-platform/`](data-platform/) | A lakehouse and data-quality platform for multimodal autonomous-driving sensor logs. Pre-registration stage — no measurements taken yet. |

That is the whole index. **A directory appears here when it has a deliverable in it**, so this
table describes the repository rather than planning it.

Reserving directories for intended work is the failure this rule exists to prevent. A folder named
for an intention, still empty a year later, is visible evidence of a plan that did not happen — and
it is visible on the front page, which is the surface a reader actually looks at. Plans are cheap
to revise while they are written down somewhere else; a plan encoded in a directory listing is not.

## Direction

The long-term aim is an end-to-end system for physical AI: sensor data through evaluation,
simulation, and eventually a robot runtime.

```
collect → curate → evaluate → simulate → deploy → collect
```

`data-platform` implements one segment of that loop and nothing more. Stating the direction is not
a claim of progress against it — when a second segment exists, the diagram above becomes a
description instead of an intention.

Each piece added here has to stand on its own. `data-platform` is worth building whether or not the
rest of the chain ever gets built, and that is the property every future subdirectory needs. Pieces
that need hardware or a simulator not available today are not started today.

## Status

Nothing here has been measured yet. When the first numbers exist they appear in the relevant
subdirectory's README together with the baseline, the workload, the hardware, the commit hash, and
the repetition count — or they do not appear at all.

Runs that fail their pre-registered checks are published as runs that failed their pre-registered
checks.
