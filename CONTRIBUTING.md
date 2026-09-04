# Conventions

## Commit messages

```
type: subject in lowercase
```

`type` is one of `feat`, `fix`, `test`, `docs`, `style`, `chore`, `refactor`.

**No parenthesised scope.** Write `docs: state the blob contract`, never
`docs(data-platform): state the blob contract`. The subject says what changed; the diff says where.

Subjects are lowercase, imperative, and describe the change rather than the activity — `fix: reject
a manifest whose checksum was never read`, not `fix: fixed bug`.

No trailers naming a tool or assistant. Authorship is the person who ran the work.

## Branches

`feat/*` → `dev` → `main`. Branch from `dev`, merge back to `dev`. Promotion to `main` is a
deliberate step, not a consequence of merging.

## Documentation

Only `README.md` files live in this repository. Design notes, pre-registrations, measurement
records and decision history are kept outside it and are not mirrored here.

A README states what exists. When a claim needs a number, the number comes with its baseline,
workload, hardware, commit hash and repetition count, or the claim is not made.
