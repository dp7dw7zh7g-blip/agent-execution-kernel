# UNAE–DTOS Adapter (integration slice 0.1)

Independent integration package. It does not modify the upstream UNAE 4.3S or DTOS packages.

Implemented in this slice:
- protocol envelope validation for v1.1 and per-message required fields;
- durable inbox with duplicate detection by `message_id`;
- quarantine when a repeated `message_id` carries different immutable content;
- append-only durable correlation `operation_id -> effect_intent_id -> effect_attempt_id`;
- strict `fencing_token` validation against SQLite's signed 64-bit INTEGER range;
- no external retries and no automatic transition/commit on receipt.

Not yet implemented in this slice: orchestration calls into both Stores, signed receipt verification bridging, external provider dispatch, reconciler, and end-to-end fault-injection tests. Do not deploy as a production adapter until these are complete.

Run tests from this directory:

```sh
PYTHONPATH=. python -m unittest discover -s tests -v
```
