# Tokyo private archive

The deployed service uses a private S3 bucket, two Python 3.12 ARM64 Lambdas with function
URLs (the archive service, which also has the S3 event trigger, and the cloud API) and a
DynamoDB on-demand quota table that both use. All data, compute and quota
resources are in `ap-northeast-1`; IAM is global. No GitHub secrets are required for
ordinary recording or uploading. Each uploader reaches only its own uploads, by its identity
(see the private archive section of [ARCHITECTURE.md](../ARCHITECTURE.md)).

With operator AWS credentials, provision a **new dedicated** installation:

```sh
.venv/bin/python infra/deploy.py --bucket YOUR_UNIQUE_TOKYO_BUCKET [--seed-demo PACKAGE]
```

This updates `briskapi/archive.json` (bucket, region, `api_url` and `query_url`). Deployment
is repeatable and updates Lambda code. The archive function is deployed first, then the cloud
API's (below). Resource names are `brisk-recorder-archive` (the project's former name; renaming them
would create a second deployment) and, for the cloud API's function, role and log group,
`brisk-recorder-query`. Use a separate AWS account or change
`NAME` and `QUERY` for a second installation. The deployer needs S3, Lambda, IAM role/policy,
CloudWatch Logs, DynamoDB and STS access. It does not modify default AWS regions.

The execution role can read and write only staging, archive, timing, `reference/` and
`bars/` objects, list the bucket (so that a missing key answers 404, not 403) and object
versions only under `archive/` and `timing/`, delete current
objects in staging, archive, timing and `bars/`, and delete object versions only in staging (so an
owner's delete leaves noncurrent versions it cannot purge, and it can delete nothing under
`reference/`). It also updates the quota table and writes its own logs. The function URL accepts requests
without AWS credentials; each names its owner with an identity secret, and the function
scopes it to that owner's prefixes. S3 POST policies grant only the ticket's particular key
and exact size, and downloads are 15-minute presigned URLs for the owner's own objects. The bucket
has S3 Transfer Acceleration on, and upload forms are for its endpoint (the function's
`UPLOAD_ACCELERATION`), so an upload enters AWS at its nearest edge location; AWS charges $0.04/GB
for it only where it is faster.
The bucket blocks public ACLs; its policy allows anonymous reads and listings of `samples/`
only and refuses non-TLS requests. ACLs are disabled, encryption and versioning enabled.
Logs expire after 14 days.

Ingest accepts canonical demo/synthetic recordings that match a reference replay
and decoded broker recordings (SBI, e-shiten, Matsui, Monex, SMBC Nikko; a trading session each, up to 4 GiB,
checked a row group at a time on the function's 10 GB of `/tmp`, by `brisk-parquet`, which `deploy.py` builds static for arm64 with Docker and ships in the function's zip) with structural checks, and timing reports
from either. It
stores the verified Parquet tables, each as its own object (see the integrity and storage
sections of [ARCHITECTURE.md](../ARCHITECTURE.md)). The bucket's prefixes:

| Prefix | Holds |
| --- | --- |
| `incoming/TICKET/` | Staging: a ticket's manifest, owner, status and the uploaded `recording.zip`, the only zip |
| `archive/OWNER/YYYYMMDD/FROM-TO-SHA256/` | A recording's tables, `files.json` (where each is) and, written last, `manifest.json` |
| `reference/SOURCE/TIMELINE/` | A demo or synthetic replay's market tables, kept once, and `complete.json` |
| `timing/OWNER/YYYYMMDD/` | Timing reports |
| `bars/OWNER/YYYYMMDD/` | An owner's OHLCV bars of a trading day (`ohlcv-1s`, `-1m`, `-1h`, `-1d` `.parquet`), built from its live recordings of that day |
| `results/OWNER/` | Cloud API results too large for a response, kept for a day |
| `samples/`, `deploy/` | The legacy sample download; the dependency layer |

A demo or synthetic replay's market tables are the same for every recording of it, so the
first verified replay becomes the archive's copy in `reference/`, and each recording of
that replay (the first too) stores only its timing table. Later uploads of the replay send
just that table; until a copy exists the service answers HTTP 409 and the client sends the
whole recording. Nothing is configured for this, and an owner's delete leaves the copy
alone. The synthetic probe of `cloud_smoke.py` leaves its own copy under
`reference/synthetic_test/` the same way. The deployment package (one zip, both functions) holds
`archive_service.py`, `query_engine.py`, `query_service.py`, `briskapi/schema.py`,
`briskapi/_parquet.py`, `briskapi/records.py` (the record schemas, shared with `briskapi.Live`) and
`briskapi/references/*.json`, with an empty package init so the client API stays
out of it. pyarrow and numpy come from a layer the deployer builds from their pinned
manylinux arm64 wheels (`LAYER_REQUIREMENTS`; pip downloads them from PyPI) and
publishes as a layer version through the bucket's `deploy/` prefix, once per distinct layer;
the cloud API's function uses the same layer.
The archive function has 3,008 MB, the most this account's Lambda allows: verifying a complete demo replay holds its decoded
columns in memory (about 2.2 GB at peak) and takes under a minute, a timing-only upload
included, since the service verifies the assembled recording in full either way. Each validated staging upload, and any extra upload made
with an already-used ticket, is deleted on arrival. The deployer creates a random
`QUOTA_SALT` once and keeps it across redeploys, so per-IP quota entries are keyed
hashes that cannot be reversed to addresses.

The deployer reserves concurrency at four for the archive function and eight for the cloud
API's when the regional account quota allows it, which caps what either can cost. The current
small Tokyo account quota prevents reserving that capacity; AWS's
account concurrency limit and application quotas still apply. This does not alter
other functions' concurrency allocations. Every archive request counts against its address
(120 a minute) before its identity is read, and deletes and restores against both (60 an hour).
Upload quotas are intentionally modest and defined in `archive_service.py`; adjust them when
usage warrants it.

S3 can take a few minutes to apply a changed notification filter (0.9.0 changed it
from `events.jsonl.gz` to `recording.zip`). An upload in that window is not ingested;
invoke the function with that object's S3 event (key and version) to process it, or
let staging expire it after two days.

Events are version bound, storage uses conditional immutable writes and the
manifest is a commit marker (a replay's `reference/` copy has `complete.json`).
Infrastructure/storage failures retry via Lambda;
malformed data produces a `rejected` status. The CLI prints a bearer-ticket
status URL and reports a pending upload if its wait expires. Operators can
inspect `/aws/lambda/brisk-recorder-archive` for infrastructure failures. Rejected
recordings are never stored.

The cloud API ([docs/cloud-api.md](../docs/cloud-api.md)) is a second function,
`brisk-recorder-query` (`query_service.py` with `query_engine.py`), with its own role, log
group (14 days) and function URL, which accepts requests without AWS credentials: each names
its owner with an identity secret as its API key. It runs Python 3.12 on arm64 with the same
layer, 3,008 MB and a 300 second timeout; its environment is `ARCHIVE_BUCKET` and
`QUOTA_TABLE` and the archive's `QUOTA_SALT` (so per-address entries are keyed hashes), and it
has no bucket trigger. Its role can read `archive/`, `reference/`, `bars/` and `results/`
objects, write `results/`, list the bucket (unconditionally, for the same 404 reason), update the
quota table and write its own logs. It cannot write elsewhere or read staging or timing
reports. Quotas: 60 requests a minute and 2 GB returned a UTC day per identity, and, since
identities cost nothing to make, 120 requests a minute and 4 GB returned a day per IP address
(IPv6 by /64, counted before the key is read) and 20 GB returned a day for everyone (`REQUESTS`,
`RETURNED`, `ADDRESS`, `ADDRESS_BYTES` and `SERVED` in `query_service.py`), over which a request
gets HTTP 429. The global byte cap bounds the data-transfer bill; the per-address one keeps a
single address from using it up. Results of up to 4 MiB are returned
inline; larger ones are written to `results/OWNER/UUID.EXT` and fetched through a 303 to a
15-minute presigned URL. The function prints nothing, so its log group holds only Lambda's
own start, end and report lines.

Each owner's live broker recordings also have OHLCV bars built ahead, under
`bars/OWNER/YYYYMMDD/`. The archive function rebuilds an owner's day of bars whenever one of its
live recordings of that day is stored, deleted or restored, inside that request or ingest: it
reads the trades of the owner's recordings of the day (about 0.8 s for a day of a million
trades) and writes the four bar files. No other owner's recordings are read. A rebuild that
fails removes that day's bars and logs `Bars for DATE not rebuilt` in the archive function's
log group; the next store, delete or restore rebuilds them, and until then queries build them
from the recordings. The role can delete current `bars/` objects but no versions, so replaced
or removed bars remain as noncurrent versions until `ExpireReplacedBars` (below) removes them.
A rebuild that finds the owner's recordings of the day changed while it ran (another upload,
delete or restore) runs again, up to three times. 1.0.0 kept one set of shared bars per day
under `central/`; none was ever built in this deployment, and nothing uses that prefix now.

`XTKS.BRISKDEMO` reads the archive's copy of the demo replay under `reference/historical_mock/`
and answers 404 until it exists. The first verified whole demo upload from any identity
creates it. `--seed-demo PACKAGE` does so at deployment: after deploying it uploads PACKAGE
(a complete demo recording with its `manifest.json`: `brisk record --no-upload
--output DIR` writes one, except that with upload consent revoked it writes the tables alone)
under the identity of the machine running `deploy.py`, sending the whole recording whether
or not a copy exists, and adds the result as `seeded` to what it prints. That upload counts
against the identity's upload quotas, and the recording stays in its archive like any other.

Costs accrue to the operator: storage, data transfer, Lambda requests/compute and
DynamoDB requests. Staging expires; stored data is kept until its owner deletes it, and a
deleted upload for 31 days after that (below). The
daily upload budget controls ticket issuance, not lifetime archive size or read traffic;
queries add compute, S3 reads and the data returned, which the per-identity quotas bound
for one identity only.

An owner's delete (`brisk delete`) removes the current objects, and the versioned bucket keeps
the earlier versions as noncurrent ones. `brisk list --deleted` shows what can still be
restored and `brisk restore` brings it back, for 30 days (`RESTORE_DAYS` in
`archive_service.py`). The lifecycle rules `deploy.py` sets, `ExpireDeleted-archive` and
`ExpireDeleted-timing`, remove noncurrent versions under `archive/` and `timing/` 31
days after the delete (`DELETED_DAYS`) and expired delete markers, so a deleted upload is gone a day after its
restore window closes; S3 applies lifecycle rules asynchronously, so removal can trail that by
a day. Staging has its own rule (`ExpireStaging`: two days), and so do the cloud API's
prefixes: `ExpireResults` removes `results/` objects a day after they are written (and their
noncurrent versions a day after they become so), and `ExpireReplacedBars` removes the
noncurrent versions of `bars/` objects a day after they become so, with expired delete
markers. Recordings' tables are stored in S3 Intelligent-Tiering (`_put` in `archive_service.py`),
as they are mostly written once and seldom read: unread for 30 days a table costs about half, for 90
a fifth, read as fast and with no retrieval fee. `IntelligentTiering-archive` and
`IntelligentTiering-reference` move the tables stored before that (objects under 128 KB, the
manifests, stay as they are). Deployment replaces the bucket's
whole lifecycle configuration, so a rule added by hand does not survive it, and the new rules
also reach noncurrent versions that exist when they are first deployed: those older than 31
days go at the next lifecycle run.

Making the bucket private (policy version 6) left the objects stored while it was public
in place, outside any owner's prefix: no one can reach them through the function, and the
operator removes them on request or with the AWS console. Each deployment copies the legacy
wire-format sample (`brisk-sbi-wire-jsonl-v1`) among them to `samples/DATE/SHA256/`, the
public download (`publish_samples` in `deploy.py`; idempotent, the originals stay).

To stop new uploads, disable/delete the function URL or set reserved
concurrency to zero; owners then can't reach their uploads either. To stop queries,
disable/delete the cloud API's function URL (it only reads, apart from `results/`, so
stored data is unaffected). For complete
removal: remove bucket notifications, delete the Lambdas/URLs, quota table, log groups
and execution roles/inline policies (`brisk-recorder-archive` and `brisk-recorder-query`),
then remove all bucket object versions and delete
the bucket. Removing the versioned bucket is an explicit destructive operation;
the deployer does not do it automatically.

Broker recordings from briskapi 1.9 on have two more columns in their depths table (book changes and
checkpoints) and the `book-deltas` schema reads them. Deploy before releasing such a client: the
archive's verifier (`brisk-parquet` in the archive function) refuses the extended table until it is
updated, and the query function serves `book-deltas` only once it is.
