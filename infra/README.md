# Tokyo shared archive

The deployed service uses an S3 bucket, a Python 3.12 ARM64 Lambda with a public
function URL and S3 event trigger, and a DynamoDB on-demand quota table. All data,
compute and quota resources are in `ap-northeast-1`; IAM is global. No GitHub
secrets are required for ordinary recording or contribution.

With operator AWS credentials, provision a **new dedicated** installation:

```sh
.venv/bin/python infra/deploy.py --bucket YOUR_UNIQUE_TOKYO_BUCKET
```

This updates `briskapi/archive.json`. Deployment is repeatable and updates Lambda code.
Resource names are `brisk-recorder-archive` (the project's former name; renaming them
would create a second deployment). Use a separate AWS account or change
`NAME` for a second installation. The deployer needs S3, Lambda, IAM role/policy,
CloudWatch Logs, DynamoDB and STS access. It does not modify default AWS regions.

The execution role can read/write only staging and archive objects, delete
staging objects, update the quota table and write its own logs. The function URL
permits anonymous ticket requests; S3 POST policies grant only the ticket's
particular key and exact size. Public bucket policy permits only TLS reads of
`archive/` and listing that prefix. ACLs are disabled, encryption and versioning
enabled. Logs expire after 14 days.

Ingest accepts canonical demo/synthetic recordings that match a reference replay
and opted-in decoded SBI recordings with structural/continuity checks. It
publishes its own gzip of the validated content (see the integrity section of
[ARCHITECTURE.md](../ARCHITECTURE.md)). The deployment package holds
`archive_service.py`, `briskapi/schema.py` and `briskapi/references/*.json`,
with an empty package init so the client API stays out of it. The function has
1,769 MB (one vCPU) because validating and recompressing a complete 420 MB replay
takes about a minute. Each validated staging upload, and any extra upload made
with an already-used ticket, is deleted on arrival. The deployer creates a random
`QUOTA_SALT` once and keeps it across redeploys, so per-IP quota entries are keyed
hashes that cannot be reversed to addresses.

The deployer reserves concurrency at four when the regional account quota allows
it. The current small Tokyo account quota prevents reserving that capacity; AWS's
account concurrency limit and application quotas still apply. This does not alter
other functions' concurrency allocations. Upload quotas are intentionally modest
and defined in `archive_service.py`; adjust them when usage warrants it.

Events are version bound, publications use conditional immutable writes and the
manifest is a commit marker. Infrastructure/storage failures retry via Lambda;
malformed data produces a private `rejected` status. The CLI prints a bearer-ticket
status URL and reports pending publication if its wait expires. Operators can
inspect `/aws/lambda/brisk-recorder-archive` for infrastructure failures. Rejected
recordings are never copied into the public catalog.

Costs accrue to the operator: storage, data transfer, Lambda requests/compute and
DynamoDB requests. Staging expires; published data is retained. The daily upload
budget controls ticket issuance, not lifetime public archive size or read traffic.

To stop new contributions, disable/delete the function URL or set reserved
concurrency to zero. Existing public recordings remain readable. For complete
removal: remove bucket notifications, delete the Lambda/URL, quota table, log group
and execution role/inline policy, then remove all bucket object versions and delete
the bucket. Removing the versioned bucket is an explicit destructive operation;
the deployer does not do it automatically.
