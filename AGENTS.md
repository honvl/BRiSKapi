# Contributor automation guidance

Use Python in `.venv`; install `requirements-dev.txt` for local archive tests.
Do not commit downloaded vendor assets, market recordings, AWS credentials or
upload tickets or SBI session cookies. Public `briskapi/archive.json` contains only
bucket, region and API URL. SBI market recordings require explicit opt-in at the
beginning of each capture (Enter accepts the interactive prompt) and current
contribution consent. Other SBI sessions contribute timing reports only. Never
archive cookies, tokens or connection diagnostics. New uploads must be decoded;
the existing legacy wire capture is labeled sample_data and is not an ingest format.

Run focused archive and API coverage (minimum 85%), the fixture reference test,
Node tests and Rust tests/fmt/Clippy. CI supplies downloaded demo fixtures. Keep README.md (usage only),
its Japanese translation README.ja.md, ARCHITECTURE.md (internals), PRIVACY.md and
schema documentation synchronized; changes to collected data bump `POLICY_VERSION`.

`infra/deploy.py` mutates AWS resources and is reserved for explicit deployment
work. `cloud_smoke.py` publishes self-authored synthetic data to the configured
archive. Unit tests must never publish data or mutate cloud resources.
