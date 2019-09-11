# End to end testing

## Prerequisites

This project uses [endly](https://github.com/viant/endly/) end to end test runner.

1. Install latest [runner](https://github.com/viant/endly/releases) or use [endly docker image](https://github.com/viant/endly/tree/master/docker)
2. Create/setup dedicated end to end testing GCP project
3. Create e2e service account with admin permission on dedicated e2e test project
4. Generate and download [google secrets](https://github.com/viant/endly/tree/master/doc/secrets#gc) to ~/.secret/gcp-e2e.json 
2. Checkout the this project:
```bash
git clone https://bqtail.git
cd bqtail/e2e
# run e2e test runner
endly
```

## Use cases

- [Synchronous data files ingestionn](regression/cases/001_tail_nop/README.md)

- [Asynchronous data ingestion](regression/cases/002_tail_async/README.md)

- [Synchronous in batch data ingestion](regression/cases/003_tail_batch_sync/README.md)

- [Asynchronous in batch data ingestion](regression/cases/004_tail_batch_async/README.md)

- [Ingestion with transient dataset](regression/cases/005_tail_transient)

- [Ingestion with data deduplication](regression/cases/006_tail_batch_dedupe)

- [Ingestion with nested data deduplication](regression/cases/007_tail_dedupe_nested)


