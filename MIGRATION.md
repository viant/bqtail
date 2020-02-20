##Migration 

The following document outline migration step between major bqtail versions:

#### v1.x.x -> 2.x.x

1. Deploy build v1.1.1
    - set global config.Disable flag to true
    - wait for all events processing ($AsyncTaskURL should be empty)
    - update to version v2.x.x.
    - set global config.Disable flag to false
    - replay backloged file files

