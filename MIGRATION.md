##Migration 

The following document outline migration step between major bqtail versions:

#### v1.x.x -> 2.x.x
 1. Set global config.Disable flag to true
 2. Deploy only bqtail cloud functions (v1.1.1)
 3. Wait for all events processing ($AsyncTaskURL should be empty)
 4. Deploy all components with version v2.x.x.
 5. Set global config.Disable flag to false
 6. Replay backlogged data file

