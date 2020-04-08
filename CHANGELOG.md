## April 10 2020 2.1.1
 * Extended batch grouping to datafile extension
 * Added BaseOperationURL CLI option
 * Patched regexp grouping in the pattern with CLI
 
## April 6 2020 2.1.0
 * Added DMLAppend option (since DML has no more limits, it is possible to reduce the batch frequency with that option)
 * Added AllowFieldAddition that works also with JSON source format
 * Streamline bqtail CLI with bucket/project extraction
 * Patched mem fs race condition
 * Patched dest split with partitioned template
 * Added cli batching stress test
 
## March 27  2020 2.0.3
 * Refactored retries

## March 14 2020 2.0.2
 * Patch transient table auto clustering and partitioning  with table split option
 * Added expiry time exclusion, when create table from a template
 
## March 12 2020 2.0.1
 * Added autodetect option cli options
 * Added cap to list operation in dispatcher
 * Minor patches
 * Added drop table retry
 * Added temp table creation when template option is used

   
## Feb 28 2020 2.0.0
  * Streamlined actions, introduced Process/Activity: BREAKING CHANGE - see [Migration](MIGRATION.md) 
  * Added custom transient project(s) on rule level support (reservation/billing project, distributing load)
  * Added batch job throttling
  * Updated dispatcher to work across projects
  * Completed jobs logging with basic info i.e bytes, slots usage, time taken
  * Added transient projects load balancer (rand/fallback)
  * Deprecated TransientDataset - please use Transient.Dataset (currently both supported)
  * Patch pattern setting with yaml format
  * Added seamless rule transition (more than one rule matching the same path but only one enabled) 
  * Dest.Schema.TransientTemplate move to Dest.Transient.Template
  * Added Rule.MaxReload option to control attempts to re-run load job, each excluding corrupted location from batch load job.
  * Added Config.Async - the global setting for all rules
  * Added URL pattern name substitution parameters
  * Added pubsub push action
  * Added stand-alone BqTail command
  * Added LongRunning process info (bqmon)
  * Added bq.query action destination template
  * Update Dynamic Table Destination (split) to work with AVRO source files
  * Added dynamic patching with Schema.template

## Jan 14 2020 1.1.0

  * Added HTTP API call
  * Added YAML rule format support
  * Optimized further down Storage Class A usage
  * Streamlining error handling
    - recoverable vs non-recoverable errors
    - recoverable error with retires limit

  * Enhanced monitoring
    - added unprocessed files check
    - added error reporting per rule, (Permission, InvalidSchema, CorruptedData)
    - added scheduler with bqtail rule to get monitor checks to BigQuery bqtail.bqmonitor table.

  * End to end testing
    - streamline serverless wait time
    - added common error use cases
    - refactor rule from JSON to YAML

