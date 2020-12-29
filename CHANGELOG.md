## December 30 2020 2.10.0
 * Added batch grouping
 * Updated error GCE internal errors handling
 * Updated contract with afs
 
## September 20 2020 2.9.0
 * Added Copy.MultiPartition to copy with $PartitionID

## September 24 2020 2.8.0
 * Added Template attribute to the copy action
 * Added Date based on the source global expanding variable

## September 24 2020 2.7.0
 * Introduced conditional action execution (When.Exists for now)
 * Extended SiteInput with Inner bool flag for INNER JOIN (default LEFT JOIN)
 * Added retries to CreateTableIfNotExist 

## August 31 2020 2.6.0
 * Extended export request with UseAvroLogicalTypes (default true for avro format export)
 * Added $TriggerBucket variable

## August 25 2020 2.5.0
 * Upgraded cloud function to g113 runtime
 * Upgraded CLI to go1.15 SDK
 * Patched post-action sharing nodes related intermittent issue 
 * Requires endly v0.49.2+


## August 8 2020 2.4.2
 * Delayed temp table deletion after last SQL chain action for post SQL execution action
  
## May 8 2020 2.4.1
 * Extended retry for load job, get job status with internal server error checks 

## April 24 2020 2.4.0
 * Added Insert action (StreamingAPI)
 * Added bqmonitor CLI

 
## April 17 2020 2.2.0
 * Extended direct eventing mode in bqtail CLI                                           
 * Added arbitrary schema field addition with JSON format
 * Added expiry option
 * Added Transient.CopyMethod to control copy strategy.
 * Removed Dest.DMLAppend 
 * Added batch source location generation to upload file only once
 * Added mover to run move operation concurrently
 * Added config.BqJobInfoPath to log batches logging
 
 
## April 10 2020 2.1.1
 * Extended batch grouping to datafile extension
 * Added BaseOperationURL CLI option
 * Patched regexp grouping in the pattern with CLI
 * Patch missing file retry error
 * Added process restart to sync mode
 * Added limit on process restart per the same ingestion process                
 * Extended direct eventing mode in bqtail CLI                                           

  
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

