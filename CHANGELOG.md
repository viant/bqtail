## Feb 3 2020 2.0.0
  * Added custom transient project(s) on rule level support (reservation/billing project, distributing load)
  * Added batch job throttling
  * Updated dispatcher to work across projects
  * Completed jobs logging with basic info i.e bytes, slots usage, time taken
  * Added transient projects load balancer (rand/fallback)

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

