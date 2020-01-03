## Jan 7 2019 1.1.0

  * Added HTTP API call
  * Added YAML rule format support
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

