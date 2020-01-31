CREATE OR REPLACE TABLE bqtail.bqjob (
ProjectID STRING,
JobType STRING,
JobID STRING,
DestinationTable STRING,
Error STRING,
TempTable STRING,
CreateTime TIMESTAMP,
StartTime TIMESTAMP,
EndTime TIMESTAMP,
ReservationName STRING,
TotalBytesProcessed INT64,
InputFileBytes INT64,
InputFiles INT64,
OutputBytes INT64,
OutputRows  INT64,
BadRecords INT64,
ExecutionTimeMs INT64,
TotalSlotMs INT64,
TimeTakenMs INT64
) PARTITION BY DATE(CreateTime);


CREATE OR REPLACE  TABLE bqtail.bqmon (
    Timestamp TIMESTAMP,
    Status STRING,
    Error  STRING,
    UploadError STRING,
    PermissionError STRING,
    SchemaError STRING,
    CorruptedError STRING,
    Running STRUCT<
                   Count INT64,
                   Min TIMESTAMP,
                   Max TIMESTAMP,
                   Lag STRING,
                   LagInSec INT64
                 >,
    Stages STRUCT<
                        Items  ARRAY<STRUCT<
                                            Key STRING,
                                            Min TIMESTAMP,
                                            Max TIMESTAMP,
                                            Count INT64
                                        >
                                >
    >,
    Stalled STRUCT<
                     Items  ARRAY<STRUCT<
                                            Key STRING,
                                            Min TIMESTAMP,
                                            Max TIMESTAMP,
                                            Count INT64
                                        >
                    >
    >,
    Scheduled STRUCT<
                   Count INT64,
                   Min TIMESTAMP,
                   Max TIMESTAMP,
                   Lag STRING,
                   LagInSec INT64
    >,
   InvalidSchema STRUCT<
        Min TIMESTAMP,
        Max TIMESTAMP,
        Count INT64
    >,
    Corrupted STRUCT<
        Min TIMESTAMP,
        Max TIMESTAMP,
        Count INT64
    >,

    Dest ARRAY<
            STRUCT<
                    Table STRING,
                    RuleURL STRING,
                    Running STRUCT<
                                   Count INT64,
                                   Min TIMESTAMP,
                                   Max TIMESTAMP,
                                   Lag STRING,
                                   LagInSec INT64
                                 >,
                    Scheduled STRUCT<
                                   Count INT64,
                                   Min TIMESTAMP,
                                   Max TIMESTAMP,
                                   Lag STRING,
                                   LagInSec INT64
                    >,
                    Done STRUCT<
                            Min TIMESTAMP,
                            Max TIMESTAMP,
                            Count INT64
                    >,
                    Stalled STRUCT<
                         Items  ARRAY<STRUCT<
                            Min TIMESTAMP,
                            Max TIMESTAMP,
                            Count INT64,
                            Lag STRING,
                            LagInSec INT64
                            >
                        >
                    >,
                    Stages STRUCT<
                        Items  ARRAY<STRUCT<
                                            Key STRING,
                                            Min TIMESTAMP,
                                            Max TIMESTAMP,
                                            Count INT64
                                        >
                                >
                    >,
                    Error STRUCT<
                                 ProcessURL STRING,
                                 Message STRING,
                                 EventID INT64,
                                 ModTime TIMESTAMP,
                                 Destination STRING,
                                 IsPermission BOOL,
                                 IsSchema BOOL,
                                 IsCorrupted BOOL
                    >,
                    InvalidSchema STRUCT<
                                        Min TIMESTAMP,
                                        Max TIMESTAMP,
                                        Count INT64
                    >,
                    Corrupted STRUCT<
                            Min TIMESTAMP,
                            Max TIMESTAMP,
                            Count INT64
                    >
            >
        >
) PARTITION BY DATE(Timestamp);