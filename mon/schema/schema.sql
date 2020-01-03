CREATE OR REPLACE  TABLE bqtail.bqmon (
    Timestamp TIMESTAMP,
    Status STRING,
    Error  STRING,
    UploadError STRING,
    PermissionError STRING,

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
                    Min TIMESTAMP,
                    Max TIMESTAMP,
                    Count INT64,
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
                        Min TIMESTAMP,
                        Max TIMESTAMP,
                        Count INT64,
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