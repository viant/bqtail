### Batch grouping by filename parameters

This scenario uploads four files in one batch window. Two files belong to
`(UidType=ip, UserPoolID=123)` and two belong to
`(UidType=device, UserPoolID=234)`.

The rule groups on both parameters, producing two batch jobs and preserving
the correct filename-derived values on every destination row.
