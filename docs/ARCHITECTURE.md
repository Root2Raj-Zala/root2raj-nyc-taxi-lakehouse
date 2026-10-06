# Architecture, contract and recovery runbook

## Data layers and grains

Bronze is the unchanged local Parquet and zone CSV with SHA-256 provenance. Staging adds physical source-row lineage and one primary quality status. Silver consists of accepted fact_trip records, dim_date, dim_zone and a compressed local Parquet export. Gold contains one row per pickup date and one row per pickup borough.

Required source fields: pickup/dropoff timestamps, pickup/dropoff zone IDs, distance, fare, total amount and payment type. A missing field fails before replacing warehouse data. Schema presence is checked explicitly; source types are cast in staging/facts, so incompatible values fail the run.

## Routing priority

Missing required measurement → pickup outside January → duration outside (0,4 hours] → distance outside (0,100 miles] → negative financial amount → unmapped zone → accepted. Counts are primary reasons, not independent flags: a row violating several rules appears once. Quarantine preserves source_row and measurements for review. No automatically discarded row is labelled fraudulent.

## Replay and failure behaviour

The atomic transaction replaces facts, dimensions, quarantine, manifest and marts together. Failure during that transaction rolls back to the prior committed snapshot. The immutable source is not edited. Download failures leave a .download file rather than a falsely completed source. Schema failure precedes replacement. Retry the same pipeline after fixing the source/contract; the complete month is rebuilt, not appended.

Silver Parquet and CSV exports happen after the database commit. An export failure can leave the database complete but external artifacts incomplete; rerun to regenerate them. There is no distributed transaction across those files. Concurrent readers/writers, retention and privileges require a deployment design rather than an unsupported claim here.

## Operational adoption gates

Pin source schema/version and review 2025's extra congestion field before adding later years. Define the financial-adjustment ledger separately from the operational trip policy. Partition a multi-month implementation by pickup month, maintain a catalog of source checksums and quarantine policy versions, and coordinate readers on committed snapshots. Add service ownership, access controls, encrypted storage, monitoring and replay drills before production. Those are proposed extensions, not completed integrations.

## Interview explanation

I used the whole real source file and the agency's real lookup table. I defined the fact grain, documented exclusions, reconciled the money and counts, and demonstrated that retries preserve totals. The code was developed with AI assistance; this is a tested reference implementation rather than a claim of production ownership.
