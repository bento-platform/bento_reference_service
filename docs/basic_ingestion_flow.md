# Basic ingestion flow

A JSON object following the [`Genome` Pydantic model format](../bento_reference_service/models.py) should be POSTed
to the `/genomes` endpoint.

The bulk of this JSON object (barring correct URLs) can be generated using the 
[`fasta-checksum-utils`](https://github.com/bento-platform/fasta-checksum-utils) CLI tool.

The service expects that the FASTA and FAI (index) files will have already been either ingested into 
[DRS](https://github.com/bento-platform/bento_drs) or are located at a public HTTP-accessible location; these URLs are
passed in the JSON object.

Optionally, a GFF3 containing features, with a GFF3 TBI index, can be passed in as well. These features must be 
indexed in the service before being usable; see the [Ingesting feature data](./ingesting_features.md) guide for 
more information.
