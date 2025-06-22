# Bento Reference Service

Reference data (genomes &amp; annotations) service for the Bento platform.

## Goals

* Provide a well-known location for genomic reference materials within Bento nodes - both human and non-human
* Provide a translation service for looking up annotations, returning responses containing genomic coordinates for 
  genes, exons, etc.
* Facilitate node-level ingestion of reference genomes
* Implement the [RefGet v2.0.0](http://samtools.github.io/hts-specs/refget.html) specification. RefGet is a standardized
  API specification for obtaining sequence data.
* Possibly function as a standalone reference data service, if ever desired.

## Status

* Bento-style genome ingestion: **DONE**
* API endpoint permissions: **DONE**
* RefGet v2.0.0 implementation: _Partially done_
* Annotation service: _Partially done_
* Tests: _Partially done_
* Documentation: _Partially done_

## Documentation

### API

To view interactive API documentation, go to the `/docs` path in a running instance of the service.
In a local Bento instance, this would (likely) look like `https://bentov2.local/api/reference/docs`.

### Other documentation

* [Basic ingestion flow](./docs/basic_ingestion_flow.md)
* [Deleting genomes](./docs/deleting_genomes.md)
* [Ingesting annotation data](./docs/ingesting_annotations.md)

### More reading

* [The FASTA format (Wikipedia)](https://en.wikipedia.org/wiki/FASTA_format)
* [The FAI index format](https://www.htslib.org/doc/faidx.html)
* The GFF3 genome annotation format
* [RefGet v2.0.0](http://samtools.github.io/hts-specs/refget.html)
