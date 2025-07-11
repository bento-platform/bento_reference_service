# Bento Reference Service

[![Lint](https://github.com/bento-platform/bento_reference_service/actions/workflows/lint.yml/badge.svg)](https://github.com/bento-platform/bento_reference_service/actions/workflows/lint.yml)
[![Test](https://github.com/bento-platform/bento_reference_service/actions/workflows/test.yml/badge.svg)](https://github.com/bento-platform/bento_reference_service/actions/workflows/test.yml)
[![Build and push bento_reference_service](https://github.com/bento-platform/bento_reference_service/actions/workflows/build.yml/badge.svg)](https://github.com/bento-platform/bento_reference_service/actions/workflows/build.yml)

Reference data (genomes &amp; features/annotations) service for the Bento platform.


## Goals

* Provide a well-known location for genomic reference materials within Bento nodes - both human and non-human
* Provide a translation service for looking up features/annotations, returning responses containing genomic coordinates 
  for genes, exons, etc.
* Facilitate node-level ingestion of reference genomes
* Implement the [RefGet v2.0.0](http://samtools.github.io/hts-specs/refget.html) specification. RefGet is a standardized
  API specification for obtaining sequence data.
* Possibly function as a standalone reference data service, if ever desired.


## Documentation

### API

To view interactive API documentation, go to the `/docs` path in a running instance of the service.
In a local Bento instance, this would (likely) look like `https://bentov2.local/api/reference/docs`.

### Other documentation

* [Basic ingestion flow](./docs/basic_ingestion_flow.md)
* [Deleting genomes](./docs/deleting_genomes.md)
* [Ingesting features](docs/ingesting_features.md)
* [Querying features](docs/querying_features.md)
* [Using RefGet](./docs/using_refget.md)

### More reading

* [The FASTA format (Wikipedia)](https://en.wikipedia.org/wiki/FASTA_format)
* [The FAI index format](https://www.htslib.org/doc/faidx.html)
* [The GFF3 genome feature format](https://gmod.org/wiki/GFF3)
* [RefGet v2.0.0](http://samtools.github.io/hts-specs/refget.html)
