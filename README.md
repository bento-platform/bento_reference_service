# Bento Reference Service

Reference data (genomes &amp; annotations) service for the Bento platform.

## Goals

* Provide a well-known location for genomic reference materials within Bento nodes - both human and non-human
* Provide a translation service for looking up annotations, returning responses 
  containing genomic coordinates for genes, exons, etc.
  * Use ElasticSearch to index features to allow fuzzy text search on feature name
* Facilitate project-specific ingestion of reference genomes
* Implement the [RefGet](http://samtools.github.io/hts-specs/refget.html) specification for obtaining sequence data

## Status

* Bento-style genome ingestion: **DONE**
* API endpoint permissions: _Partially done_
* RefGet implementation: _Partially done_
* Annotation service: **Not done**
* Tests: _Partially done_
* Documentation: **Not done**

## API

TODO
