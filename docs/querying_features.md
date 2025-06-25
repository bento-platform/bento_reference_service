# Querying features

Genome features can be queried from the `/genomes/<genome_id>/features` endpoint.

A few query parameters are available for filtering/querying:

* `q`: "free-text" search (`q_fzy`: whether this should be a "fuzzy" free-text search)
* `name` (`name_fzy`: whether this should be a "fuzzy" name search)
* `position`: feature formatted position **prefix search**, e.g., `chr1:4032` becomes a prefix query: `chr1:4032%`
* `contig`: feature contig name, matching primary contig name of the corresponding reference genome
* `start`: includes all features with **start position** `>=` this value
* `end`: includes all features with **start position** `<=` this value
* `feature_type`: GFF3 feature type (e.g., gene, exon)
* `offset`: Pagination record offset
* `limit`: Pagination record limit

Responses are of the following form:

```js
{
  "results": [/* ... */],
  "pagination": {
    "offset": 0, // Record offset from start of result-set
    "limit": 10, // Record limit, per-page
    "total": 100 // Total # of matching records
  }
}
```
