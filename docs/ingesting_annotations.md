# Ingesting annotation data

Genome annotations (in the form of GFF3 files, pre-ingestion) can be loaded into the reference service via a two-step 
process.

First, the genome record should be updated with URLs for the GFF3 file and index, via, e.g., PATCHing to 
`/genomes/hg38` with the following body (where `hg38` is a genome ID):

```json
{
  "gff3_gz": "drs://<...>",
  "gff3_gz_tbi": "drs://<...>"
}
```

Then, these records must be loaded into the queryable database endpoint; this can be done by creating an 
`ingest_features` task by POSTing to the `/tasks` endpoint, with a body like the following:

```json
{
  "genome_id": "hg38",
  "kind": "ingest_features"
}
```

This will respond with a new task record, looking something like the following:

```json
{
  "id": 1,
  "status": "queued",
  "message": "",
  "created": "2025-06-25T09:22:28.080719"
}
```

Then, the task status can be checked by going to `/tasks/1` (where `1` here is the task ID); the `status` field can be 
one of `queued`, `running`, `success`, `error`. The `error` status should also come with a populated `message` field.

When annotations are loaded into the genome (in this example `hg38`), they can be queried at the 
`/genomes/hg38/features` endpoint.
