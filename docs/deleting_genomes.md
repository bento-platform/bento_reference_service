# Deleting genomes

Genomes (with all related contig/annotation records) can be deleted from the reference service database via the 
`DELETE /genomes/<genome ID>` endpoint.

**It is important to note** that this DOES NOT remove the FASTA/FAI/GFF3/GFF3 TBI files from 
[DRS](https://github.com/bento-platform/bento_drs); removing these is left to the deleter, if desired.
