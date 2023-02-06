workflow ingest_GRCh38_UCSC_IDs_no_alt {
    call download_genome_and_checksum {}
}

task download_genome_and_checksum {
    command <<<
        GENOME_PATH="https://ftp.ncbi.nlm.nih.gov/genomes/all/GCA/000/001/405/GCA_000001405.27_GRCh38.p12/GRCh38_major_release_seqs_for_alignment_pipelines/GCA_000001405.15_GRCh38_no_alt_analysis_set.fna"
        wget "$\{GENOME_PATH\}.gz" -O "genome.fa.gz"
        wget "$\{GENOME_PATH\}.fai" -O "genome.fa.fai"
        fasta-checksum-utils genome.fa.gz --genome-id GRCh38 --out-format bento-json > metadata.json
    >>>

    output {
        File genome = "genome.fa.gz"
        File genome_index = "genome.fa.fai"
        File metadata_json = "metadata.json"
    }
}
