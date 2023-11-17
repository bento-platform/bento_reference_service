workflow ingest_fasta {
    input {
        String genome_id
        File genome_fasta
        String access_token
        String drs_url
        String reference_url
    }

    call generate_bgzipped_fasta_and_fai_if_needed as s1 {
        input:
            genome_fasta = genome_fasta
    }

    call ingest_into_drs as drs_fasta {
        input: file = s1.fasta_bgzip, access_token = access_token
    }

    call ingest_into_drs as drs_fai {
        input: file = s1.fai, access_token = access_token
    }
}

task generate_bgzipped_fasta_and_fai_if_needed {
    input {
        File genome_fasta
    }

    command <<<
        bgzip -c '~{genome_fasta}` > genome.fasta.gz
        samtools faidx genome.fasta.gz --fai-idx genome.fasta.gz.fai
    >>>

    output {
        File fasta_bgzip = "genome.fasta.gz"
        File fai = "genome.fasta.gz.fai"
    }
}

task ingest_into_drs {
    input {
        File file
        String access_token
    }

    command <<<
        TODO: ingest + output DRS Id
    >>>

    output {
        String drs_id = "TODO"
    }
}

task generate_metadata {
    command <<<
        fasta-checksum-utils genome.fa.gz --genome-id GRCh38 --out-format bento-json > metadata.json
    >>>

    output {
        File genome = "genome.fa.gz"
        File genome_index = "genome.fa.fai"
        File metadata_json = "metadata.json"
    }
}
