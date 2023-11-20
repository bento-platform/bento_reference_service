version 1.0

workflow fasta_ref {
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

    call ingest_metadata_into_ref {
        input:
            fasta_bgzip = s1.fasta_bgzip,
            fai = s1.fai,
            fasta_drs_uri = drs_fasta.drs_uri,
            fai_drs_uri = drs_fai.drs_uri,
            reference_url = reference_url,
            token = access_token
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
        TODO: ingest + output DRS URI
    >>>

    output {
        String drs_uri = "TODO"
    }
}

task ingest_metadata_into_ref {
    input {
        String fasta_bgzip
        String fai
        String fasta_drs_uri
        String fai_drs_uri
        String reference_url
        String token
    }

    command <<<
        fasta-checksum-utils genome.fa.gz --genome-id GRCh38 --out-format bento-json | \
          jq '.fasta = "~{fasta_drs_uri}" | .fai = "~{fai_drs_uri}"' > metadata.json

        RESPONSE=$(curl -X POST -k -s -w "%{http_code}" \
            -H "Content-Type: application/json" \
            -H "Authorization: Bearer ~{token}" \
            --data "@metadatajson" \
            "~{reference_url}/genomes")
        if [[ "${RESPONSE}" != "204" ]]
        then
            echo "Error: Reference service replied with ${RESPONSE}" 1>&2  # to stderr
            exit 1
        fi
        echo ${RESPONSE}
    >>>

    output {
        String out = stdout()
        String err = stderr()
    }
}
