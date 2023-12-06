version 1.0

workflow fasta_ref {
    input {
        String genome_id
        String taxon_term_json
        File genome_fasta
        String access_token
        String drs_url
        String reference_url
        Boolean validate_ssl
    }

    call uncompress_fasta_and_generate_fai_if_needed as s1 {
        input:
            genome_fasta = genome_fasta
    }

    call ingest_into_drs as drs_fasta {
        input:
            file = s1.fasta,
            drs_url = drs_url,
            access_token = access_token,
            validate_ssl = validate_ssl
    }

    call ingest_into_drs as drs_fai {
        input:
            file = s1.fai,
            drs_url = drs_url,
            access_token = access_token,
            validate_ssl = validate_ssl
    }

    call ingest_metadata_into_ref {
        input:
            genome_id = genome_id,
            taxon_term_json = taxon_term_json,
            fasta = s1.fasta,
            fai = s1.fai,
            fasta_drs_uri = drs_fasta.drs_uri,
            fai_drs_uri = drs_fai.drs_uri,
            reference_url = reference_url,
            token = access_token
    }
}

task uncompress_fasta_and_generate_fai_if_needed {
    input {
        File genome_fasta
    }

    command <<<
        if [[ '~{genome_fasta}' == *.gz ]]; then
            gunzip -c '~{genome_fasta}' > genome.fasta
            rm '~{genome_fasta}'
        else
            mv '~{genome_fasta}' genome.fasta
        fi
        samtools faidx genome.fasta --fai-idx genome.fasta.fai
    >>>

    output {
        File fasta = "genome.fasta"
        File fai = "genome.fasta.fai"
    }
}

task ingest_into_drs {
    input {
        File file
        String drs_url
        String access_token
        Boolean validate_ssl
    }

    command <<<
        drs_res=$(
            curl ~{true="" false="-k" validate_ssl} \
                -X POST \
                -F "file=@~{file}" \
                -F "project_id=$project_id" \
                -F "dataset_id=$dataset_id" \
                -F "public=true" \
                -H "Authorization: Bearer ~{access_token}" \
                --fail-with-body \
                "~{drs_url}/ingest"
        )
        exit_code=$?
        rm '~{file}'
        if [[ "${exit_code}" == 0 ]]; then
            jq -r '.id' <<< "${drs_res}"
        else
            exit "${exit_code}"
        fi
    >>>

    output {
        String drs_uri = read_string(stdout())
    }
}

task ingest_metadata_into_ref {
    input {
        String genome_id
        String taxon_term_json
        File fasta
        File fai
        String fasta_drs_uri
        String fai_drs_uri
        String reference_url
        String token
    }

    command <<<
        fasta-checksum-utils '~{fasta}' --fai '~{fai}' --genome-id '~{genome_id}' --out-format bento-json | \
          jq '.fasta = "~{fasta_drs_uri}" | .fai = "~{fai_drs_uri}" | .taxon = ~{taxon_term_json}' > metadata.json

        rm '~{fasta}' '~{fai}'

        RESPONSE=$(curl -X POST -k -s -w "%{http_code}" \
            -H "Content-Type: application/json" \
            -H "Authorization: Bearer ~{token}" \
            --data "@metadata.json" \
            "~{reference_url}/genomes")
        if [[ "${RESPONSE}" != "204" ]]; then
            echo "Error: Reference service replied with ${RESPONSE}" 1>&2  # to stderr
            exit 1
        fi

        echo ${RESPONSE}
    >>>

    output {
        File out = stdout()
        File err = stderr()
    }
}
