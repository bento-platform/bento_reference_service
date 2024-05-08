version 1.0

workflow fasta_ref {
    input {
        String genome_id
        String taxon_term_json
        File genome_fasta
        File? genome_gff3
        String access_token
        String drs_url
        String reference_url
        Boolean validate_ssl
    }

    call uncompress_fasta_and_generate_fai_if_needed as s1 {
        input:
            genome_id = genome_id,
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

    if (defined(genome_gff3)) {
        call normalize_and_compress_gff3_and_index as gi {
            input:
                genome_id = genome_id,
                gff3 = select_first([genome_gff3])  # Coerce File? into File via select_first
        }

        call ingest_into_drs as drs_gff3 {
            input:
                file = gi.sorted_gff3_gz,
                drs_url = drs_url,
                access_token = access_token,
                validate_ssl = validate_ssl
        }

        call ingest_into_drs as drs_gff3_tbi {
            input:
                file = gi.sorted_gff3_gz_tbi,
                drs_url = drs_url,
                access_token = access_token,
                validate_ssl = validate_ssl
        }
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
            token = access_token,
            validate_ssl = validate_ssl
    }

    if (defined(genome_gff3)) {
        call ingest_gff3_into_ref {
            input:
                genome_id = genome_id,
                gff3_gz = select_first([gi.sorted_gff3_gz]),  # Coerce File? into File via select_first
                gff3_gz_tbi = select_first([gi.sorted_gff3_gz_tbi]),  # "
                reference_url = reference_url,
                token = access_token,
                validate_ssl = validate_ssl
        }
    }
}

task uncompress_fasta_and_generate_fai_if_needed {
    input {
        String genome_id
        File genome_fasta
    }

    command <<<
        if [[ '~{genome_fasta}' == *.gz ]]; then
            gunzip -c '~{genome_fasta}' > '~{genome_id}.fasta'
            rm '~{genome_fasta}'
        else
            mv '~{genome_fasta}' '~{genome_id}.fasta'
        fi
        samtools faidx '~{genome_id}.fasta' --fai-idx '~{genome_id}.fasta.fai'
    >>>

    output {
        File fasta = "${genome_id}.fasta"
        File fai = "${genome_id}.fasta.fai"
    }
}

# TODO: shared file with this task
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
            jq -r '.self_uri' <<< "${drs_res}"
        else
            exit "${exit_code}"
        fi
    >>>

    output {
        String drs_uri = read_string(stdout())
    }
}

# TODO: shared file with this task
task normalize_and_compress_gff3_and_index {
    input {
        String genome_id
        File gff3
    }

    command <<<
        if [[ '~{gff3}' == *.gz ]]; then
            zcat '~{gff3}' > unsorted.gff3
        else
            cp '~{gff3}' unsorted.gff3
        fi

        out_file='~{genome_id}_annotation.gff3.gz'

        # See http://www.htslib.org/doc/tabix.html#EXAMPLE
        #  - sort the GFF3 file
        (grep ^"#" unsorted.gff3; grep -v ^"#" unsorted.gff3 | sort -k1,1 -k4,4n) | bgzip -@ 2 > "${out_file}"
        tabix "${out_file}"
    >>>

    output {
        File sorted_gff3_gz = "${genome_id}_annotation.gff3.gz"
        File sorted_gff3_gz_tbi = "${genome_id}_annotation.gff3.gz.tbi"
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
        String? gff3_gz_drs_uri
        String? gff3_gz_tbi_drs_uri
        String reference_url
        String token
        Boolean validate_ssl
    }

    command <<<
        fasta-checksum-utils '~{fasta}' --fai '~{fai}' --genome-id '~{genome_id}' --out-format bento-json | \
            jq '.fasta = "~{fasta_drs_uri}" | .fai = "~{fai_drs_uri}" | .taxon = ~{taxon_term_json}' > metadata.json

        if [[ '~{gff3_gz_drs_uri}' != '' ]]; then  # assume if this is set then both gff3 variables are set.
            cat metadata.json | \
                jq '.gff3_gz = "~{gff3_gz_drs_uri}" | .gff_gz_tbi = "~{gff3_gz_tbi_drs_uri}"' > metadata.json.tmp
            mv metadata.json.tmp metadata.json
        fi

        rm '~{fasta}' '~{fai}'

        curl ~{true="" false="-k" validate_ssl} \
            -X POST \
            -H "Content-Type: application/json" \
            -H "Authorization: Bearer ~{token}" \
            --data "@metadata.json" \
            --fail-with-body \
            "~{reference_url}/genomes"
    >>>

    output {
        File out = stdout()
        File err = stderr()
    }
}

task ingest_gff3_into_ref {
    input {
        String genome_id
        File gff3_gz
        File gff3_gz_tbi
        String reference_url
        String token
        Boolean validate_ssl
    }

    command <<<
        curl ~{true="" false="-k" validate_ssl} \
            -X PUT \
            -F "gff3_gz=@~{gff3_gz}" \
            -F "gff3_gz_tbi=@~{gff3_gz_tbi}" \
            -H "Authorization: Bearer ~{token}" \
            --fail-with-body \
            "~{reference_url}/genomes/~{genome_id}/features.gff3.gz"
    >>>

    output {
        File out = stdout()
        File err = stderr()
    }
}
