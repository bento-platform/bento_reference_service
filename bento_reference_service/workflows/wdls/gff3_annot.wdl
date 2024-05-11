version 1.0

workflow gff3_annot {
    input {
        String genome_id
        File genome_gff3
        String access_token
        String drs_url
        String reference_url
        Boolean validate_ssl
    }

    call normalize_and_compress_gff3_and_index as gi {
        input:
            genome_id = genome_id,
            gff3 = genome_gff3
    }

    # TODO: DRS ingestion + updating reference metadata record

    call ingest_gff3_into_ref {
        input:
            genome_id = genome_id,
            gff3_gz = gi.sorted_gff3_gz,
            gff3_gz_tbi = gi.sorted_gff3_gz_tbi,
            reference_url = reference_url,
            token = access_token,
            validate_ssl = validate_ssl
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
        task_res=$(
            curl ~{true="" false="-k" validate_ssl} \
                -X PUT \
                -F "gff3_gz=@~{gff3_gz}" \
                -F "gff3_gz_tbi=@~{gff3_gz_tbi}" \
                -H "Authorization: Bearer ~{token}" \
                --fail-with-body \
                "~{reference_url}/genomes/~{genome_id}/features.gff3.gz"
        )
        exit_code=$?
        if [[ "${exit_code}" == 0 ]]; then
            task_url=$(jq -r '.task' <<< "${task_res}")
            while true; do
                task_status_res=$(
                    curl ~{true="" false="-k" validate_ssl} \
                        -H "Authorization: Bearer ~{token}" \
                        --fail-with-body \
                        "${task_url}"
                )

                task_exit_code=$?
                if [[ "${task_exit_code}" != 0 ]]; then
                    echo "task status response returned non-success status code" >&2
                    exit 1
                fi

                task_status=$(jq -r '.status' <<< "${task_status_res}")
                task_message=$(jq -r '.message' <<< "${task_status_res}")

                if [[ "${task_status}" == 'success' ]]; then
                    echo "task succeeded with message: ${task_message}"
                    break  # success
                fi
                if [[ "${task_status}" == 'error' ]]; then
                    echo "task failed with message: ${task_message}" >&2
                    exit 1
                fi

                # otherwise, running - wait
                sleep 10
            done
        else
            exit "${exit_code}"
        fi
    >>>

    output {
        File out = stdout()
        File err = stderr()
    }
}
