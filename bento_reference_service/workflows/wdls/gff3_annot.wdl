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

    call ingest_gff3_into_ref {
        input:
            genome_id = genome_id,
            gff3_gz_drs_uri = drs_gff3.drs_uri,
            gff3_gz_tbi_drs_uri = drs_gff3_tbi.drs_uri,
            reference_url = reference_url,
            token = access_token,
            validate_ssl = validate_ssl
    }

    output {}
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

task ingest_gff3_into_ref {
    input {
        String genome_id
        String gff3_gz_drs_uri
        String gff3_gz_tbi_drs_uri
        String reference_url
        String token
        Boolean validate_ssl
    }

    command <<<
        patch_res=$(
            curl ~{true="" false="-k" validate_ssl} \
                -X PATCH \
                --json '{"gff3_gz": "~{gff3_gz_drs_uri}", "gff3_gz_tbi": "~{gff3_gz_tbi_drs_uri}"}' \
                -H "Authorization: Bearer ~{token}" \
                --fail-with-body \
                "~{reference_url}/genomes/~{genome_id}"
        )

        exit_code=$?
        if [[ "${exit_code}" != 0 ]]; then
            echo "patch failed with body: ${patch_res}" >&2
            exit "${exit_code}"
        fi

        task_res=$(
            curl ~{true="" false="-k" validate_ssl} \
                -X POST \
                --json '{"genome_id": "~{genome_id}", "kind": "ingest_features"}' \
                -H "Authorization: Bearer ~{token}" \
                --fail-with-body \
                "~{reference_url}/tasks"
        )
        exit_code=$?
        if [[ "${exit_code}" == 0 ]]; then
            echo "task created: ${task_res}"
            task_id=$(jq -r '.id' <<< "${task_res}")
            while true; do
                task_status_res=$(
                    curl ~{true="" false="-k" validate_ssl} \
                        -H "Authorization: Bearer ~{token}" \
                        --fail-with-body \
                        "~{reference_url}/tasks/${task_id}"
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
            echo "task creation failed: ${task_res}" >&2
            exit "${exit_code}"
        fi
    >>>

    output {
        File out = stdout()
        File err = stderr()
    }
}
