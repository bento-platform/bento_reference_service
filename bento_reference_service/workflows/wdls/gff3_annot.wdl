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
