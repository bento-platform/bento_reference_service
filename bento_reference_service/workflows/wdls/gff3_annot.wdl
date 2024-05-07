version 1.0

workflow fasta_ref {
    input {
        String genome_id
        File genome_gff3
        String access_token
        String drs_url
        String reference_url
        Boolean validate_ssl
    }
}
