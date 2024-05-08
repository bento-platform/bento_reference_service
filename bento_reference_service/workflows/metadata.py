from pathlib import Path
from bento_lib.workflows import models as wm
from bento_lib.workflows.workflow_set import WorkflowSet

__all__ = ["workflow_set"]

WORKFLOW_FASTA_REFERENCE = "fasta_ref"
WORKFLOW_GFF3_ANNOTATION = "gff3_annot"

workflow_set = WorkflowSet(Path(__file__).parent / "wdls")

workflow_set.add_workflow(
    WORKFLOW_FASTA_REFERENCE,
    wm.WorkflowDefinition(
        name="Ingest FASTA-formatted reference genome",
        type="ingestion",
        description=(
            "Given a FASTA or gzipped FASTA reference genome, and optionally a GFF3-formatted annotation file, this "
            "workflow indexes and ingests it into the Bento Reference Service. All ingested FASTA files are COMPLETELY "
            "PUBLIC, so do not ingest any sensitive data!"
        ),
        file="fasta_ref.wdl",
        tags=frozenset(("reference", "fasta")),
        inputs=[
            # Injected
            wm.WorkflowSecretInput(id="access_token", key="access_token"),
            wm.WorkflowServiceUrlInput(id="drs_url", service_kind="drs"),
            wm.WorkflowServiceUrlInput(id="reference_url", service_kind="reference"),
            wm.WorkflowConfigInput(id="validate_ssl", key="validate_ssl"),
            # User-specified
            wm.WorkflowStringInput(
                id="genome_id", help="Standard unique ID for this genome; e.g., hg38, GRCh38, UrsMar_1.0"
            ),
            wm.WorkflowStringInput(
                id="taxon_term_json",
                help=(
                    "Phenopackets-style JSON representation for an NCBITaxon ontology term; for example: <br />"
                    '<code>{"id":"NCBITaxon:9606","label":"Homo sapiens"}</code> <br />'
                    '<code>{"id":"NCBITaxon:3847","label":"Glycine max"}</code> <br />'
                    '<code>{"id":"NCBITaxon:871304","label":"Lymantria dispar asiatica"}</code> <br />'
                    '<code>{"id":"NCBITaxon:86327","label":"Rangifer tarandus caribou"}</code> <br />'
                    '<code>{"id":"NCBITaxon:7460","label":"Apis mellifera"}</code> <br />'
                    '<code>{"id":"NCBITaxon:29073","label":"Ursus maritimus"}</code>'
                ),
            ),  # NCBITaxon:#####
            wm.WorkflowFileInput(
                id="genome_fasta",
                pattern=r"^.*\.(fa|fa.gz|fna|fna.gz|fas|fas.gz|fasta|fasta.gz)$",
                help="FASTA file for the reference genome, either gzipped or uncompressed.",
            ),
            wm.WorkflowFileInput(
                id="genome_gff3",
                pattern=r"^.*\.(gff|gff3)$",
                required=False,
                help="GFF3-formatted annotation file for the reference genome.",
            ),
        ],
    ),
)

workflow_set.add_workflow(
    WORKFLOW_GFF3_ANNOTATION,
    wm.WorkflowDefinition(
        name="Add GFF3-formatted annotation data to a reference genome",
        type="ingestion",
        description=(
            "Given a GFF3-formatted annotation file, extract the features to make them queryable and attach them to an "
            "existing reference genome."
        ),
        file="gff3_annot.wdl",
        tags=frozenset(("reference", "gff3")),
        inputs=[
            # Injected
            wm.WorkflowSecretInput(id="access_token", key="access_token"),
            wm.WorkflowServiceUrlInput(id="drs_url", service_kind="drs"),
            wm.WorkflowServiceUrlInput(id="reference_url", service_kind="reference"),
            wm.WorkflowConfigInput(id="validate_ssl", key="validate_ssl"),
            # User-specified
            wm.WorkflowEnumInput(
                id="genome_id",
                values="{{ serviceUrls.reference }}/genomes?response_format=id_list",
                help="The reference genome to annotate with the GFF3 file.",
            ),
            wm.WorkflowFileInput(
                id="genome_gff3",
                pattern=r"^.*\.(gff|gff3|gff.gz|gff3.gz)$",
                help="GFF3-formatted annotation file for the reference genome.",
            ),
        ],
    ),
)
