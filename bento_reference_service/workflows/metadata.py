from pathlib import Path
from bento_lib.workflows import models as wm
from bento_lib.workflows.workflow_set import WorkflowSet

__all__ = ["workflow_set"]

WORKFLOW_FASTA_REFERENCE = "fasta_ref"

workflow_set = WorkflowSet(Path(__file__).parent / "wdls")

workflow_set.add_workflow(
    WORKFLOW_FASTA_REFERENCE,
    wm.WorkflowDefinition(
        name="Ingest FASTA-formatted reference genome",
        type="ingestion",
        description=(
            "Given a FASTA or gzipped FASTA reference genome, this workflow indexes and ingests it into the Bento "
            "Reference Service. All ingested FASTA files are COMPLETELY PUBLIC, so do not ingest any sensitive data!"
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
                id="genome_id", help="Standard unique ID for this genome; e.g., hg38, GRCH38, UrsMar_1.0"
            ),
            wm.WorkflowStringInput(
                id="taxon_term_json",
                help=(
                    "Phenopackets-style JSON representation for an NCBITaxon ontology term; for example: <br />"
                    '<code>{"id":"NCBITaxon:9606","label":"Homo sapiens"}</code> <br />'
                    '<code>{"id":"NCBITaxon:3847","label":"Glycine max"}</code> <br />'
                    '<code>{"id":"NCBITaxon:871304","label":"Lymantria dispar asiatica"}</code> <br />'
                    '<code>{"id":"NCBITaxon:7460","label":"Apis mellifera"}</code> <br />'
                    '<code>{"id":"NCBITaxon:29073","label":"Ursus maritimus"}</code>'
                ),
            ),  # NCBITaxon:#####
            wm.WorkflowFileInput(
                id="genome_fasta",
                pattern=r"^.*\.(fa|fa.gz|fas|fas.gz|fasta|fasta.gz)$",
                help="FASTA file for the reference genome, either gzipped or uncompressed.",
            ),
        ],
    ),
)
