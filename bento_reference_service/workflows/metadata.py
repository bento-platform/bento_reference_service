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
        file="fasta_ref",
        tags=frozenset(("reference", "fasta")),
        inputs=[
            # Injected
            wm.WorkflowSecretInput(id="access_token", key="access_token"),
            wm.WorkflowServiceUrlInput(id="drs_url", service_kind="drs"),
            wm.WorkflowServiceUrlInput(id="reference_url", service_kind="reference"),
            # User-specified
            wm.WorkflowStringInput(id="genome_id"),
            wm.WorkflowFileInput(id="genome_fasta", pattern=r"^.*\.(fa|fa.gz|fas|fas.gz|fasta|fasta.gz)$"),
        ],
    ),
)
