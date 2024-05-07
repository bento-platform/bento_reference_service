import asyncpg
import json
from bento_lib.db.pg_async import PgAsyncDatabase
from fastapi import Depends
from functools import lru_cache
from pathlib import Path
from typing import Annotated, AsyncIterator

from .config import Config, ConfigDependency
from .models import Alias, ContigWithRefgetURI, Genome, GenomeWithURIs, OntologyTerm


SCHEMA_PATH = Path(__file__).parent / "sql" / "schema.sql"


class Database(PgAsyncDatabase):
    def __init__(self, config: Config):
        self._config: Config = config
        super().__init__(config.database_uri, SCHEMA_PATH)

    @staticmethod
    def deserialize_alias(rec: asyncpg.Record | dict) -> Alias:
        return Alias(alias=rec["alias"], naming_authority=rec["naming_authority"])

    def deserialize_contig(self, rec: asyncpg.Record | dict) -> ContigWithRefgetURI:
        service_base_url = self._config.service_url_base_path.rstrip("/")
        refget_uri_base = f"{service_base_url}/sequence"
        md5 = rec["md5_checksum"]
        ga4gh = rec["ga4gh_checksum"]
        return ContigWithRefgetURI(
            name=rec["contig_name"],
            # aliases is [None] if no aliases defined:
            aliases=tuple(map(Database.deserialize_alias, filter(None, rec["aliases"]))),
            md5=md5,
            ga4gh=ga4gh,
            length=rec["contig_length"],
            circular=rec["circular"],
            refget_uris=(
                # Multiple synonymous URLs for accessing this contig
                f"{refget_uri_base}/{md5}",
                f"{refget_uri_base}/md5:{md5}",
                f"{refget_uri_base}/{ga4gh}",
                f"{refget_uri_base}/ga4gh:{ga4gh}",
            ),
        )

    def deserialize_genome(self, rec: asyncpg.Record, external_resource_uris: bool) -> GenomeWithURIs:
        service_base_url = self._config.service_url_base_path.rstrip("/")
        genome_uri = f"{service_base_url}/genomes/{rec['id']}"
        return GenomeWithURIs(
            id=rec["id"],
            # aliases is [None] if no aliases defined:
            aliases=tuple(map(Database.deserialize_alias, filter(None, rec["aliases"]))),
            uri=genome_uri,
            contigs=tuple(map(self.deserialize_contig, json.loads(rec["contigs"]))),
            md5=rec["md5_checksum"],
            ga4gh=rec["ga4gh_checksum"],
            fasta=f"{genome_uri}.fa" if external_resource_uris else rec["fasta_uri"],
            fai=f"{genome_uri}.fa.fai" if external_resource_uris else rec["fai_uri"],
            gff3_gz=(
                (f"{genome_uri}/features.gff3.gz" if external_resource_uris else rec["gff3_gz_uri"])
                if rec["gff3_gz_uri"]
                else None
            ),
            gff3_gz_tbi=(
                (f"{genome_uri}/features.gff3.gz.tbi" if external_resource_uris else rec["gff3_gz_tbi_uri"])
                if rec["gff3_gz_tbi_uri"]
                else None
            ),
            taxon=OntologyTerm(id=rec["taxon_id"], label=rec["taxon_label"]),
        )

    async def _select_genomes(self, g_id: str | None, external_resource_uris: bool) -> AsyncIterator[GenomeWithURIs]:
        conn: asyncpg.Connection
        async with self.connect() as conn:
            where_clause = "WHERE g.id = $1" if g_id is not None else ""
            res = await conn.fetch(
                f"""
                SELECT
                    id, md5_checksum, ga4gh_checksum, fasta_uri, fai_uri, taxon_id, taxon_label,
                    array(
                        SELECT json_agg(ga.*) FROM genome_aliases ga WHERE g.id = ga.genome_id
                    ) aliases,
                    (
                        WITH contigs_tmp AS (
                            SELECT
                                contig_name, contig_length, circular, md5_checksum, ga4gh_checksum,
                                array(
                                    SELECT json_agg(gca.*)
                                    FROM genome_contig_aliases gca
                                    WHERE g.id = gca.genome_id AND gc.contig_name = gca.contig_name
                                ) aliases
                            FROM genome_contigs gc WHERE g.id = gc.genome_id
                        )
                        SELECT json_agg(contigs_tmp.*) FROM contigs_tmp
                    ) contigs
                FROM genomes g {where_clause}
                """,
                *((g_id,) if g_id is not None else ()),
            )

            for r in map(lambda g: self.deserialize_genome(g, external_resource_uris), res):
                yield r

    async def get_genomes(self, external_resource_uris: bool = False) -> tuple[GenomeWithURIs, ...]:
        return tuple([r async for r in self._select_genomes(None, external_resource_uris)])

    async def get_genome(self, g_id: str, external_resource_uris: bool = False) -> GenomeWithURIs | None:
        return await anext(self._select_genomes(g_id, external_resource_uris), None)

    async def delete_genome(self, g_id: str) -> None:
        conn: asyncpg.Connection
        async with self.connect() as conn:
            await conn.execute("DELETE FROM genomes WHERE id = $1", g_id)

    async def get_genome_and_contig_by_checksum_str(
        self, checksum_str: str
    ) -> tuple[GenomeWithURIs, ContigWithRefgetURI] | None:
        chk_norm: str = checksum_str.rstrip("ga4gh:").rstrip("md5:")  # strip optional checksum prefixes if present
        conn: asyncpg.Connection
        async with self.connect() as conn:
            # TODO: these SQL statements could be optimized into one for performance reasons if it becomes necessary
            contig_res = await conn.fetchrow(
                "SELECT * FROM genome_contigs WHERE md5_checksum = $1 OR ga4gh_checksum = $1", chk_norm
            )
            genome_res = (
                (await anext(self._select_genomes(contig_res["genome_id"], False), None)) if contig_res else None
            )
            if genome_res is None or contig_res is None:
                return None
            return genome_res, self.deserialize_contig(contig_res)

    async def create_genome(self, g: Genome, return_external_resource_uris: bool) -> GenomeWithURIs | None:
        conn: asyncpg.Connection
        async with self.connect() as conn:
            async with conn.transaction():
                # Create the genome record:
                await conn.execute(
                    """
                    INSERT INTO genomes (
                        id, 
                        md5_checksum, 
                        ga4gh_checksum, 
                        fasta_uri, 
                        fai_uri, 
                        gff3_uri, 
                        gff3_tbi_uri, 
                        taxon_id, 
                        taxon_label
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    """,
                    g.id,
                    g.md5,
                    g.ga4gh,
                    g.fasta,
                    g.fai,
                    g.gff3,
                    g.gff3_tbi,
                    g.taxon.id,
                    g.taxon.label,
                )

                # Create records for each genome alias:
                if g.aliases:
                    await conn.executemany(
                        "INSERT INTO genome_aliases (genome_id, alias, naming_authority) VALUES ($1, $2, $3)",
                        tuple((g.id, alias.alias, alias.naming_authority) for alias in g.aliases),
                    )

                # Create records for each genome contig and all contig aliases:
                contig_tuples = []
                contig_alias_tuples = []
                for contig in g.contigs:
                    contig_tuples.append((g.id, contig.name, contig.length, contig.circular, contig.md5, contig.ga4gh))
                    for contig_alias in contig.aliases:
                        contig_alias_tuples.append(
                            (g.id, contig.name, contig_alias.alias, contig_alias.naming_authority)
                        )

                await conn.executemany(
                    "INSERT INTO genome_contigs "
                    "   (genome_id, contig_name, contig_length, circular, md5_checksum, ga4gh_checksum)"
                    "   VALUES ($1, $2, $3, $4, $5, $6)",
                    contig_tuples,
                )

                if contig_alias_tuples:
                    await conn.executemany(
                        "INSERT INTO genome_contig_aliases (genome_id, contig_name, alias, naming_authority) "
                        "VALUES ($1, $2, $3, $4)",
                        contig_alias_tuples,
                    )

        return await self.get_genome(g.id, external_resource_uris=return_external_resource_uris)


@lru_cache()
def get_db(config: ConfigDependency) -> Database:  # pragma: no cover
    return Database(config)


DatabaseDependency = Annotated[Database, Depends(get_db)]
