import asyncpg
import json
import logging
from bento_lib.db.pg_async import PgAsyncDatabase
from fastapi import Depends
from functools import lru_cache
from pathlib import Path
from typing import Annotated, AsyncIterator, Iterable

from .config import Config, ConfigDependency
from .models import Alias, ContigWithRefgetURI, Genome, GenomeWithURIs, OntologyTerm, GenomeFeatureEntry, GenomeFeature


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
                    id, 
                    md5_checksum, 
                    ga4gh_checksum, 
                    fasta_uri, 
                    fai_uri, 
                    gff3_gz_uri, 
                    gff3_gz_tbi_uri, 
                    taxon_id, 
                    taxon_label,
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
                        gff3_gz_uri, 
                        gff3_gz_tbi_uri, 
                        taxon_id, 
                        taxon_label
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    """,
                    g.id,
                    g.md5,
                    g.ga4gh,
                    g.fasta,
                    g.fai,
                    g.gff3_gz,
                    g.gff3_gz_tbi,
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

    async def genome_feature_types_summary(self, g_id: str):
        conn: asyncpg.Connection
        async with self.connect() as conn:
            res = await conn.fetch(
                """
                SELECT feature_type, COUNT(feature_type) as ft_count
                FROM genome_features 
                WHERE genome_id = $1 
                GROUP BY feature_type
            """,
                g_id,
            )

        return {row["feature_type"]: row["ft_count"] for row in res}

    @staticmethod
    def deserialize_genome_feature_entry(rec: asyncpg.Record | dict) -> GenomeFeatureEntry:
        return GenomeFeatureEntry(
            start_pos=rec["start_pos"],
            end_pos=rec["end_pos"],
            score=rec["score"],
            phase=rec["phase"],
        )

    @staticmethod
    def deserialize_genome_feature(rec: asyncpg.Record) -> GenomeFeature:
        return GenomeFeature(
            genome_id=rec["genome_id"],
            contig_name=rec["contig_name"],
            strand=rec["strand"],
            feature_id=rec["feature_id"],
            feature_name=rec["feature_name"],
            feature_type=rec["feature_type"],
            source=rec["source"],
            entries=tuple(map(Database.deserialize_genome_feature_entry, json.loads(rec["entries"]))),
            annotations=json.loads(rec["annotations"]),  # TODO
            parents=tuple(rec["parents"]),  # tuple of parent IDs
        )

    @staticmethod
    def _feature_inner_entries_query(where_expr: str | None = None) -> str:
        where_clause = f"AND {where_expr}" if where_expr else ""
        return f"""
        WITH entries_tmp AS (
            SELECT start_pos, end_pos, score, phase FROM genome_feature_entries gfe
            WHERE gfe.genome_id = gf.genome_id AND gfe.feature_id = gf.feature_id {where_clause}
        ) 
        SELECT jsonb_agg(entries_tmp.*) FROM entries_tmp
        """

    async def get_genome_features_by_ids(
        self,
        g_id: str,
        f_ids: list[str],
        offset: int = 0,
        limit: int = 10,
        existing_conn: asyncpg.Connection | None = None,
    ):
        final_query = f"""
        SELECT
            genome_id,
            contig_name,
            strand,
            feature_id,
            feature_name,
            feature_type,
            source,
            ({self._feature_inner_entries_query()}) entries,
            (
                SELECT array_agg(gfp.parent_id) FROM genome_feature_parents gfp 
                WHERE gfp.genome_id = gf.genome_id AND gfp.feature_id = gf.feature_id
            ) parents
        FROM genome_features gf
        WHERE gf.genome_id = $1 AND feature_id IN $2
        OFFSET $3 LIMIT $4
        """

        conn: asyncpg.Connection
        async with self.connect(existing_conn) as conn:
            final_res = await conn.fetch(final_query, g_id, f_ids, offset, limit)
            return [self.deserialize_genome_feature(r) for r in final_res]

    async def get_genome_feature_by_id(self, g_id: str, f_id: str) -> GenomeFeature | None:
        res = await self.get_genome_features_by_ids(g_id, [f_id], 0, 1)
        return res[0] if res else None

    async def query_genome_features(
        self,
        g_id: str,
        q: str | None,
        name: str | None,
        position: str | None,
        start: int | None,
        end: int | None,
        feature_types: list[str] | None,
        offset: int = 0,
        limit: int = 10,
    ) -> tuple[list[GenomeFeature], dict]:  # list of genome features + pagination dict object
        gf_where_items: list[str] = []
        gfe_where_items: list[str] = []
        q_params: list[str | int] = []

        def _q_param(pv: str | int) -> str:
            q_params.append(pv)
            return f"${len(gf_where_items) + 2}"

        if q:
            param = _q_param(q)
            gf_where_items.append(f"(gf.feature_id ~ {param} OR gf.feature_name ~ {param})")

        if name:
            gf_where_items.append(f"gf.feature_name = {_q_param(name)}")

        if position:
            gfe_where_items.append(f"gfe.position_text ~ {_q_param(position)}")

        if start is not None:
            gfe_where_items.append(f"gfe.start_pos >= {_q_param(start)}")

        if end is not None:
            gfe_where_items.append(f"gfe.start_pos <= {_q_param(end)}")

        if feature_types:
            or_items = []
            for ft in feature_types:
                gf_where_items.append(f"gf.feature_type = f{_q_param(ft)}")
            gf_where_items.append(f"({' OR '.join(or_items)})")

        where_clause = " AND ".join(gf_where_items) if gf_where_items else "true"
        gfe_where_clause = " AND ".join(gfe_where_items) if gfe_where_items else None

        id_query = f"""
        SELECT feature_id, ({self._feature_inner_entries_query(gfe_where_clause)}) entries 
        FROM genome_features gf 
        WHERE 
            gf.genome_id = $1
            AND jsonb_array_length(gf.entries) > 0
            AND {where_clause};
        """

        conn: asyncpg.Connection
        async with self.connect() as conn:
            id_res = await conn.fetch(id_query, g_id, *q_params)
            final_list = await self.get_genome_features_by_ids(
                g_id, [r["feature_id"] for r in id_res], offset, limit, conn
            )

        return final_list, {"offset": offset, "limit": limit, "total": len(id_res)}

    async def clear_genome_features(self, g_id: str):
        conn: asyncpg.Connection
        async with self.connect() as conn:
            await conn.execute("DELETE FROM genome_feature_attributes WHERE genome_id = $1", g_id)
            await conn.execute("DELETE FROM genome_feature_entries WHERE genome_id = $1", g_id)
            await conn.execute("DELETE FROM genome_feature_parents WHERE genome_id = $1", g_id)
            await conn.execute("DELETE FROM genome_features WHERE genome_id = $1", g_id)

    async def bulk_ingest_genome_features(self, features: Iterable[GenomeFeature], logger: logging.Logger):
        feature_types: list[tuple[str]] = []
        entries: list[tuple[str, str, int, int, str, float | None, int | None]] = []
        attributes: list[tuple[str, str, str, str]] = []
        parents: list[tuple[str, str, str]] = []
        feature_tuples: list[tuple[str, str, str, str, str, str, str]] = []

        for feature in features:
            genome_id = feature.genome_id
            contig_name = feature.contig_name
            feature_id = feature.feature_id

            feature_types.append((feature.feature_type,))

            entries.extend(
                (
                    genome_id,
                    feature_id,
                    e.start_pos,
                    e.end_pos,
                    f"{contig_name}:{e.start_pos}-{e.end_pos}",
                    e.score,
                    e.phase,
                )
                for e in feature.entries
            )

            for attr_tag, attr_vals in feature.attributes.items():
                attributes.extend((genome_id, feature_id, attr_tag, attr_val) for attr_val in attr_vals)

            parents.extend((genome_id, feature_id, p) for p in feature.parents)

            feature_tuples.append(
                (
                    genome_id,
                    contig_name,
                    feature.strand,
                    feature_id,
                    feature.feature_name,
                    feature.feature_type,
                    feature.source,
                )
            )

        conn: asyncpg.Connection
        async with self.connect() as conn:
            async with conn.transaction():
                logger.debug(f"bulk_ingest_genome_features: have {len(feature_types)} feature types for batch")
                await conn.executemany(
                    "INSERT INTO genome_feature_types(type_id) VALUES ($1) ON CONFLICT DO NOTHING", feature_types
                )

                logger.debug(f"bulk_ingest_genome_features: have {len(feature_tuples)} features for batch")
                await conn.copy_records_to_table(
                    "genome_features",
                    columns=[
                        "genome_id",
                        "contig_name",
                        "strand",
                        "feature_id",
                        "feature_name",
                        "feature_type",
                        "source",
                    ],
                    records=feature_tuples,
                )

                logger.debug(f"bulk_ingest_genome_features: have {len(attributes)} feature attribute records for batch")
                await conn.copy_records_to_table(
                    "genome_feature_attributes",
                    columns=[
                        "genome_id",
                        "feature_id",
                        "attr_tag",
                        "attr_val",
                    ],
                    records=attributes,
                )

                logger.debug(f"bulk_ingest_genome_features: have {len(entries)} feature entries for batch")
                await conn.copy_records_to_table(
                    "genome_feature_entries",
                    columns=[
                        "genome_id",
                        "feature_id",
                        "start_pos",
                        "end_pos",
                        "position_text",
                        "score",
                        "phase",
                    ],
                    records=entries,
                )

                logger.debug(f"bulk_ingest_genome_features: have {len(parents)} feature parent records for batch")
                await conn.copy_records_to_table(
                    "genome_feature_parents",
                    columns=[
                        "genome_id",
                        "feature_id",
                        "parent_id",
                    ],
                    records=parents,
                )


@lru_cache()
def get_db(config: ConfigDependency) -> Database:  # pragma: no cover
    return Database(config)


DatabaseDependency = Annotated[Database, Depends(get_db)]
