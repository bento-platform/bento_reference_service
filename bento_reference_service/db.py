import asyncpg
import json
from bento_lib.db.pg_async import PgAsyncDatabase
from fastapi import Depends
from functools import lru_cache
from pathlib import Path
from structlog.stdlib import BoundLogger
from typing import Annotated, AsyncIterator, Literal

from .config import Config, ConfigDependency
from .logger import LoggerDependency
from .models import (
    Alias,
    ContigWithRefgetURI,
    Genome,
    GenomeWithURIs,
    GenomeGFF3Patch,
    OntologyTerm,
    GenomeFeatureEntry,
    GenomeFeature,
    TaskStatus,
    Task,
)


SCHEMA_PATH = Path(__file__).parent / "sql" / "schema.sql"


class Database(PgAsyncDatabase):
    def __init__(self, config: Config, logger: BoundLogger):
        self._config: Config = config
        self.logger: BoundLogger = logger
        super().__init__(config.database_uri, SCHEMA_PATH)

    @staticmethod
    def deserialize_alias(rec: asyncpg.Record | dict) -> Alias:
        return Alias(alias=rec["alias"], naming_authority=rec["naming_authority"])

    def deserialize_contig(self, rec: asyncpg.Record | dict) -> ContigWithRefgetURI:
        service_base_url = self._config.service_url_base_path.rstrip("/")
        refget_uri_base = f"{service_base_url}/sequence"

        md5 = rec["md5_checksum"]
        ga4gh = rec["ga4gh_checksum"]

        aliases = rec["aliases"]
        if isinstance(aliases, str):
            aliases = json.loads(aliases)

        return ContigWithRefgetURI(
            name=rec["contig_name"],
            # aliases is [None] if no aliases defined:
            aliases=tuple(map(Database.deserialize_alias, aliases)) if aliases else (),
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
            aliases=tuple(map(Database.deserialize_alias, json.loads(rec["aliases"]))) if rec["aliases"] else (),
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

    async def _select_genomes(
        self,
        g_ids: list[str] | None,
        taxon_id: str | None = None,
        external_resource_uris: bool = False,
    ) -> AsyncIterator[GenomeWithURIs]:
        where_items: list[str] = []
        q_params: list[str | int] = []

        def _q_param(pv: str | int) -> str:
            q_params.append(pv)
            return f"${len(q_params)}"

        if g_ids:
            g_id_ors = " OR ".join(f"g.id = {_q_param(g_id)}" for g_id in g_ids)
            where_items.append(f"({g_id_ors})")

        if taxon_id:
            where_items.append(f"taxon_id = {_q_param(taxon_id)}")

        conn: asyncpg.Connection
        async with self.connect() as conn:
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
                    (
                        SELECT jsonb_agg(ga.*) FROM genome_aliases ga WHERE g.id = ga.genome_id
                    ) aliases,
                    (
                        WITH contigs_tmp AS (
                            SELECT
                                contig_name, contig_length, circular, md5_checksum, ga4gh_checksum,
                                (
                                    SELECT jsonb_agg(gca.*)
                                    FROM genome_contig_aliases gca
                                    WHERE g.id = gca.genome_id AND gc.contig_name = gca.contig_name
                                ) aliases
                            FROM genome_contigs gc WHERE g.id = gc.genome_id
                        )
                        SELECT jsonb_agg(contigs_tmp.*) FROM contigs_tmp
                    ) contigs
                FROM genomes g {("WHERE " + " AND ".join(where_items)) if where_items else ""}
                """,
                *q_params,
            )

        for r in map(lambda g: self.deserialize_genome(g, external_resource_uris), res):
            yield r

    async def get_genomes(
        self, g_ids: list[str] | None = None, taxon_id: str | None = None, external_resource_uris: bool = False
    ) -> tuple[GenomeWithURIs, ...]:
        return tuple([r async for r in self._select_genomes(g_ids, taxon_id, external_resource_uris)])

    async def get_genome(self, g_id: str, *, external_resource_uris: bool = False) -> GenomeWithURIs | None:
        return await anext(self._select_genomes([g_id], external_resource_uris=external_resource_uris), None)

    async def delete_genome(self, g_id: str) -> None:
        conn: asyncpg.Connection
        async with self.connect() as conn:
            await conn.execute("DELETE FROM genomes WHERE id = $1", g_id)

    async def get_genome_and_contig_by_checksum_str(
        self, checksum_str: str
    ) -> tuple[GenomeWithURIs, ContigWithRefgetURI] | None:
        # strip optional checksum prefixes if present:
        chk_norm: str = checksum_str.removeprefix("ga4gh:").removeprefix("md5:")

        conn: asyncpg.Connection
        async with self.connect() as conn:
            # TODO: these SQL statements could be optimized into one for performance reasons if it becomes necessary
            contig_res = await conn.fetchrow(
                """
                SELECT
                    genome_id, contig_name, contig_length, circular, md5_checksum, ga4gh_checksum,
                    (
                        SELECT jsonb_agg(gca.*)
                        FROM genome_contig_aliases gca
                        WHERE gc.genome_id = gca.genome_id AND gc.contig_name = gca.contig_name
                    ) aliases
                FROM genome_contigs gc
                WHERE md5_checksum = $1 OR ga4gh_checksum = $1
                """,
                chk_norm,
            )

        genome_res = (await anext(self._select_genomes([contig_res["genome_id"]]), None)) if contig_res else None
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

        await self.logger.adebug(f"Created genome: {g}")

        return await self.get_genome(g.id, external_resource_uris=return_external_resource_uris)

    async def update_genome(self, g_id: str, patch: GenomeGFF3Patch):
        conn: asyncpg.Connection
        async with self.connect() as conn:
            await conn.execute(
                "UPDATE genomes SET gff3_gz_uri = $2, gff3_gz_tbi_uri = $3 WHERE id = $1",
                g_id,
                patch.gff3_gz,
                patch.gff3_gz_tbi,
            )

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
            entries=list(map(Database.deserialize_genome_feature_entry, json.loads(rec["entries"] or "[]"))),
            gene_id=rec["gene_nat_id"],
            attributes=json.loads(rec["attributes"] or "{}"),
            parents=tuple(rec["parents"] or ()),  # tuple of parent IDs
        )

    @staticmethod
    def _feature_inner_entries_query(where_expr: str | None = None, gf_table_name: str = "gf") -> str:
        where_clause = f"AND {where_expr}" if where_expr else ""
        return f"""
        WITH entries_tmp AS (
            SELECT start_pos, end_pos, score, phase FROM genome_feature_entries gfe
            WHERE gfe.feature = {gf_table_name}.id {where_clause}
        ) 
        SELECT jsonb_agg(entries_tmp.*) FROM entries_tmp
        """

    async def get_genome_features_by_ids(
        self,
        g_id: str,
        f_ids: list[str],
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
            (SELECT ggf.feature_id FROM genome_features ggf WHERE ggf.id = gf.gene_id) gene_nat_id,
            ({self._feature_inner_entries_query()}) entries,
            (
                SELECT array_agg(gffp.feature_id) 
                    FROM genome_feature_parents gfp JOIN genome_features gffp ON gfp.parent = gffp.id
                WHERE gfp.feature = gf.id
            ) parents,
            (
                WITH attrs_tmp AS (
                    SELECT gfav.attr_key, array_agg(attr_val) attr_vals 
                    FROM genome_feature_attributes_view gfav
                    WHERE gfav.feature = gf.id
                    GROUP BY gfav.attr_key
                )
                SELECT jsonb_object_agg(attrs_tmp.attr_key, attrs_tmp.attr_vals) FROM attrs_tmp
            ) attributes
        FROM genome_features gf
        WHERE gf.genome_id = $1 AND feature_id = any($2::text[])
        """

        conn: asyncpg.Connection
        async with self.connect(existing_conn) as conn:
            final_res = await conn.fetch(final_query, g_id, f_ids)
        return [self.deserialize_genome_feature(r) for r in final_res]

    async def get_genome_feature_by_id(self, g_id: str, f_id: str) -> GenomeFeature | None:
        res = await self.get_genome_features_by_ids(g_id, [f_id])
        return res[0] if res else None

    async def query_genome_features(
        self,
        g_id: str,
        /,
        q: str | None = None,
        q_fzy: bool = False,
        name: str | None = None,
        name_fzy: bool = False,
        position: str | None = None,
        start: int | None = None,
        end: int | None = None,
        feature_types: list[str] | None = None,
        offset: int = 0,
        limit: int = 10,
    ) -> tuple[list[GenomeFeature], dict]:  # list of genome features + pagination dict object
        # TODO: refactor to use standard Bento search in the future, when Bento search makes more sense

        gf_select_items: list[str] = []
        gf_where_items: list[str] = []
        gf_order_items: list[str] = []
        gfe_where_items: list[str] = []
        q_params: list[str | int] = []

        def _q_param(pv: str | int) -> str:
            q_params.append(pv)
            return f"${len(q_params) + 3}"  # plus 3: g_id, offset, limit at start

        if q:
            query_param = _q_param(q)
            q_op = "%" if q_fzy else "~"
            gf_where_items.append(
                f"""
                gf.feature_id IN (
                    SELECT feature_id FROM (
                        SELECT 
                            feature_id,
                            feature_name,
                            feature_type
                        FROM genome_features gf_tmp_1
                        WHERE 
                            gf_tmp_1.genome_id = $1 AND (
                                gf_tmp_1.feature_id {q_op} {query_param}
                                OR gf_tmp_1.feature_name {q_op} {query_param}
                                OR EXISTS (
                                    SELECT attr_val FROM genome_feature_attributes_view gfav 
                                    WHERE gfav.feature = gf_tmp_1.id AND gfav.attr_val {q_op} {query_param}
                                )
                            )
                    ) gf_tmp_2
                )
            """
            )

        if name:
            param = _q_param(name)
            if name_fzy:
                gf_select_items.append(f"similarity(gf.feature_name, {param}) gf_fn_sml")
                gf_where_items.append(f"gf.feature_name % {param}")
                gf_order_items.append("gf_fn_sml DESC")
            else:
                gf_where_items.append(f"gf.feature_name = {param}")

        if position:
            gfe_where_items.append(f"gfe.position_text ILIKE {_q_param(position + '%')}")

        if start is not None:
            gfe_where_items.append(f"gfe.start_pos >= {_q_param(start)}")

        if end is not None:
            gfe_where_items.append(f"gfe.start_pos <= {_q_param(end)}")

        if feature_types:
            or_items = []
            for ft in feature_types:
                or_items.append(f"gf.feature_type = {_q_param(ft)}")
            gf_where_items.append(f"({' OR '.join(or_items)})")

        where_clause = " AND ".join(gf_where_items) if gf_where_items else "true"
        gfe_where_clause = " AND ".join(gfe_where_items) if gfe_where_items else None

        id_query = f"""
        SELECT feature_id {", " + ", ".join(gf_select_items) if gf_select_items else ""} FROM (
            SELECT 
                feature_id,
                feature_name,
                feature_type,
                ({self._feature_inner_entries_query(gfe_where_clause, "gf_tmp")}) entries
            FROM genome_features gf_tmp
            WHERE 
                gf_tmp.genome_id = $1
        ) gf
        WHERE 
            {"jsonb_array_length(gf.entries) > 0 AND" if gfe_where_clause else ""} 
            {where_clause}
        {"ORDER BY " + ", ".join(gf_order_items) if gf_order_items else ""}
        OFFSET $2 
        LIMIT  $3
        """

        offset = max(offset, 0)
        limit = min(max(limit, 0), self._config.feature_response_record_limit)

        conn: asyncpg.Connection
        async with self.connect() as conn:
            id_res = await conn.fetch(id_query, g_id, offset, limit, *q_params)
            final_list = await self.get_genome_features_by_ids(g_id, [r["feature_id"] for r in id_res], conn)

        return final_list, {"offset": offset, "limit": limit, "total": len(id_res)}

    async def clear_genome_features(self, g_id: str):
        conn: asyncpg.Connection
        async with self.connect() as conn:
            await conn.execute("DELETE FROM genome_features WHERE genome_id = $1", g_id)

    async def get_genome_feature_attribute_keys(
        self, existing_conn: asyncpg.Connection | None
    ) -> list[tuple[int, str]]:
        conn: asyncpg.Connection
        async with self.connect(existing_conn) as conn:
            res = await conn.fetch("SELECT id, attr_key FROM genome_feature_attribute_keys")
        return [(row["id"], row["attr_key"]) for row in res]

    async def get_genome_feature_attribute_values(
        self, existing_conn: asyncpg.Connection | None
    ) -> list[tuple[int, str]]:
        conn: asyncpg.Connection
        async with self.connect(existing_conn) as conn:
            res = await conn.fetch("SELECT id, attr_val FROM genome_feature_attribute_values")
        return [(row["id"], row["attr_val"]) for row in res]

    async def bulk_ingest_genome_features(self, features: tuple[GenomeFeature, ...]):
        # Manually generate sequential IDs
        # This requires an exclusive write lock on the database, so we don't get conflicting IDs

        conn: asyncpg.Connection
        async with self.connect() as conn:
            async with conn.transaction():
                fr = await conn.fetchrow("SELECT COALESCE(MAX(id), 0) + 1 AS next_id FROM genome_features")
                kr = await conn.fetchrow(
                    "SELECT COALESCE(MAX(id), 0) + 1 AS next_id FROM genome_feature_attribute_keys"
                )
                vr = await conn.fetchrow(
                    "SELECT COALESCE(MAX(id), 0) + 1 AS next_id FROM genome_feature_attribute_values"
                )

                assert fr
                assert kr
                assert vr

                # We generate a numeric ID for features to save space and improve lookup time.;
                current_feature_row_id: int = fr["next_id"]
                current_attr_key_id: int = kr["next_id"]
                current_attr_value_id: int = vr["next_id"]

                feature_row_ids: dict[str, int] = {}
                attr_key_ids: dict[str, int] = {t[1]: t[0] for t in await self.get_genome_feature_attribute_keys(conn)}
                new_attr_key_ids: dict[str, int] = {}
                attr_value_ids: dict[str, int] = {
                    t[1]: t[0] for t in await self.get_genome_feature_attribute_values(conn)
                }
                new_attr_value_ids: dict[str, int] = {}

                # ------------------------------------------------------------------------------------------------------

                feature_types: set[tuple[str]] = set()
                entries: list[tuple[int, int, int, str, float | None, int | None]] = []
                attributes: list[tuple[int, int, int]] = []
                parents: list[tuple[int, int]] = []
                feature_tuples: list[tuple[int, str, str, str, str, str, str, str, str | None]] = []

                for f in features:  # iter 1: populate row ID lookup dict
                    feature_row_ids[f.feature_id] = current_feature_row_id
                    current_feature_row_id += 1

                for feature in features:
                    feature_id = feature.feature_id

                    row_id = feature_row_ids[feature_id]
                    genome_id = feature.genome_id
                    contig_name = feature.contig_name

                    feature_types.add((feature.feature_type,))

                    e: GenomeFeatureEntry
                    entries.extend(
                        (
                            row_id,
                            e.start_pos,
                            e.end_pos,
                            f"{contig_name}:{e.start_pos}-{e.end_pos}",
                            e.score,
                            e.phase,
                        )
                        for e in feature.entries
                    )

                    # to reduce attribute storage, we deduplicate storage of keys and values by giving them integer IDs,
                    # and put these in the attributes table. we also have to put the key/value lookups into their
                    # respective tables.
                    for attr_key, attr_vals in feature.attributes.items():
                        if attr_key in attr_key_ids:
                            ak = attr_key_ids[attr_key]
                        elif attr_key in new_attr_key_ids:
                            ak = new_attr_key_ids[attr_key]
                        else:
                            ak = current_attr_key_id
                            new_attr_key_ids[attr_key] = current_attr_key_id
                            current_attr_key_id += 1

                        for attr_val in attr_vals:
                            if attr_val in attr_value_ids:
                                av = attr_value_ids[attr_val]
                            elif attr_val in new_attr_value_ids:
                                av = new_attr_value_ids[attr_val]
                            else:
                                av = current_attr_value_id
                                new_attr_value_ids[attr_val] = current_attr_value_id
                                current_attr_value_id += 1

                            attributes.append((row_id, ak, av))

                    for p in feature.parents:
                        try:
                            parents.append((row_id, feature_row_ids[p]))
                        except KeyError as e:
                            await self.logger.aerror(
                                f"Could not find parent row ID '{p}' for feature {feature.feature_id}"
                            )
                            raise e

                    feature_tuples.append(
                        (
                            row_id,
                            genome_id,
                            contig_name,
                            feature.strand,
                            feature_id,
                            feature.feature_name,
                            feature.feature_type,
                            feature.source,
                            feature_row_ids.get(feature.gene_id) if feature.gene_id else None,
                        )
                    )

                await self.logger.adebug(
                    f"bulk_ingest_genome_features: have {len(feature_types)} feature types for batch "
                    f"({[ft[0] for ft in feature_types][:20]})"
                )
                await conn.executemany(
                    "INSERT INTO genome_feature_types(type_id) VALUES ($1) ON CONFLICT DO NOTHING", feature_types
                )

                await self.logger.adebug(f"bulk_ingest_genome_features: have {len(feature_tuples)} features for batch")
                await conn.copy_records_to_table(
                    "genome_features",
                    columns=[
                        "id",
                        "genome_id",
                        "contig_name",
                        "strand",
                        "feature_id",
                        "feature_name",
                        "feature_type",
                        "source",
                        "gene_id",
                    ],
                    records=feature_tuples,
                )

                new_attribute_keys: list[tuple[int, str]] = [(ik, sk) for sk, ik in new_attr_key_ids.items()]
                await self.logger.adebug(
                    f"bulk_ingest_genome_features: have {len(new_attribute_keys)} new feature attribute keys for batch"
                )
                await conn.copy_records_to_table(
                    "genome_feature_attribute_keys", columns=["id", "attr_key"], records=new_attribute_keys
                )

                new_attribute_values: list[tuple[int, str]] = [(iv, sv) for sv, iv in new_attr_value_ids.items()]
                await self.logger.adebug(
                    f"bulk_ingest_genome_features: have {len(new_attribute_values)} new feature attribute values for "
                    f"batch"
                )
                await conn.copy_records_to_table(
                    "genome_feature_attribute_values", columns=["id", "attr_val"], records=new_attribute_values
                )

                await self.logger.adebug(
                    f"bulk_ingest_genome_features: have {len(attributes)} feature attribute records for batch"
                )
                await conn.copy_records_to_table(
                    "genome_feature_attributes",
                    columns=[
                        "feature",
                        "attr_key",
                        "attr_val",
                    ],
                    records=attributes,
                )

                await self.logger.adebug(f"bulk_ingest_genome_features: have {len(entries)} feature entries for batch")
                await conn.copy_records_to_table(
                    "genome_feature_entries",
                    columns=[
                        "feature",
                        "start_pos",
                        "end_pos",
                        "position_text",
                        "score",
                        "phase",
                    ],
                    records=entries,
                )

                await self.logger.adebug(
                    f"bulk_ingest_genome_features: have {len(parents)} feature parent records for batch"
                )
                await conn.copy_records_to_table(
                    "genome_feature_parents",
                    columns=[
                        "feature",
                        "parent",
                    ],
                    records=parents,
                )

    @staticmethod
    def deserialize_task(rec: asyncpg.Record | dict) -> Task:
        return Task(
            id=rec["id"],
            genome_id=rec["genome_id"],
            kind=rec["kind"],
            status=rec["status"],
            message=rec["message"],
            created=rec["created"],
        )

    async def get_task(self, t_id: int) -> Task | None:
        conn: asyncpg.Connection
        async with self.connect() as conn:
            res = await conn.fetchrow("SELECT * FROM tasks WHERE id = $1", t_id)
        return self.deserialize_task(res) if res is not None else None

    async def query_tasks(self, g_id: str | None, task_kind: Literal["ingest_features"] | None) -> tuple[Task, ...]:
        conn: asyncpg.Connection
        async with self.connect() as conn:
            where_clauses: list[str] = []
            params: list[int | str] = []

            if g_id is not None:
                where_clauses.append(f"genome_id = ${len(where_clauses) + 1}")
                params.append(g_id)

            if task_kind is not None:
                where_clauses.append(f"kind = ${len(where_clauses) + 1}::task_kind")
                params.append(task_kind)

            where_part = " AND ".join(where_clauses) if where_clauses else "true"

            res = await conn.fetch(f"SELECT * FROM tasks WHERE {where_part}", *params)
        return tuple(self.deserialize_task(r) for r in res)

    async def update_task_status(self, t_id: int, status: TaskStatus, message: str = ""):
        conn: asyncpg.Connection
        async with self.connect() as conn:
            await conn.execute(
                "UPDATE tasks SET status = $2::task_status, message = $3 WHERE id = $1", t_id, status, message
            )

    async def create_task(self, g_id: str, task_kind: Literal["ingest_features"]) -> int:
        conn: asyncpg.Connection
        async with self.connect() as conn:
            res = await conn.fetchrow(
                "INSERT INTO tasks (genome_id, kind) VALUES ($1, $2::task_kind) RETURNING id",
                g_id,
                task_kind,
            )
        assert res is not None
        return res["id"]

    async def move_running_tasks_to_error(self):
        update_q = """
        UPDATE tasks
        SET 
            status = 'error'::task_status, 
            message = 'This task had an invalid status at application startup: "' || $1 || '"'
        WHERE status = $1::task_status
        """

        conn: asyncpg.Connection
        async with self.connect() as conn:
            await conn.execute(update_q, "queued")
            await conn.execute(update_q, "running")


@lru_cache()
def get_db(config: ConfigDependency, logger: LoggerDependency) -> Database:  # pragma: no cover
    return Database(config, logger)


DatabaseDependency = Annotated[Database, Depends(get_db)]
