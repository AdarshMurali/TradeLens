"""Register gold tables in the Glue Data Catalog for Athena.

For a resume project the simplest robust path is: run the pipeline, then execute
the DDL in sql/athena/create_tables.sql (via the Athena console or boto3). This
module optionally automates that with boto3 (athena start_query_execution).
"""
from __future__ import annotations

from tradelens.common.logging_utils import get_logger

logger = get_logger(__name__)


def run_athena_ddl(sql_file: str = "sql/athena/create_tables.sql") -> None:
    """TODO: read the SQL file and submit each statement via boto3 athena client,
    polling for completion. Use TRADELENS_ATHENA_WORKGROUP + _OUTPUT from env."""
    logger.info("TODO: submit %s to Athena", sql_file)
