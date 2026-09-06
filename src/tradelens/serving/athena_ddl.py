"""Register gold tables in the Glue Data Catalog by running Athena DDL.

Reads sql/athena/create_tables.sql, expands ${VAR} placeholders (bucket name)
from the environment, splits it into individual statements, and submits each
one via the Athena Data API (boto3), polling until it succeeds or fails.

The gold tables are Delta, not plain Parquet — the DDL uses Athena engine v3's
native Delta Lake table type (TBLPROPERTIES ('table_type'='DELTA')), so there
is no column list here to keep in sync and no MSCK REPAIR TABLE step: Athena
reads schema and partitions straight from the Delta transaction log.
"""
from __future__ import annotations

import os
import re
import time

import boto3
from dotenv import load_dotenv

from tradelens.common.logging_utils import get_logger

logger = get_logger(__name__)

_ENV_PATTERN = re.compile(r"\$\{([A-Z0-9_]+)\}")
_POLL_SECONDS = 2
_TERMINAL_STATES = {"SUCCEEDED", "FAILED", "CANCELLED"}


def _expand_env(sql: str) -> str:
    return _ENV_PATTERN.sub(lambda m: os.environ[m.group(1)], sql)


def _split_statements(sql: str) -> list[str]:
    """Strip '--' comments and blank lines, then split on ';'."""
    lines = [line for line in sql.splitlines() if not line.strip().startswith("--")]
    statements = "\n".join(lines).split(";")
    return [s.strip() for s in statements if s.strip()]


def _run_statement(client, sql: str, workgroup: str, output_location: str) -> None:
    logger.info("Submitting Athena statement: %s", sql.split("\n")[0][:80])
    resp = client.start_query_execution(
        QueryString=sql,
        WorkGroup=workgroup,
        ResultConfiguration={"OutputLocation": output_location},
    )
    query_id = resp["QueryExecutionId"]

    while True:
        status = client.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]["Status"]
        state = status["State"]
        if state in _TERMINAL_STATES:
            break
        time.sleep(_POLL_SECONDS)

    if state != "SUCCEEDED":
        reason = status.get("StateChangeReason", "no reason given")
        raise RuntimeError(f"Athena statement failed ({state}): {reason}\nSQL: {sql}")
    logger.info("Statement succeeded (queryId=%s)", query_id)


def run_athena_ddl(sql_file: str = "sql/athena/create_tables.sql") -> None:
    """Submit every statement in `sql_file` to Athena and wait for each to finish.

    Reads TRADELENS_ATHENA_WORKGROUP, TRADELENS_ATHENA_OUTPUT and AWS_REGION
    from the environment (.env), plus whatever ${VAR} placeholders the SQL
    file itself references (TRADELENS_CURATED_BUCKET for the gold locations).
    """
    load_dotenv()
    workgroup = os.environ["TRADELENS_ATHENA_WORKGROUP"]
    output_location = os.environ["TRADELENS_ATHENA_OUTPUT"]
    region = os.environ.get("AWS_REGION")

    with open(sql_file, "r", encoding="utf-8") as fh:
        raw_sql = fh.read()
    sql = _expand_env(raw_sql)
    statements = _split_statements(sql)

    client = boto3.client("athena", region_name=region)
    for statement in statements:
        _run_statement(client, statement, workgroup, output_location)

    logger.info("Registered %d statement(s) from %s in workgroup %s", len(statements), sql_file, workgroup)


if __name__ == "__main__":
    run_athena_ddl()
