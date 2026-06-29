"""Airflow DAG template for SakunaGraPH source fetch/scrape jobs.

This file is intentionally a template: copy or symlink it into your Airflow
DAGs folder, then adjust the source configs as fetch modules are implemented.

Expected fetch module CLIs:
    python -m fetch.dromic --year 2026
    python -m fetch.ndrrmc --year 2026
    python -m fetch.emdat

The DAG runs from the etl/ directory so the repository's existing relative
paths such as ../data/raw/... and ../logs/... resolve correctly.
"""

from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Sequence

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.operators.python import PythonOperator


DAG_ID = "sakunagraph_fetch_sources"

# Prefer an Airflow Variable in production. The fallback supports local testing
# when this file remains inside etl/fetch/.
ETL_DIR = Path(
    Variable.get(
        "sakunagraph_etl_dir",
        default_var=os.environ.get(
            "SAKUNAGRAPH_ETL_DIR",
            str(Path(__file__).resolve().parents[1]),
        ),
    )
)

# Airflow Variable examples:
#   sakunagraph_fetch_year = 2026
#   sakunagraph_dromic_max_pages = 100
FETCH_YEAR = int(Variable.get("sakunagraph_fetch_year", default_var=datetime.now().year))
DROMIC_MAX_PAGES = int(Variable.get("sakunagraph_dromic_max_pages", default_var=100))


@dataclass(frozen=True)
class FetchSource:
    """Configuration for one source fetch command."""

    source_id: str
    module: str
    args: Sequence[str] = ()
    enabled: bool = True


FETCH_SOURCES: tuple[FetchSource, ...] = (
    FetchSource(
        source_id="dromic",
        module="fetch.dromic",
        args=("--year", str(FETCH_YEAR), "--max-pages", str(DROMIC_MAX_PAGES)),
    ),
    # TODO: Implement fetch/ndrrmc.py with a CLI, then set enabled=True.
    FetchSource(
        source_id="ndrrmc",
        module="fetch.ndrrmc",
        args=("--year", str(FETCH_YEAR)),
        enabled=False,
    ),
    # TODO: Implement fetch/emdat.py with a CLI, then set enabled=True.
    FetchSource(
        source_id="emdat",
        module="fetch.emdat",
        enabled=False,
    ),
)


def run_fetch_module(module: str, args: Sequence[str], enabled: bool) -> None:
    """Run one fetch module as a subprocess from the etl directory."""
    if not enabled:
        raise AirflowSkipException(f"{module} is not enabled yet.")

    command = ["python", "-m", module, *args]
    subprocess.run(command, cwd=ETL_DIR, check=True)


default_args = {
    "owner": "sakunagraph",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}


with DAG(
    dag_id=DAG_ID,
    description="Fetch or scrape raw disaster data sources for SakunaGraPH.",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="@weekly",
    catchup=False,
    max_active_runs=1,
    tags=("sakunagraph", "fetch", "scrape"),
) as dag:
    fetch_tasks = []

    for source in FETCH_SOURCES:
        task = PythonOperator(
            task_id=f"fetch_{source.source_id}",
            python_callable=run_fetch_module,
            op_kwargs={
                "module": source.module,
                "args": source.args,
                "enabled": source.enabled,
            },
        )
        fetch_tasks.append(task)

    # Sources are independent by default. If a future source depends on another,
    # add explicit dependencies here, for example:
    # fetch_tasks[0] >> fetch_tasks[1]
