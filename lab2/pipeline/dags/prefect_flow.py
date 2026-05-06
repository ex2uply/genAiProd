"""Prefect DAG orchestrating medallion stages."""
from pathlib import Path

from prefect import flow, task

_PIPELINE_ROOT = Path(__file__).resolve().parent.parent


@task
def bronze_task() -> None:
    """Run bronze ingestion stage."""
    import sys

    sys.path.insert(0, str(_PIPELINE_ROOT))
    from main import execute

    execute(["bronze"])


@task
def silver_task() -> None:
    """Run silver cleaning stage."""
    import sys

    sys.path.insert(0, str(_PIPELINE_ROOT))
    from main import execute

    execute(["silver"])


@task
def gold_task() -> None:
    """Run gold aggregation stage."""
    import sys

    sys.path.insert(0, str(_PIPELINE_ROOT))
    from main import execute

    execute(["gold"])


@flow(name="healthcare-medallion")
def pipeline_flow() -> None:
    """Orchestrate the full medallion pipeline."""
    bronze_task()
    silver_task()
    gold_task()


if __name__ == "__main__":
    pipeline_flow()
