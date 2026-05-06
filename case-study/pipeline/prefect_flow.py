"""Prefect orchestration for agriculture pipeline."""
from pathlib import Path

from prefect import flow, task

_PIPELINE = Path(__file__).resolve().parent


@task
def bronze_task() -> None:
    import sys

    sys.path.insert(0, str(_PIPELINE))
    from main import load_config, run

    run(load_config(), ["bronze"])


@task
def silver_task() -> None:
    import sys

    sys.path.insert(0, str(_PIPELINE))
    from main import load_config, run

    run(load_config(), ["silver"])


@task
def gold_task() -> None:
    import sys

    sys.path.insert(0, str(_PIPELINE))
    from main import load_config, run

    run(load_config(), ["gold"])


@flow(name="smart-ag-pipeline")
def ag_flow() -> None:
    bronze_task()
    silver_task()
    gold_task()


if __name__ == "__main__":
    ag_flow()
