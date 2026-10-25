"""Import the DAG with real Airflow and fail on any import error (used in CI)."""
import sys
from pathlib import Path

from airflow.models import DagBag

dags = Path(__file__).resolve().parents[1] / "airflow" / "dags"
bag = DagBag(dag_folder=str(dags), include_examples=False)
if bag.import_errors:
    print(bag.import_errors)
    sys.exit(1)
dag = bag.get_dag("product_event_pipeline")
assert dag is not None, "DAG not found"
print("tasks:", [t.task_id for t in dag.tasks])
