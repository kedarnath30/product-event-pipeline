"""Import the Airflow DAG with a minimal stub of the airflow package (no Airflow install needed).
CI also imports it against real Airflow in a separate job."""
import importlib.util
import sys
import types
from pathlib import Path

DAG_FILE = Path(__file__).resolve().parents[1] / "airflow" / "dags" / "product_event_pipeline.py"


class _Op:
    _ctx = []

    def __init__(self, task_id, **kw):
        self.task_id, self.kw, self.downstream = task_id, kw, []
        if _Op._ctx:
            _Op._ctx[-1].tasks[task_id] = self

    def __rshift__(self, other):
        self.downstream.append(other)
        return other


class _DAG:
    def __init__(self, dag_id, **kw):
        self.dag_id, self.kw, self.tasks = dag_id, kw, {}

    def __enter__(self):
        _Op._ctx.append(self)
        return self

    def __exit__(self, *a):
        _Op._ctx.pop()


def _load(monkeypatch):
    airflow = types.ModuleType("airflow")
    airflow.DAG = _DAG
    ops = types.ModuleType("airflow.operators")
    bash = types.ModuleType("airflow.operators.bash")
    py = types.ModuleType("airflow.operators.python")
    bash.BashOperator = type("BashOperator", (_Op,), {})
    py.PythonOperator = type("PythonOperator", (_Op,), {})
    for name, mod in {"airflow": airflow, "airflow.operators": ops,
                      "airflow.operators.bash": bash, "airflow.operators.python": py}.items():
        monkeypatch.setitem(sys.modules, name, mod)
    spec = importlib.util.spec_from_file_location("product_event_pipeline_dag", DAG_FILE)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_dag_imports_and_wires_tasks(monkeypatch):
    mod = _load(monkeypatch)
    dag = mod.dag
    assert dag.dag_id == "product_event_pipeline"
    assert dag.kw["schedule"] == "@hourly" and dag.kw["max_active_runs"] == 1
    assert dag.kw["catchup"] is False
    assert list(dag.tasks) == ["ingest_micro_batches", "dbt_build", "dq_monitor"]
    t = dag.tasks
    assert t["ingest_micro_batches"].downstream == [t["dbt_build"]]
    assert t["dbt_build"].downstream == [t["dq_monitor"]]
    assert "dbt build --profiles-dir ." in t["dbt_build"].kw["bash_command"]
    assert "pipeline_as_of" in t["dbt_build"].kw["bash_command"]
    assert callable(t["ingest_micro_batches"].kw["python_callable"])
    assert dag.kw["default_args"]["retries"] >= 1
