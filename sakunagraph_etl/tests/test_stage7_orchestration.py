from concurrent.futures import ThreadPoolExecutor
from datetime import date
import json
from pathlib import Path
import subprocess
import tempfile
import threading
import unittest
from unittest.mock import patch

from sakunagraph_etl.config import load_settings
from sakunagraph_etl.io import LocalFileStorage, record_artifact_run
from sakunagraph_etl.orchestration.catalog import WORKFLOWS
from sakunagraph_etl.orchestration.cli import (
    _completed_result,
    _write_uri,
    materialize_result_dependencies,
)
from sakunagraph_etl.orchestration.models import (
    TaskSpec,
    TaskStatus,
    WorkflowSpec,
)
from sakunagraph_etl.orchestration.observability import AlertSink
from sakunagraph_etl.orchestration.runner import (
    WorkflowBusyError,
    WorkflowRunner,
    backfill_dates,
)


def settings_for(root: Path):
    return load_settings(
        environ={
            "SAKUNA_REPOSITORY_ROOT": str(root),
            "SAKUNA_ETL_PROFILE": "local",
            "SAKUNA_DATA_ROOT": str(root / "data"),
            "SAKUNA_ARTIFACT_ROOT": str(root / "artifacts"),
            "SAKUNA_LOGS_ROOT": str(root / "logs"),
        }
    )


def artifact_envelope(root: Path, *, validation_status: str = "PASSED"):
    source = root / "input.xlsx"
    output = root / "output.ttl"
    source.write_bytes(b"source")
    output.write_text("@prefix : <https://sakuna.ph/> .\n", encoding="utf-8")
    storage = LocalFileStorage(root / "artifacts")
    result = record_artifact_run(
        "emdat",
        input_paths=(source,),
        output_paths=(output,),
        validation_status=validation_status,
        settings=settings_for(root),
        storage=storage,
    )
    manifest_path = storage.path_for(result.manifest_key)
    item = {
        "run_id": result.run_id,
        "pipeline": result.manifest.pipeline,
        "status": result.manifest.status,
        "manifest_key": result.manifest_key,
        "manifest_uri": str(manifest_path),
        "manifest_path": str(manifest_path),
        "validation_statuses": [validation_status],
    }
    return result, item


class Stage7WorkflowTests(unittest.TestCase):
    def test_catalog_uses_cli_tasks_and_orders_alignment_before_publish(self) -> None:
        self.assertEqual(
            set(WORKFLOWS),
            {
                "source-emdat",
                "source-gda",
                "source-psgc",
                "source-ndrrmc",
                "source-dromic",
                "integration",
            },
        )
        integration = WORKFLOWS["integration"]
        self.assertEqual([task.task_id for task in integration.tasks], ["align", "publish"])
        self.assertEqual(integration.tasks[1].depends_on, ("align",))
        self.assertEqual(integration.tasks[0].required_manifest_parameters, ("source_manifests",))
        self.assertEqual(integration.max_active_runs, 1)
        for workflow in WORKFLOWS.values():
            for task in workflow.tasks:
                self.assertNotEqual(task.command[0], "python")

    def test_artifact_boundary_emits_a_small_retry_stable_envelope(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            result_file = root / "task-result.json"
            with patch.dict("os.environ", {"SAKUNA_TASK_RESULT_FILE": str(result_file)}):
                result, _ = artifact_envelope(root)
                storage = LocalFileStorage(root / "artifacts")
                source = root / "input.xlsx"
                output = root / "output.ttl"
                retry = record_artifact_run(
                    "emdat",
                    input_paths=(source,),
                    output_paths=(output,),
                    validation_status="PASSED",
                    settings=settings_for(root),
                    storage=storage,
                )

            payload = json.loads(result_file.read_text(encoding="utf-8"))
            self.assertEqual(result.run_id, retry.run_id)
            self.assertEqual(len(payload["artifacts"]), 1)
            self.assertLess(result_file.stat().st_size, 4096)
            self.assertEqual(payload["artifacts"][0]["status"], "COMPLETED")

    def test_retry_then_resume_does_not_repeat_completed_immutable_work(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            _, item = artifact_envelope(root)
            calls = 0

            def executor(command, timeout, environment):
                nonlocal calls
                calls += 1
                if calls == 1:
                    return subprocess.CompletedProcess(command, 12, "", "transient")
                result_path = Path(environment["SAKUNA_TASK_RESULT_FILE"])
                result_path.parent.mkdir(parents=True, exist_ok=True)
                result_path.write_text(
                    json.dumps({"schema_version": 1, "artifacts": [item]}),
                    encoding="utf-8",
                )
                return subprocess.CompletedProcess(command, 0, "done", "")

            workflow = WorkflowSpec(
                name="retry-resume",
                description="test",
                tasks=(
                    TaskSpec(
                        task_id="source",
                        command=("emdat", "--out", "unused.ttl"),
                        retries=1,
                        retry_delay_seconds=0,
                    ),
                ),
            )
            runner = WorkflowRunner(
                settings_for(root),
                state_root=root / "state",
                command_executor=executor,
                sleeper=lambda _: None,
            )
            first = runner.run(workflow, parameters={}, run_id="stable-run")
            resumed = runner.run(workflow, parameters={}, run_id="stable-run")

            self.assertEqual(first.status, TaskStatus.COMPLETED)
            self.assertEqual(first.tasks["source"].attempts, 2)
            self.assertEqual(resumed.status, TaskStatus.COMPLETED)
            self.assertEqual(calls, 2)
            self.assertTrue((root / "logs" / "metrics" / "sakunagraph.prom").is_file())
            lineage = root / "logs" / "lineage" / "openlineage.jsonl"
            self.assertEqual(json.loads(lineage.read_text().splitlines()[0])["eventType"], "COMPLETE")

    def test_failed_validation_stops_before_alignment_and_is_alerted(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            result, _ = artifact_envelope(root, validation_status="FAILED")
            manifest = LocalFileStorage(root / "artifacts").path_for(result.manifest_key)
            called = False

            def executor(command, timeout, environment):
                nonlocal called
                called = True
                return subprocess.CompletedProcess(command, 0, "", "")

            workflow = WorkflowSpec(
                name="validation-gate",
                description="test",
                required_parameters=("source_manifests",),
                tasks=(
                    TaskSpec(
                        task_id="align",
                        command=("align",),
                        required_manifest_parameters=("source_manifests",),
                    ),
                    TaskSpec(
                        task_id="publish",
                        command=("load-graphdb",),
                        depends_on=("align",),
                        produces_artifacts=False,
                    ),
                ),
            )
            state = WorkflowRunner(
                settings_for(root),
                state_root=root / "state",
                command_executor=executor,
            ).run(workflow, parameters={"source_manifests": str(manifest)})

            self.assertEqual(state.status, TaskStatus.FAILED)
            self.assertFalse(called)
            self.assertNotIn("publish", state.tasks)
            self.assertIn("non-publishable", state.tasks["align"].error or "")
            alert = json.loads(
                (root / "logs" / "alerts" / "workflow-alerts.jsonl")
                .read_text(encoding="utf-8")
                .splitlines()[0]
            )
            self.assertEqual(alert["severity"], "critical")
            self.assertEqual(alert["task_id"], "align")

    def test_timeout_is_bounded_and_retried_only_as_configured(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            calls = 0
            delays = []

            def executor(command, timeout, environment):
                nonlocal calls
                calls += 1
                raise subprocess.TimeoutExpired(command, timeout)

            workflow = WorkflowSpec(
                name="timeout",
                description="test",
                tasks=(
                    TaskSpec(
                        task_id="bounded",
                        command=("emdat",),
                        retries=1,
                        retry_delay_seconds=3,
                        timeout_seconds=9,
                    ),
                ),
            )
            state = WorkflowRunner(
                settings_for(root),
                state_root=root / "state",
                command_executor=executor,
                sleeper=delays.append,
            ).run(workflow, parameters={})

            self.assertEqual(state.status, TaskStatus.FAILED)
            self.assertEqual(state.tasks["bounded"].attempts, 2)
            self.assertEqual(state.tasks["bounded"].error, "task exceeded timeout of 9 seconds")
            self.assertEqual(calls, 2)
            self.assertEqual(delays, [3])

    def test_workflow_concurrency_lock_rejects_a_second_active_run(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            _, item = artifact_envelope(root)
            entered = threading.Event()
            release = threading.Event()

            def executor(command, timeout, environment):
                entered.set()
                release.wait(timeout=5)
                path = Path(environment["SAKUNA_TASK_RESULT_FILE"])
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(json.dumps({"artifacts": [item]}), encoding="utf-8")
                return subprocess.CompletedProcess(command, 0, "", "")

            workflow = WorkflowSpec(
                name="exclusive",
                description="test",
                tasks=(TaskSpec(task_id="one", command=("emdat",)),),
            )
            runner_one = WorkflowRunner(
                settings_for(root), state_root=root / "state", command_executor=executor
            )
            runner_two = WorkflowRunner(
                settings_for(root), state_root=root / "state", command_executor=executor
            )
            with ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(
                    runner_one.run, workflow, parameters={}, run_id="first"
                )
                self.assertTrue(entered.wait(timeout=2))
                with self.assertRaises(WorkflowBusyError):
                    runner_two.run(workflow, parameters={}, run_id="second")
                release.set()
                self.assertEqual(future.result(timeout=5).status, TaskStatus.COMPLETED)

    def test_backfill_result_pointer_and_dependency_materialization(self) -> None:
        self.assertEqual(
            backfill_dates(date(2026, 7, 1), date(2026, 7, 3)),
            (date(2026, 7, 1), date(2026, 7, 2), date(2026, 7, 3)),
        )
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            _, item = artifact_envelope(root)
            result_uri = str(root / "result.json")
            _write_uri(
                result_uri,
                json.dumps({"status": "FAILED", "artifacts": [item]}).encode(),
            )
            self.assertFalse(_completed_result(result_uri))
            _write_uri(
                result_uri,
                json.dumps({"status": "COMPLETED", "artifacts": [item]}).encode(),
            )
            self.assertTrue(_completed_result(result_uri))
            manifests = materialize_result_dependencies(
                {"source": result_uri}, root / "dependencies"
            )
            adapted = json.loads(manifests["source:0"].read_text(encoding="utf-8"))
            output = next(a for a in adapted["artifacts"] if a["role"] == "output")
            self.assertTrue(Path(output["original_path"]).is_file())
            stored_manifest = json.loads(Path(item["manifest_path"]).read_text(encoding="utf-8"))
            stored_output = next(
                artifact for artifact in stored_manifest["artifacts"]
                if artifact["role"] == "output"
            )
            (root / "artifacts" / stored_output["path"]).write_bytes(b"tampered")
            self.assertFalse(_completed_result(result_uri))

    def test_webhook_failure_does_not_hide_durable_alert(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "alerts.jsonl"
            sink = AlertSink(path, webhook_url="https://alerts.invalid")
            with patch(
                "sakunagraph_etl.orchestration.observability.requests.post",
                side_effect=__import__("requests").ConnectionError("offline"),
            ):
                sink.send({"workflow": "test", "error": "failure"})
            self.assertEqual(json.loads(path.read_text())["error"], "failure")


if __name__ == "__main__":
    unittest.main()
