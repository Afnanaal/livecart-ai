from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

from openlineage.client import OpenLineageClient
from openlineage.client.run import Job, Run, RunEvent, RunState


PROJECT_ROOT = Path(__file__).resolve().parents[2]
LINEAGE_DIR = PROJECT_ROOT / "outputs" / "lineage"
LINEAGE_DIR.mkdir(parents=True, exist_ok=True)


def _event(
    stage: str,
    state: RunState,
    run_id: str,
) -> RunEvent:
    return RunEvent(
        eventType=state,
        eventTime=datetime.now(timezone.utc).isoformat(),
        run=Run(runId=run_id),
        job=Job(
            namespace="livecart-ai",
            name=stage,
        ),
        producer="https://openlineage.io",
        inputs=[],
        outputs=[],
    )


def emit_lineage(
    stage: str,
    state: RunState,
    run_id: str | None = None,
) -> str:
    """
    Emit a real OpenLineage RunEvent and persist the event as evidence.
    """

    run_id = run_id or str(uuid.uuid4())

    event = _event(
        stage=stage,
        state=state,
        run_id=run_id,
    )

    # Instantiate the official OpenLineage client.
    # The event is also persisted locally as auditable evidence.
    client = OpenLineageClient()

    try:
        client.emit(event)
        client_status = "emitted"
    except Exception as exc:
        # Local evidence must still be retained even if no
        # external OpenLineage backend is configured.
        client_status = f"client_error: {exc}"

    evidence = {
        "eventType": state.value,
        "eventTime": event.eventTime,
        "run": {
            "runId": run_id,
        },
        "job": {
            "namespace": "livecart-ai",
            "name": stage,
        },
        "producer": "https://openlineage.io",
        "client_status": client_status,
    }

    output_file = LINEAGE_DIR / f"{stage}_{run_id}_{state.value.lower()}.json"

    output_file.write_text(
        json.dumps(evidence, indent=2),
        encoding="utf-8",
    )

    print(
        f"🔗 OpenLineage | stage={stage} | "
        f"state={state.value} | run_id={run_id}"
    )

    return run_id


def start_stage(stage: str) -> str:
    return emit_lineage(
        stage=stage,
        state=RunState.START,
    )


def complete_stage(stage: str, run_id: str) -> None:
    emit_lineage(
        stage=stage,
        state=RunState.COMPLETE,
        run_id=run_id,
    )


def fail_stage(stage: str, run_id: str) -> None:
    emit_lineage(
        stage=stage,
        state=RunState.FAIL,
        run_id=run_id,
    )


if __name__ == "__main__":
    stage = "quality_gate"

    run_id = start_stage(stage)

    try:
        print("Running OpenLineage test stage...")
        complete_stage(stage, run_id)
        print("✅ OpenLineage START/COMPLETE test passed")
    except Exception:
        fail_stage(stage, run_id)
        raise