from pathlib import Path
import subprocess
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def run_stage(module: str) -> None:
    """
    Execute a project stage as a Python module.

    The stage must exit with code 0 on success.
    Any non-zero exit code is propagated to Airflow.
    """

    command = [
        sys.executable,
        "-m",
        module,
    ]

    print("=" * 70)
    print(f"Starting stage: {module}")
    print("=" * 70)

    result = subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        check=False,
    )

    if result.returncode != 0:
        raise RuntimeError(
            f"Stage failed: {module} "
            f"(exit code={result.returncode})"
        )

    print("=" * 70)
    print(f"Stage completed successfully: {module}")
    print("=" * 70)