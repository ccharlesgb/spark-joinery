from __future__ import annotations

from pathlib import Path
import subprocess
import sys


REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
DOCS_SOURCE_DIRECTORY = REPOSITORY_ROOT / "docs_src"


def write_log(script_path: Path, stream_name: str, content: str) -> None:
    log_path = script_path.with_name(f"{script_path.stem}_{stream_name}.log")
    if content:
        log_path.write_text(content)
    elif log_path.exists():
        log_path.unlink()


def main() -> None:
    for script_path in sorted(DOCS_SOURCE_DIRECTORY.rglob("*.py")):
        print(f"Running {script_path}")
        result = subprocess.run(
            [sys.executable, str(script_path)],
            capture_output=True,
            cwd=REPOSITORY_ROOT,
            text=True,
        )

        write_log(script_path, "stdout", result.stdout)
        assert result.returncode == 0, f"{script_path} exited with {result.returncode}"


if __name__ == "__main__":
    main()
