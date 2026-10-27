"""The data-test gate in tests/conftest.py."""

import os
import subprocess
import sys
from pathlib import Path

import pytest

CONFTEST = Path(__file__).parents[1] / "conftest.py"


@pytest.mark.parametrize(("require_data", "exit_code"), [("", 0), ("1", 1)])
def test_skipped_data_test_fails_only_when_data_is_required(
    tmp_path: Path, require_data: str, exit_code: int
) -> None:
    (tmp_path / "conftest.py").write_text(CONFTEST.read_text())
    (tmp_path / "pytest.ini").write_text("[pytest]\nmarkers = data: needs the cache\n")
    (tmp_path / "test_cold.py").write_text(
        "import pytest\n\n@pytest.mark.data\ndef test_cold():\n    pytest.skip('cold')\n"
    )
    env = {**os.environ, "ODA_DATA_REQUIRE_DATA": require_data}
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider"],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == exit_code, result.stdout
