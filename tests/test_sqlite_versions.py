from dataclasses import dataclass
from pathlib import Path

import pytest
from docker.models.containers import ExecResult
from testcontainers.core.container import DockerContainer
from testcontainers.core.image import DockerImage


@dataclass(frozen=True, slots=True)
class SQLiteRelease:
    """Describe an official SQLite source release."""

    version: str
    year: int
    archive_version: int


SQLITE_RELEASES = (
    SQLiteRelease(version="3.34.1", year=2021, archive_version=3340100),
    SQLiteRelease(version="3.35.0", year=2021, archive_version=3350000),
    SQLiteRelease(version="3.40.1", year=2022, archive_version=3400100),
    SQLiteRelease(version="3.45.3", year=2024, archive_version=3450300),
)

CONTAINER_PROJECT_PATH = "/workspace"
DOCKERFILE_PATH = "tests/test.dockerfile"


def require_success(result: ExecResult, action: str) -> str:
    """Return command output or fail the test with its diagnostics."""
    output = result.output.decode(errors="replace")
    if result.exit_code != 0:
        pytest.fail(f"{action} failed with exit code {result.exit_code}:\n{output}")
    return output


@pytest.mark.parametrize(
    "release",
    SQLITE_RELEASES,
    ids=[release.version for release in SQLITE_RELEASES],
)
def test_suite_passes_with_sqlite_release(release: SQLiteRelease) -> None:
    """Compile a SQLite release on Debian and run the test suite against it."""
    project_path = Path(__file__).resolve().parents[1]
    image_tag = f"litequeue-sqlite:{release.version}"
    build_arguments = {
        "SQLITE_VERSION": release.version,
        "SQLITE_YEAR": str(release.year),
        "SQLITE_ARCHIVE_VERSION": str(release.archive_version),
    }
    test_script = f"""
set -eu
cd {CONTAINER_PROJECT_PATH}
actual_version=$(uv run --no-sync python -c 'import sqlite3; print(sqlite3.sqlite_version)')
echo "SQLite version: expected {release.version}, loaded $actual_version"
test "$actual_version" = "{release.version}"
uv run --no-sync pytest tests --ignore=tests/test_sqlite_versions.py
"""

    image = DockerImage(
        path=project_path,
        dockerfile_path=DOCKERFILE_PATH,
        tag=image_tag,
        clean_up=False,
        buildargs=build_arguments,
    )

    with image:
        container = DockerContainer(image=str(image))
        container.with_volume_mapping(
            host=project_path,
            container=CONTAINER_PROJECT_PATH,
            mode="ro",
        )

        with container:
            test_result = container.exec(command=["sh", "-c", test_script])
            require_success(
                result=test_result,
                action=f"testing SQLite {release.version}",
            )
