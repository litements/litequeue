from collections.abc import Iterator
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
IMAGE_TAG = "litequeue-sqlite-matrix:latest"


def require_success(result: ExecResult, action: str) -> str:
    """Return command output or fail the test with its diagnostics."""
    output = result.output.decode(errors="replace")
    if result.exit_code != 0:
        pytest.fail(f"{action} failed with exit code {result.exit_code}:\n{output}")
    return output


def get_sqlite_build_argument() -> str:
    """Serialize the Python release matrix for the Docker build."""
    release_arguments = [
        f"{release.version}:{release.year}:{release.archive_version}"
        for release in SQLITE_RELEASES
    ]
    return " ".join(release_arguments)


@pytest.fixture(scope="module")
def sqlite_test_image() -> Iterator[str]:
    """Build the reusable image containing every supported SQLite release."""
    project_path = Path(__file__).resolve().parents[1]
    build_arguments = {"SQLITE_RELEASES": get_sqlite_build_argument()}
    image = DockerImage(
        path=project_path,
        dockerfile_path=DOCKERFILE_PATH,
        tag=IMAGE_TAG,
        clean_up=False,
        buildargs=build_arguments,
    )

    with image:
        yield str(image)


@pytest.mark.parametrize(
    "release",
    SQLITE_RELEASES,
    ids=[release.version for release in SQLITE_RELEASES],
)
def test_suite_passes_with_sqlite_release(
    release: SQLiteRelease,
    sqlite_test_image: str,
) -> None:
    """Run the test suite with one SQLite release from the matrix image."""
    project_path = Path(__file__).resolve().parents[1]
    sqlite_prefix = f"/opt/sqlite/{release.version}"
    test_script = f"""
set -eu
cd {CONTAINER_PROJECT_PATH}
actual_version=$(uv run --no-sync python -c 'import sqlite3; print(sqlite3.sqlite_version)')
echo "SQLite version: expected {release.version}, loaded $actual_version"
test "$actual_version" = "{release.version}"
uv run --no-sync pytest tests --ignore=tests/test_sqlite_versions.py
"""

    container = DockerContainer(image=sqlite_test_image)
    container.with_env(
        key="LD_LIBRARY_PATH",
        value=f"{sqlite_prefix}/lib",
    )
    container.with_env(
        key="LD_PRELOAD",
        value=f"{sqlite_prefix}/lib/libsqlite3.so.0",
    )
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
