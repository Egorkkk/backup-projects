from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from backup_projects.adapters.restic_adapter import (
    ResticBackupRequest,
    ResticBackupResult,
    run_restic_backup,
)
from backup_projects.domain.models import ManifestResult

EMPTY_MANIFEST_SKIP_MESSAGE = "Backup skipped: manifest include set is empty"


@dataclass(frozen=True, slots=True)
class BackupServiceRequest:
    manifest_result: ManifestResult
    restic_binary: str
    restic_repository: str
    restic_password_env_var: str
    restic_timeout_seconds: int


@dataclass(frozen=True, slots=True)
class BackupServiceResult:
    manifest_result: ManifestResult
    restic_result: ResticBackupResult | None
    message: str | None = None


def run_backup_from_manifest(
    request: BackupServiceRequest,
    *,
    backup_runner=run_restic_backup,
) -> BackupServiceResult:
    manifest_file_path = request.manifest_result.manifest_file_path
    normalized_manifest_file_path = manifest_file_path.strip()

    if normalized_manifest_file_path == "":
        raise ValueError("manifest_result.manifest_file_path must not be empty")

    manifest_path = Path(normalized_manifest_file_path)
    if not manifest_path.exists() or not manifest_path.is_file():
        raise FileNotFoundError(manifest_file_path)

    if not request.manifest_result.manifest_paths:
        return BackupServiceResult(
            manifest_result=request.manifest_result,
            restic_result=None,
            message=EMPTY_MANIFEST_SKIP_MESSAGE,
        )

    restic_result = backup_runner(
        ResticBackupRequest(
            manifest_file_path=manifest_file_path,
            binary=request.restic_binary,
            repository=request.restic_repository,
            password_env_var=request.restic_password_env_var,
            timeout_seconds=request.restic_timeout_seconds,
        )
    )

    return BackupServiceResult(
        manifest_result=request.manifest_result,
        restic_result=restic_result,
    )
