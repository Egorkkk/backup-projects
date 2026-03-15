import pytest

from backup_projects.services.retention_service import run_retention
from backup_projects.services.verify_service import run_verify


def test_run_verify_raises_not_implemented_error_with_stable_message() -> None:
    with pytest.raises(
        NotImplementedError,
        match="^verify service is not implemented in v1 baseline$",
    ):
        run_verify()


def test_run_retention_raises_not_implemented_error_with_stable_message() -> None:
    with pytest.raises(
        NotImplementedError,
        match="^retention service is not implemented in v1 baseline$",
    ):
        run_retention()
