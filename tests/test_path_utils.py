from pathlib import Path

from backup_projects.adapters.filesystem.path_utils import (
    is_relative_to,
    is_same_filesystem,
    join_path,
    relative_to,
    resolve_path,
    to_path,
)


def test_to_path_and_join_path_return_path_objects(tmp_path) -> None:
    joined = join_path(tmp_path, "show-a", "edit.prproj")

    assert to_path(str(tmp_path)) == tmp_path
    assert isinstance(joined, Path)
    assert joined == tmp_path / "show-a" / "edit.prproj"


def test_relative_path_helpers_use_resolved_paths(tmp_path) -> None:
    base_path = tmp_path / "projects"
    child_path = base_path / "show-a" / "edit.prproj"
    outside_path = tmp_path / "outside" / "other.prproj"

    child_path.parent.mkdir(parents=True)
    child_path.write_text("edit")
    outside_path.parent.mkdir(parents=True)
    outside_path.write_text("other")

    assert resolve_path(base_path) == base_path.resolve()
    assert relative_to(child_path, base_path) == Path("show-a") / "edit.prproj"
    assert is_relative_to(child_path, base_path) is True
    assert relative_to(outside_path, base_path) is None
    assert is_relative_to(outside_path, base_path) is False


def test_is_same_filesystem_returns_true_for_paths_under_same_tmp_tree(tmp_path) -> None:
    left = tmp_path / "left"
    right = tmp_path / "right"
    left.mkdir()
    right.mkdir()

    assert is_same_filesystem(left, right) is True
