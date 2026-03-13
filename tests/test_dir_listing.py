import pytest

from backup_projects.adapters.filesystem.dir_listing import DirEntryInfo, list_dir


def test_list_dir_returns_stable_direct_children_contract(tmp_path) -> None:
    nested_dir = tmp_path / "nested"
    nested_dir.mkdir()
    (nested_dir / "deep.txt").write_text("deep")
    (tmp_path / "alpha.txt").write_text("alpha")
    (tmp_path / ".hidden.txt").write_text("hidden")

    entries = list_dir(tmp_path)

    assert entries == (
        DirEntryInfo(
            name=".hidden.txt",
            path=tmp_path / ".hidden.txt",
            is_dir=False,
            is_file=True,
            is_symlink=False,
        ),
        DirEntryInfo(
            name="alpha.txt",
            path=tmp_path / "alpha.txt",
            is_dir=False,
            is_file=True,
            is_symlink=False,
        ),
        DirEntryInfo(
            name="nested",
            path=tmp_path / "nested",
            is_dir=True,
            is_file=False,
            is_symlink=False,
        ),
    )


def test_list_dir_handles_empty_directory(tmp_path) -> None:
    assert list_dir(tmp_path) == ()


def test_list_dir_can_skip_hidden_entries(tmp_path) -> None:
    (tmp_path / ".hidden.txt").write_text("hidden")
    (tmp_path / "visible.txt").write_text("visible")

    entries = list_dir(tmp_path, include_hidden=False)

    assert [entry.name for entry in entries] == ["visible.txt"]


def test_list_dir_raises_for_missing_directory(tmp_path) -> None:
    with pytest.raises(FileNotFoundError):
        list_dir(tmp_path / "missing")
