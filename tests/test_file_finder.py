import os

import pytest

from backup_projects.adapters.filesystem import file_finder as file_finder_module
from backup_projects.adapters.filesystem.file_finder import FoundFileInfo, find_files
from backup_projects.adapters.filesystem.stat_reader import StatInfo


def test_find_files_recursively_returns_deterministic_relative_paths(tmp_path) -> None:
    (tmp_path / "a").mkdir()
    (tmp_path / "a" / "nested.prproj").write_text("nested")
    (tmp_path / "b").mkdir()
    (tmp_path / "b" / "deep").mkdir()
    (tmp_path / "b" / "deep" / "clip.aaf").write_text("clip")
    (tmp_path / "root.txt").write_text("root")

    results = find_files(tmp_path, allowed_extensions=None)

    assert results == (
        FoundFileInfo(
            path=tmp_path / "a" / "nested.prproj",
            relative_path=(tmp_path / "a" / "nested.prproj").relative_to(tmp_path),
        ),
        FoundFileInfo(
            path=tmp_path / "b" / "deep" / "clip.aaf",
            relative_path=(tmp_path / "b" / "deep" / "clip.aaf").relative_to(tmp_path),
        ),
        FoundFileInfo(
            path=tmp_path / "root.txt",
            relative_path=tmp_path.joinpath("root.txt").relative_to(tmp_path),
        ),
    )


def test_find_files_filters_by_extension_case_insensitively(tmp_path) -> None:
    (tmp_path / "edit.PRPROJ").write_text("edit")
    (tmp_path / "audio.wav").write_text("audio")

    results = find_files(tmp_path, allowed_extensions={"prproj"})

    assert [item.relative_path.as_posix() for item in results] == ["edit.PRPROJ"]


def test_find_files_can_prune_excluded_files_and_directories(tmp_path) -> None:
    (tmp_path / "keep").mkdir()
    (tmp_path / "keep" / "edit.prproj").write_text("edit")
    (tmp_path / "ignore_me").mkdir()
    (tmp_path / "ignore_me" / "skip.aaf").write_text("skip")
    (tmp_path / "skip.tmp").write_text("tmp")

    results = find_files(
        tmp_path,
        allowed_extensions=None,
        excluded_path_patterns=("ignore_me/**", "*.tmp"),
    )

    assert [item.relative_path.as_posix() for item in results] == ["keep/edit.prproj"]


def test_find_files_raises_for_missing_start_directory(tmp_path) -> None:
    with pytest.raises(FileNotFoundError):
        find_files(tmp_path / "missing")


def test_find_files_raises_for_file_start_path(tmp_path) -> None:
    file_path = tmp_path / "single.prproj"
    file_path.write_text("single")

    with pytest.raises(NotADirectoryError):
        find_files(file_path)


@pytest.mark.skipif(not hasattr(os, "symlink"), reason="symlink support unavailable")
def test_find_files_ignores_symlinks_by_default(tmp_path) -> None:
    target_dir = tmp_path / "target"
    target_dir.mkdir()
    (target_dir / "linked.prproj").write_text("linked")
    os.symlink(target_dir, tmp_path / "linked_dir")
    os.symlink(target_dir / "linked.prproj", tmp_path / "linked_file.prproj")
    (tmp_path / "real.prproj").write_text("real")

    results = find_files(tmp_path, allowed_extensions={"prproj"})

    assert [item.relative_path.as_posix() for item in results] == [
        "real.prproj",
        "target/linked.prproj",
    ]


@pytest.mark.skipif(not hasattr(os, "symlink"), reason="symlink support unavailable")
def test_find_files_can_follow_symlinks_with_cycle_guard(tmp_path) -> None:
    real_dir = tmp_path / "real"
    real_dir.mkdir()
    (real_dir / "inside.aaf").write_text("inside")
    os.symlink(real_dir, tmp_path / "linked_dir")
    os.symlink(tmp_path, real_dir / "cycle_back")

    results = find_files(
        tmp_path,
        allowed_extensions={"aaf"},
        follow_symlinks=True,
    )

    assert [item.relative_path.as_posix() for item in results] == ["linked_dir/inside.aaf"]


def test_find_files_can_stay_on_filesystem_when_child_device_differs(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    keep_dir = tmp_path / "keep"
    keep_dir.mkdir()
    (keep_dir / "keep.prproj").write_text("keep")
    other_fs_dir = tmp_path / "other_fs"
    other_fs_dir.mkdir()
    (other_fs_dir / "skip.prproj").write_text("skip")

    original_read_stat = file_finder_module.read_stat

    def fake_read_stat(path, *, follow_symlinks=False):
        stat_info = original_read_stat(path, follow_symlinks=follow_symlinks)
        if stat_info is None:
            return None
        if stat_info.path == other_fs_dir:
            return StatInfo(
                path=stat_info.path,
                exists=stat_info.exists,
                is_file=stat_info.is_file,
                is_dir=stat_info.is_dir,
                is_symlink=stat_info.is_symlink,
                size_bytes=stat_info.size_bytes,
                mtime_ns=stat_info.mtime_ns,
                ctime_ns=stat_info.ctime_ns,
                inode=stat_info.inode,
                device_id=(stat_info.device_id or 0) + 1,
            )
        return stat_info

    monkeypatch.setattr(file_finder_module, "read_stat", fake_read_stat)

    results = find_files(
        tmp_path,
        allowed_extensions={"prproj"},
        stay_on_filesystem=True,
    )

    assert [item.relative_path.as_posix() for item in results] == ["keep/keep.prproj"]
