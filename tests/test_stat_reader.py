from backup_projects.adapters.filesystem.stat_reader import StatInfo, path_exists, read_stat


def test_read_stat_returns_file_info_for_regular_file(tmp_path) -> None:
    file_path = tmp_path / "project.prproj"
    file_path.write_text("project")

    stat_info = read_stat(file_path)

    assert isinstance(stat_info, StatInfo)
    assert stat_info is not None
    assert stat_info.path == file_path
    assert stat_info.exists is True
    assert stat_info.is_file is True
    assert stat_info.is_dir is False
    assert stat_info.is_symlink is False
    assert stat_info.size_bytes == len("project")
    assert isinstance(stat_info.mtime_ns, int)
    assert isinstance(stat_info.ctime_ns, int)
    assert isinstance(stat_info.inode, int)
    assert isinstance(stat_info.device_id, int)


def test_read_stat_returns_directory_info_for_directory(tmp_path) -> None:
    dir_path = tmp_path / "episode_01"
    dir_path.mkdir()

    stat_info = read_stat(dir_path)

    assert stat_info is not None
    assert stat_info.path == dir_path
    assert stat_info.exists is True
    assert stat_info.is_file is False
    assert stat_info.is_dir is True
    assert stat_info.is_symlink is False
    assert stat_info.size_bytes is None
    assert isinstance(stat_info.inode, int)
    assert isinstance(stat_info.device_id, int)


def test_read_stat_returns_none_for_missing_path(tmp_path) -> None:
    missing_path = tmp_path / "missing.aaf"

    assert read_stat(missing_path) is None
    assert path_exists(missing_path) is False
