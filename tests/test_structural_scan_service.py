from pathlib import Path

from backup_projects.services.structural_scan_service import scan_root_structure


def test_scan_root_structure_detects_project_dirs_and_collects_all_files(tmp_path: Path) -> None:
    root_path = tmp_path / "raid_root"
    project_dir = root_path / "Show A"
    nested_dir = project_dir / "Assets"
    nested_dir.mkdir(parents=True)
    (project_dir / "edit.prproj").write_text("project")
    (nested_dir / "clip.mov").write_text("clip")
    (nested_dir / "notes.txt").write_text("notes")
    (root_path / "README.txt").write_text("ignore")

    result = scan_root_structure(root_path=root_path, allowed_extensions={"prproj"})

    assert result.root_path == root_path.resolve().as_posix()
    assert [project_dir.relative_path for project_dir in result.project_dirs] == ["Show A"]
    scanned_dir = result.project_dirs[0]
    assert scanned_dir.name == "Show A"
    assert scanned_dir.dir_type == "premiere"
    assert [file.relative_path for file in scanned_dir.files] == [
        "Assets/clip.mov",
        "Assets/notes.txt",
        "edit.prproj",
    ]


def test_scan_root_structure_prunes_nested_project_dir_candidates(tmp_path: Path) -> None:
    root_path = tmp_path / "raid_root"
    outer_dir = root_path / "Show A"
    inner_dir = outer_dir / "Nested Project"
    inner_dir.mkdir(parents=True)
    (outer_dir / "edit.prproj").write_text("project")
    (inner_dir / "nested.aep").write_text("nested")

    result = scan_root_structure(root_path=root_path, allowed_extensions={"prproj", "aep"})

    assert [project_dir.relative_path for project_dir in result.project_dirs] == ["Show A"]
    assert [file.relative_path for file in result.project_dirs[0].files] == [
        "Nested Project/nested.aep",
        "edit.prproj",
    ]
    assert result.project_dirs[0].dir_type == "mixed"


def test_scan_root_structure_classifies_all_supported_dir_types(tmp_path: Path) -> None:
    root_path = tmp_path / "raid_root"
    (root_path / "Avid Show").mkdir(parents=True)
    (root_path / "Avid Show" / "edit.avb").write_text("avid")
    (root_path / "After Effects").mkdir(parents=True)
    (root_path / "After Effects" / "comp.aep").write_text("ae")
    (root_path / "Resolve Show").mkdir(parents=True)
    (root_path / "Resolve Show" / "grade.drp").write_text("resolve")

    result = scan_root_structure(root_path=root_path, allowed_extensions={"avb", "aep", "drp"})

    assert [
        (project_dir.relative_path, project_dir.dir_type)
        for project_dir in result.project_dirs
    ] == [
        ("After Effects", "aftereffects"),
        ("Avid Show", "avid"),
        ("Resolve Show", "resolve"),
    ]


def test_scan_root_structure_marks_neutral_only_project_dir_as_unknown(tmp_path: Path) -> None:
    root_path = tmp_path / "raid_root"
    project_dir = root_path / "Neutral Package"
    project_dir.mkdir(parents=True)
    (project_dir / "timeline.aaf").write_text("aaf")
    (project_dir / "sequence.xml").write_text("xml")

    result = scan_root_structure(root_path=root_path, allowed_extensions={"aaf", "xml"})

    assert [project_dir.relative_path for project_dir in result.project_dirs] == ["Neutral Package"]
    assert result.project_dirs[0].dir_type == "unknown"


def test_scan_root_structure_supports_root_itself_as_project_dir(tmp_path: Path) -> None:
    root_path = tmp_path / "raid_root"
    root_path.mkdir()
    (root_path / "root_edit.prproj").write_text("project")
    (root_path / "media.mov").write_text("media")

    result = scan_root_structure(root_path=root_path, allowed_extensions={"prproj"})

    assert [project_dir.relative_path for project_dir in result.project_dirs] == [""]
    assert result.project_dirs[0].name == "raid_root"
    assert result.project_dirs[0].dir_type == "premiere"
    assert [file.relative_path for file in result.project_dirs[0].files] == [
        "media.mov",
        "root_edit.prproj",
    ]
