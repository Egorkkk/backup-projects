from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy.orm import Session

from backup_projects.repositories.project_dirs_repo import ProjectDirsRepository
from backup_projects.repositories.roots_repo import RootsRepository


@dataclass(frozen=True, slots=True)
class ProjectDirTableRow:
    id: int
    root_id: int
    root_name: str
    root_path: str
    relative_path: str
    name: str
    dir_type: str
    status: str
    last_seen_at: str


@dataclass(frozen=True, slots=True)
class ProjectDirsPageView:
    rows: tuple[ProjectDirTableRow, ...]


def build_project_dirs_page_view(*, session: Session) -> ProjectDirsPageView:
    roots_repo = RootsRepository(session)
    project_dirs_repo = ProjectDirsRepository(session)

    rows: list[ProjectDirTableRow] = []
    for root in roots_repo.list_all():
        for project_dir in project_dirs_repo.list_by_root(root.id):
            rows.append(
                ProjectDirTableRow(
                    id=project_dir.id,
                    root_id=root.id,
                    root_name=root.name,
                    root_path=root.path,
                    relative_path=project_dir.relative_path,
                    name=project_dir.name,
                    dir_type=project_dir.dir_type,
                    status="missing" if project_dir.is_missing else "active",
                    last_seen_at=project_dir.last_seen_at,
                )
            )

    return ProjectDirsPageView(rows=tuple(rows))
