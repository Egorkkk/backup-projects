"""Service layer package."""

from backup_projects.services.root_discovery_service import (
    DiscoveredRootCandidate,
    RootDiscoveryResult,
    discover_and_sync_roots,
    list_root_directories,
)
from backup_projects.services.structural_scan_service import (
    ScannedProjectDir,
    ScannedProjectFile,
    StructuralScanResult,
    scan_root_structure,
)

__all__ = [
    "DiscoveredRootCandidate",
    "RootDiscoveryResult",
    "ScannedProjectDir",
    "ScannedProjectFile",
    "StructuralScanResult",
    "discover_and_sync_roots",
    "list_root_directories",
    "scan_root_structure",
]
