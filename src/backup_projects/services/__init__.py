"""Service layer package."""

from backup_projects.services.root_discovery_service import (
    DiscoveredRootCandidate,
    RootDiscoveryResult,
    discover_and_sync_roots,
    list_root_directories,
)

__all__ = [
    "DiscoveredRootCandidate",
    "RootDiscoveryResult",
    "discover_and_sync_roots",
    "list_root_directories",
]
