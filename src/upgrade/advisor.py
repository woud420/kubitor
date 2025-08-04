"""Kubernetes upgrade advisor."""

from typing import Optional, List
import re

from ..model.cluster import UpgradeSuggestion
from .versions import KUBERNETES_VERSIONS


class UpgradeAdvisor:
    """Provides upgrade path suggestions for Kubernetes clusters."""

    def __init__(self):
        self.versions = KUBERNETES_VERSIONS

    def parse_version(self, version_string: str) -> Optional[tuple[int, int]]:
        """Parse Kubernetes version string to major.minor tuple."""
        # Remove 'v' prefix and extract major.minor
        match = re.match(r"v?(\d+)\.(\d+)", version_string)
        if match:
            return int(match.group(1)), int(match.group(2))
        return None

    def get_upgrade_path(
        self, current_version: str, target_version: Optional[str] = None
    ) -> List[str]:
        """Get the upgrade path from current to target version."""
        current = self.parse_version(current_version)
        if not current:
            return []

        if target_version:
            target = self.parse_version(target_version)
            if not target:
                return []
        else:
            # Default to next minor version
            target = (current[0], current[1] + 1)

        path = []
        major, minor = current

        while (major, minor) < target:
            minor += 1
            if minor > 34:  # Assuming we support up to 1.34
                break
            path.append(f"{major}.{minor}")

        return path

    def get_suggestions(
        self, current_version: str, target_version: Optional[str] = None
    ) -> UpgradeSuggestion:
        """Get upgrade suggestions for the current version."""
        parsed = self.parse_version(current_version)
        if not parsed:
            return UpgradeSuggestion(
                current_version=current_version,
                suggested_next_version="unknown",
                general_recommendations=["Could not parse version"],
            )

        major, minor = parsed

        # Determine next version
        if target_version:
            next_version = target_version
        else:
            next_version = f"{major}.{minor + 1}"

        # Get upgrade path
        upgrade_path = self.get_upgrade_path(current_version, next_version)

        # Collect all upgrade notes, deprecations, and actions for the path
        all_notes = []
        all_deprecations = []
        all_actions = []

        for version in upgrade_path:
            if version in self.versions:
                version_info = self.versions[version]
                all_notes.extend(version_info.get("notes", []))
                all_deprecations.extend(version_info.get("deprecations", []))
                all_actions.extend(version_info.get("actions", []))

        # General recommendations
        general_recommendations = [
            f"Upgrade path: {current_version} → {' → '.join(upgrade_path)}"
            if len(upgrade_path) > 1
            else f"Direct upgrade to {next_version}",
            "Test upgrade in a non-production environment first",
            f"Review the official Kubernetes release notes for each version",
            "Backup etcd before upgrading",
            "Ensure all addons and operators are compatible with target version",
            "Check deprecated APIs using: kubectl api-resources --api-group=<group>",
            "Verify Helm charts compatibility with target version",
            "Update kubectl client to match target version",
            "Review and update RBAC policies if needed",
            "Monitor cluster health during and after upgrade",
        ]

        # Add specific recommendations based on version jump
        version_jump = len(upgrade_path)
        if version_jump > 2:
            general_recommendations.insert(
                1,
                f"⚠️  Large version jump ({version_jump} versions) - consider upgrading incrementally",
            )

        return UpgradeSuggestion(
            current_version=current_version,
            suggested_next_version=next_version,
            upgrade_notes=all_notes[:10] if all_notes else ["No specific notes for this upgrade"],
            api_deprecations=all_deprecations[:10] if all_deprecations else [],
            required_actions=all_actions[:10] if all_actions else [],
            general_recommendations=general_recommendations,
        )
