"""Test upgrade advisor functionality."""

import pytest
from src.upgrade.advisor import UpgradeAdvisor


class TestUpgradeAdvisor:
    def setup_method(self):
        """Set up test fixtures."""
        self.advisor = UpgradeAdvisor()

    def test_parse_version(self):
        """Test version parsing."""
        assert self.advisor.parse_version("v1.28.0") == (1, 28)
        assert self.advisor.parse_version("1.27.5") == (1, 27)
        assert self.advisor.parse_version("invalid") is None

    def test_get_upgrade_path_single_version(self):
        """Test single version upgrade path."""
        path = self.advisor.get_upgrade_path("v1.27.0")
        assert path == ["1.28"]

    def test_get_upgrade_path_multi_version(self):
        """Test multi-version upgrade path."""
        path = self.advisor.get_upgrade_path("v1.25.0", "v1.28.0")
        assert path == ["1.26", "1.27", "1.28"]

    def test_get_suggestions_valid_version(self):
        """Test getting suggestions for valid version."""
        suggestions = self.advisor.get_suggestions("v1.27.0")

        assert suggestions.current_version == "v1.27.0"
        assert suggestions.suggested_next_version == "1.28"
        assert len(suggestions.general_recommendations) > 0
        assert any("SeccompDefault" in note for note in suggestions.upgrade_notes)

    def test_get_suggestions_invalid_version(self):
        """Test getting suggestions for invalid version."""
        suggestions = self.advisor.get_suggestions("invalid")

        assert suggestions.current_version == "invalid"
        assert suggestions.suggested_next_version == "unknown"
        assert "Could not parse version" in suggestions.general_recommendations

    def test_get_suggestions_with_target_version(self):
        """Test getting suggestions with target version."""
        suggestions = self.advisor.get_suggestions("v1.25.0", "v1.27.0")

        assert suggestions.current_version == "v1.25.0"
        assert suggestions.suggested_next_version == "v1.27.0"
        # Should include notes for both 1.26 and 1.27
        assert len(suggestions.upgrade_notes) > 0

    def test_version_specific_notes(self):
        """Test that version-specific notes are included."""
        suggestions = self.advisor.get_suggestions("v1.26.0")

        # Should include 1.27 specific notes
        assert any("SeccompDefault" in note for note in suggestions.upgrade_notes)
        assert any("k8s.gcr.io" in dep for dep in suggestions.api_deprecations)
