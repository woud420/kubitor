"""Test Kubernetes client functionality."""

import pytest
from unittest.mock import patch, MagicMock
from src.k8s.client import K8sClient


class TestK8sClient:
    @patch("subprocess.run")
    def test_kubectl_verification_success(self, mock_run):
        """Test successful kubectl verification."""
        mock_run.return_value = MagicMock(
            stdout='{"clientVersion": {"major": "1", "minor": "28"}}', stderr="", returncode=0
        )

        # Should not raise an exception
        client = K8sClient()
        assert client is not None

    @patch("subprocess.run")
    def test_kubectl_verification_failure(self, mock_run):
        """Test kubectl verification failure."""
        mock_run.side_effect = FileNotFoundError()

        with pytest.raises(RuntimeError, match="kubectl command not found"):
            K8sClient()

    @patch("subprocess.run")
    def test_build_command_basic(self, mock_run):
        """Test building basic kubectl command."""
        mock_run.return_value = MagicMock(stdout="{}", stderr="", returncode=0)
        client = K8sClient()
        cmd = client._build_command(["get", "pods"])
        assert cmd == ["kubectl", "get", "pods"]

    @patch("subprocess.run")
    def test_build_command_with_context(self, mock_run):
        """Test building command with context."""
        mock_run.return_value = MagicMock(stdout="{}", stderr="", returncode=0)
        client = K8sClient(context="test-context")
        cmd = client._build_command(["get", "pods"])
        assert cmd == ["kubectl", "--context", "test-context", "get", "pods"]

    @patch("subprocess.run")
    def test_build_command_with_namespace(self, mock_run):
        """Test building command with namespace."""
        mock_run.return_value = MagicMock(stdout="{}", stderr="", returncode=0)
        client = K8sClient(namespace="test-namespace")
        cmd = client._build_command(["get", "pods"])
        assert cmd == ["kubectl", "get", "pods", "-n", "test-namespace"]

    @patch("subprocess.run")
    def test_execute_success(self, mock_run):
        """Test successful command execution."""
        mock_run.return_value = MagicMock(stdout='{"items": []}', stderr="", returncode=0)

        client = K8sClient()
        success, output = client.execute(["get", "pods", "-o", "json"])

        assert success is True
        assert output == '{"items": []}'

    @patch("subprocess.run")
    def test_execute_failure(self, mock_run):
        """Test failed command execution."""
        from subprocess import CalledProcessError

        mock_run.side_effect = CalledProcessError(1, "kubectl", stderr="Error message")

        client = K8sClient()
        success, output = client.execute(["get", "pods"])

        assert success is False
        assert "Error message" in output
