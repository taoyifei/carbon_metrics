"""Test that frontend builds successfully."""
import os
import subprocess
import shutil

import pytest


class TestFrontendBuild:
    """Verify frontend TypeScript compilation and Vite build."""

    @pytest.mark.skipif(
        shutil.which('node') is None,
        reason='Node.js not available'
    )
    def test_npm_build(self):
        """npm run build should exit with code 0."""
        frontend_dir = os.path.join(
            os.path.dirname(__file__), '..',
            'carbon_metrics', 'frontend'
        )
        result = subprocess.run(
            ['npm', 'run', 'build'],
            cwd=frontend_dir,
            capture_output=True,
            text=True,
            timeout=120,
            shell=True,
        )
        assert result.returncode == 0, (
            f"npm run build failed (exit {result.returncode}):\n"
            f"STDOUT: {result.stdout[-500:] if result.stdout else ''}\n"
            f"STDERR: {result.stderr[-500:] if result.stderr else ''}"
        )
