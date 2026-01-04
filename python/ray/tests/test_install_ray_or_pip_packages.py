"""
Unit tests for uninstall_ray function in install_ray_or_pip_packages.py
"""
import os
import sys
import subprocess
import unittest
from unittest.mock import patch, MagicMock

# Add the project root to Python path to import ray modules
project_root = os.path.join(os.path.dirname(__file__), '..', '..')
sys.path.insert(0, project_root)

from ray._private.runtime_env.install_ray_or_pip_packages import uninstall_ray


class TestUninstallRay(unittest.TestCase):
    """Unit test class for uninstall_ray function"""

    def setUp(self):
        """Set up test fixtures before each test method."""
        pass

    def tearDown(self):
        """Clean up after each test method."""
        pass

    @patch('ray._private.runtime_env.install_ray_or_pip_packages.subprocess.run')
    @patch('ray._private.runtime_env.install_ray_or_pip_packages.logger')
    def test_uninstall_ray_success(self, mock_logger, mock_subprocess_run):
        """Test successful uninstallation of ray packages"""
        # Set up mock return value
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_subprocess_run.return_value = mock_result
        
        # Call the function under test
        uninstall_ray()
        
        # Verify subprocess.run was called correctly
        expected_command = [sys.executable, '-m', 'pip', 'uninstall', '-y', 
                          'ray', 'ant-ray', 'ant-ray-nightly']
        mock_subprocess_run.assert_called_once_with(expected_command)
        
        # Verify logger.info was called
        mock_logger.info.assert_called_once()
        call_args = mock_logger.info.call_args[0][0]
        self.assertIn("Uninstalled ray", call_args)
        self.assertIn("Return code: 0", call_args)

    @patch('ray._private.runtime_env.install_ray_or_pip_packages.subprocess.run')
    @patch('ray._private.runtime_env.install_ray_or_pip_packages.logger')
    def test_uninstall_ray_failure(self, mock_logger, mock_subprocess_run):
        """Test uninstallation with non-zero return code"""
        # Set up mock return value
        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_subprocess_run.return_value = mock_result
        
        # Call the function under test
        uninstall_ray()
        
        # Verify subprocess.run was called
        expected_command = [sys.executable, '-m', 'pip', 'uninstall', '-y', 
                          'ray', 'ant-ray', 'ant-ray-nightly']
        mock_subprocess_run.assert_called_once_with(expected_command)
        
        # Verify logger.info was called even with non-zero return code
        mock_logger.info.assert_called_once()
        call_args = mock_logger.info.call_args[0][0]
        self.assertIn("Uninstalled ray", call_args)
        self.assertIn("Return code: 1", call_args)

    @patch('ray._private.runtime_env.install_ray_or_pip_packages.subprocess.run')
    @patch('ray._private.runtime_env.install_ray_or_pip_packages.logger')
    def test_uninstall_ray_command_structure(self, mock_logger, mock_subprocess_run):
        """Test the structure of the uninstall command"""
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_subprocess_run.return_value = mock_result

        uninstall_ray()

        # Verify the command structure
        call_args = mock_subprocess_run.call_args[0][0]
        self.assertEqual(call_args[0], sys.executable)
        self.assertEqual(call_args[1], '-m')
        self.assertEqual(call_args[2], 'pip')
        self.assertEqual(call_args[3], 'uninstall')
        self.assertEqual(call_args[4], '-y')
        
        # Verify all expected packages are included
        packages = call_args[5:]
        self.assertIn('ray', packages)
        self.assertIn('ant-ray', packages)
        self.assertIn('ant-ray-nightly', packages)
        self.assertEqual(len(packages), 3)

    @patch('ray._private.runtime_env.install_ray_or_pip_packages.subprocess.run')
    @patch('ray._private.runtime_env.install_ray_or_pip_packages.logger')
    def test_uninstall_ray_exception_propagation(self, mock_logger, mock_subprocess_run):
        """Test that exceptions from subprocess.run are propagated"""
        # Mock subprocess.run to raise an exception
        mock_subprocess_run.side_effect = Exception("Mocked subprocess error")

        # The exception should be propagated
        with self.assertRaises(Exception) as context:
            uninstall_ray()
        
        self.assertEqual(str(context.exception), "Mocked subprocess error")


if __name__ == '__main__':
    unittest.main()