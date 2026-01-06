#!/usr/bin/env python3
"""
Unit tests for utility functions in image_uri.py
"""

import os
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch, MagicMock

# Import the functions directly by extracting them
import os
from ray._private.utils import (
    discover_files_by_patterns,
)


class TestFileDiscoveryUtils(unittest.TestCase):
    """Test cases for discover_files_by_patterns and _discover_devices_by_patterns functions."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.logger = Mock()
    
    @patch('glob.glob')
    @patch('os.path.exists')
    def test_discover_files_basic_functionality(self, mock_exists, mock_glob):
        """Test basic file discovery functionality."""
        mock_glob.return_value = ['/usr/lib/libcuda.so', '/usr/lib/libnvidia-ml.so']
        mock_exists.return_value = True
        
        volume_mounts = set()
        patterns = ['/usr/lib/libcuda.so', '/usr/lib/libnvidia-ml.so']
        
        discover_files_by_patterns(
            patterns, volume_mounts
        )
        
        expected_mounts = {
            '/usr/lib/libcuda.so:/usr/lib/libcuda.so:ro',
            '/usr/lib/libnvidia-ml.so:/usr/lib/libnvidia-ml.so:ro'
        }
        self.assertEqual(volume_mounts, expected_mounts)
    
    @patch('glob.glob')
    @patch('os.path.exists')
    def test_discover_files_with_custom_mount_mode(self, mock_exists, mock_glob):
        """Test file discovery with custom mount mode."""
        mock_glob.return_value = ['/dev/shm']
        mock_exists.return_value = True
        
        volume_mounts = set()
        patterns = ['/dev/shm']
        
        discover_files_by_patterns(
            patterns, volume_mounts, mount_mode=""
        )
        
        expected_mounts = {'/dev/shm:/dev/shm'}
        self.assertEqual(volume_mounts, expected_mounts)
    
    @patch('glob.glob')
    @patch('os.path.exists')
    def test_discover_files_file_only_filter(self, mock_exists, mock_glob):
        """Test file discovery with file-only filter."""
        mock_glob.return_value = ['/usr/bin/nvidia-smi']
        mock_exists.return_value = True
        with patch('os.path.isfile') as mock_isfile:
            mock_isfile.return_value = True
            
            volume_mounts = set()
            patterns = ['/usr/bin/nvidia-smi']
            
            discover_files_by_patterns(
                patterns, volume_mounts, file_only=True
            )
            
            expected_mounts = {'/usr/bin/nvidia-smi:/usr/bin/nvidia-smi:ro'}
            self.assertEqual(volume_mounts, expected_mounts)
    
    @patch('glob.glob')
    @patch('os.path.exists')
    def test_discover_files_skip_nonexistent(self, mock_exists, mock_glob):
        """Test that non-existent files are skipped."""
        mock_glob.return_value = ['/usr/lib/libcuda.so', '/usr/lib/nonexistent.so']
        mock_exists.side_effect = lambda path: path != '/usr/lib/nonexistent.so'
        
        volume_mounts = set()
        patterns = ['/usr/lib/libcuda.so', '/usr/lib/nonexistent.so']
        
        discover_files_by_patterns(
            patterns, volume_mounts
        )
        
        expected_mounts = {'/usr/lib/libcuda.so:/usr/lib/libcuda.so:ro'}
        self.assertEqual(volume_mounts, expected_mounts)
    
    @patch('glob.glob')
    @patch('os.path.exists')
    def test_discover_files_skip_directories_when_file_only(self, mock_exists, mock_glob):
        """Test that directories are skipped when file_only=True."""
        mock_glob.return_value = ['/usr/bin/nvidia-smi', '/usr/lib/nvidia']
        mock_exists.return_value = True
        with patch('os.path.isfile') as mock_isfile:
            mock_isfile.side_effect = lambda path: path == '/usr/bin/nvidia-smi'
            
            volume_mounts = set()
            patterns = ['/usr/bin/nvidia-smi', '/usr/lib/nvidia']
            
            discover_files_by_patterns(
                patterns, volume_mounts, file_only=True
            )
            
            expected_mounts = {'/usr/bin/nvidia-smi:/usr/bin/nvidia-smi:ro'}
            self.assertEqual(volume_mounts, expected_mounts)
    
    @patch('glob.glob')
    def test_discover_files_empty_patterns(self, mock_glob):
        """Test file discovery with empty patterns list."""
        mock_glob.return_value = []
        
        volume_mounts = set()
        patterns = []
        
        discover_files_by_patterns(
            patterns, volume_mounts
        )
        
        self.assertEqual(volume_mounts, set())
        mock_glob.assert_not_called()
    
    @patch('glob.glob')
    @patch('os.path.exists')
    def test_discover_files_no_matches(self, mock_exists, mock_glob):
        """Test file discovery when no files match patterns."""
        mock_glob.return_value = []
        mock_exists.return_value = True
        
        volume_mounts = set()
        patterns = ['/usr/lib/nonexistent.so']
        
        discover_files_by_patterns(
            patterns, volume_mounts
        )
        
        self.assertEqual(volume_mounts, set())
    
    @patch('glob.glob')
    def test_discover_files_glob_exception_handling(self, mock_glob):
        """Test exception handling in file discovery."""
        mock_glob.side_effect = Exception("Glob error")
        
        volume_mounts = set()
        patterns = ['/usr/lib/libcuda.so']
        
        # Should not raise exception
        discover_files_by_patterns(
            patterns, volume_mounts, logger=self.logger
        )
        
        self.assertEqual(volume_mounts, set())
        self.logger.warning.assert_called()
    
    
    @patch('glob.glob')
    @patch('os.path.exists')
    def test_integration_with_original_gpu_detection(self, mock_exists, mock_glob):
        """Test integration with original GPU detection patterns."""
        # Simulate realistic GPU detection scenario
        mock_glob.side_effect = [
            # NVIDIA devices
            ['/dev/nvidia0', '/dev/nvidia1'],
            # NCCL installations
            ['/usr/local/nccl-2.12.12'],
            # Vulkan paths
            ['/etc/vulkan'],
            # RDMA paths
            ['/dev/infiniband'],
            # NVIDIA libraries
            ['/usr/lib/x86_64-linux-gnu/libcuda.so'],
            ['/usr/lib/i386-linux-gnu/libcuda.so'],
            ['/usr/lib64/libcuda.so'],
            ['/usr/lib/libcuda.so'],
            # Firmware files
            ['/usr/lib/firmware/nvidia/525.85.12/gsp_tu10x.bin'],
            # Config files
            ['/etc/vulkan/icd.d/nvidia_icd.json'],
            # Binaries
            ['/usr/bin/nvidia-smi'],
            # Xorg modules
            ['/usr/lib64/xorg/modules/drivers/nvidia_drv.so'],
            # System libraries
            ['/usr/local/cuda/compat/libcuda.so.525.85.12']
        ]
        mock_exists.return_value = True
        
        volume_mounts = set()
        
        
        # Test file discovery for various categories
        library_patterns = [
            "/usr/lib/x86_64-linux-gnu/libcuda.so",
            "/usr/lib/i386-linux-gnu/libcuda.so",
            "/usr/lib64/libcuda.so",
            "/usr/lib/libcuda.so",
        ]
        
        config_patterns = [
            "/etc/vulkan/icd.d/nvidia*.json",
        ]
        
        binary_patterns = [
            "/usr/bin/nvidia-smi",
        ]
        
        discover_files_by_patterns(
            library_patterns, volume_mounts
        )
        discover_files_by_patterns(
            config_patterns, volume_mounts
        )
        discover_files_by_patterns(
            binary_patterns, volume_mounts, file_only=True
        )
        
        # Verify results
        self.assertIn('/usr/lib/x86_64-linux-gnu/libcuda.so:/usr/lib/x86_64-linux-gnu/libcuda.so:ro', volume_mounts)
        # Note: binary_patterns won't be found due to mock setup, but libraries should be found


if __name__ == '__main__':
    unittest.main()