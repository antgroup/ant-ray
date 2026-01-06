#!/usr/bin/env python3
"""
Unit tests for _modify_container_context_impl function in image_uri.py
"""

import os
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch, MagicMock
from typing import Dict, Any

# Add the necessary paths
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '_private'))

# Import the function directly
import importlib.util
spec = importlib.util.spec_from_file_location(
    "image_uri", 
    os.path.join(os.path.dirname(__file__), '..', '_private', 'runtime_env', 'image_uri.py')
)
image_uri_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(image_uri_module)


class MockRuntimeEnv:
    """Mock RuntimeEnv class for testing."""
    
    def __init__(self, config: Dict[str, Any]):
        self._config = config
    
    def py_container_image(self):
        return self._config.get("container", {}).get("image")
    
    def get(self, key, default=None):
        return self._config.get(key, default)
    
    def py_container_install_ray(self):
        return self._config.get("container", {}).get("install_ray", False)
    
    def py_container_isolate_pip_installation(self):
        return self._config.get("container", {}).get("isolate_pip_installation", False)
    
    def py_container_pip_list(self):
        return self._config.get("container", {}).get("pip", [])
    
    def py_container_worker_path(self):
        return self._config.get("container", {}).get("worker_path")
    
    def get_serialized_allocated_instances(self):
        return None
    
    def pip_config(self):
        return {"packages": self._config.get("pip", [])}


class MockContext:
    """Mock RuntimeEnvContext for testing."""
    
    def __init__(self):
        self.override_worker_entrypoint = None
        self.container = {}
        self.env_vars = {}
        self.working_dir = None
        self.py_executable = None
        self.native_libraries = {"code_search_path": []}


class TestModifyContainerContextImpl(unittest.TestCase):
    """Test cases for _modify_container_context_impl function."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.context = MockContext()
        self.ray_tmp_dir = "/tmp/ray"
        self.worker_path = "/usr/local/lib/python3.8/site-packages/ray/_private/workers/default_worker.py"
        self.logger = Mock()
    
    def test_no_container_image_returns_early(self):
        """Test that function returns early when no container image is specified."""
        runtime_env = MockRuntimeEnv({})
        
        original_context = MockContext()
        original_context.__dict__.update(self.context.__dict__)
        
        image_uri_module._modify_container_context_impl(
            runtime_env=runtime_env,
            context=self.context,
            ray_tmp_dir=self.ray_tmp_dir,
            worker_path=self.worker_path,
            logger=self.logger
        )
        
        # Context should remain unchanged
        self.assertIsNone(self.context.override_worker_entrypoint)
        self.assertEqual(self.context.container, {})
    
    def test_basic_container_setup(self):
        """Test basic container setup with minimal configuration."""
        runtime_env = MockRuntimeEnv({
            "container": {
                "image": "rayproject/ray:latest"
            }
        })
        
        image_uri_module._modify_container_context_impl(
            runtime_env=runtime_env,
            context=self.context,
            ray_tmp_dir=self.ray_tmp_dir,
            worker_path=self.worker_path,
            logger=self.logger
        )
        
        self.assertEqual(self.context.override_worker_entrypoint, self.worker_path)
        self.assertIn("container_command", self.context.container)
        command = self.context.container["container_command"]
        
        # Check basic command structure
        self.assertEqual(command[0], "podman")
        self.assertIn("run", command)
        self.assertIn("rayproject/ray:latest", command)
    
    def test_container_with_custom_user(self):
        """Test container setup with custom user."""
        runtime_env = MockRuntimeEnv({
            "container": {
                "image": "rayproject/ray:latest",
                "user": "custom_user"
            }
        })
        
        image_uri_module._modify_container_context_impl(
            runtime_env=runtime_env,
            context=self.context,
            ray_tmp_dir=self.ray_tmp_dir,
            worker_path=self.worker_path,
            logger=self.logger
        )
        
        command = self.context.container["container_command"]
        self.assertIn("-u", command)
        user_index = command.index("-u")
        self.assertEqual(command[user_index + 1], "custom_user")
    
    def test_container_with_sudo(self):
        """Test container setup with sudo enabled."""
        runtime_env = MockRuntimeEnv({
            "container": {
                "image": "rayproject/ray:latest",
                "sudo": True
            }
        })
        
        image_uri_module._modify_container_context_impl(
            runtime_env=runtime_env,
            context=self.context,
            ray_tmp_dir=self.ray_tmp_dir,
            worker_path=self.worker_path,
            logger=self.logger
        )
        
        command = self.context.container["container_command"]
        self.assertEqual(command[0], "sudo")
        self.assertEqual(command[1], "-E")
    
    def test_container_with_custom_working_dir(self):
        """Test container setup with custom working directory."""
        runtime_env = MockRuntimeEnv({
            "container": {
                "image": "rayproject/ray:latest"
            }
        })
        self.context.working_dir = "/custom/working/dir"
        
        image_uri_module._modify_container_context_impl(
            runtime_env=runtime_env,
            context=self.context,
            ray_tmp_dir=self.ray_tmp_dir,
            worker_path=self.worker_path,
            logger=self.logger
        )
        
        command = self.context.container["container_command"]
        self.assertIn("-w", command)
        workdir_index = command.index("-w")
        self.assertEqual(command[workdir_index + 1], "/custom/working/dir")
    
    def test_container_with_py_executable(self):
        """Test container setup with custom Python executable."""
        runtime_env = MockRuntimeEnv({
            "container": {
                "image": "rayproject/ray:latest",
                "py_executable": "/usr/bin/python3.9"
            }
        })
        
        image_uri_module._modify_container_context_impl(
            runtime_env=runtime_env,
            context=self.context,
            ray_tmp_dir=self.ray_tmp_dir,
            worker_path=self.worker_path,
            logger=self.logger
        )
        
        self.assertEqual(self.context.py_executable, "/usr/bin/python3.9")
    
    def test_container_with_run_options(self):
        """Test container setup with custom run options."""
        runtime_env = MockRuntimeEnv({
            "container": {
                "image": "rayproject/ray:latest",
                "run_options": ["--memory=1g", "--cpus=2"]
            }
        })
        
        image_uri_module._modify_container_context_impl(
            runtime_env=runtime_env,
            context=self.context,
            ray_tmp_dir=self.ray_tmp_dir,
            worker_path=self.worker_path,
            logger=self.logger
        )
        
        command = self.context.container["container_command"]
        self.assertIn("--memory=1g", command)
        self.assertIn("--cpus=2", command)
    
    def test_container_with_pip_packages(self):
        """Test container setup with pip packages."""
        runtime_env = MockRuntimeEnv({
            "container": {
                "install_ray": True,
                "image": "rayproject/ray:latest"
            },
            "pip": ["requests", "numpy"]
        })
        
        image_uri_module._modify_container_context_impl(
            runtime_env=runtime_env,
            context=self.context,
            ray_tmp_dir=self.ray_tmp_dir,
            worker_path=self.worker_path,
            logger=self.logger
        )
        self.assertIn("entrypoint_prefix", self.context.container)
        entrypoint_prefix = self.context.container["entrypoint_prefix"]
        self.assertIn("python", entrypoint_prefix)
        self.assertIn("/tmp/scripts/dependencies_installer.py", entrypoint_prefix)

    def test_container_with_pip_packages_not_install_ray(self):
        """Test container setup with pip packages."""
        runtime_env = MockRuntimeEnv({
            "container": {
                "install_ray": False,
                "image": "rayproject/ray:latest"
            },
            "pip": ["requests", "numpy"]
        })
        image_uri_module._modify_container_context_impl(
            runtime_env=runtime_env,
            context=self.context,
            ray_tmp_dir=self.ray_tmp_dir,
            worker_path=self.worker_path,
            logger=self.logger
        )
        self.assertNotIn("entrypoint_prefix", self.context.container)

    def test_container_with_inside_pip_packages(self):
        """Test container setup with pip packages."""
        runtime_env = MockRuntimeEnv({
            "container": {
                "install_ray": True,
                "image": "rayproject/ray:latest",
                "pip": ["requests", "numpy"]
            }
        })

        image_uri_module._modify_container_context_impl(
            runtime_env=runtime_env,
            context=self.context,
            ray_tmp_dir=self.ray_tmp_dir,
            worker_path=self.worker_path,
            logger=self.logger
        )
        self.assertIn("entrypoint_prefix", self.context.container)
        entrypoint_prefix = self.context.container["entrypoint_prefix"]
        self.assertIn("python", entrypoint_prefix)
        self.assertIn("/tmp/scripts/dependencies_installer.py",
                      entrypoint_prefix)
    
    def test_container_with_install_ray_true(self):
        """Test container setup with install_ray=True."""
        runtime_env = MockRuntimeEnv({
            "container": {
                "image": "rayproject/ray:latest",
                "install_ray": True
            }
        })
        
        image_uri_module._modify_container_context_impl(
            runtime_env=runtime_env,
            context=self.context,
            ray_tmp_dir=self.ray_tmp_dir,
            worker_path=self.worker_path,
            logger=self.logger
        )
        
        self.assertIn("entrypoint_prefix", self.context.container)
    
    def test_mutually_exclusive_install_ray_and_isolate_pip(self):
        """Test that install_ray and isolate_pip_installation cannot both be True."""
        runtime_env = MockRuntimeEnv({
            "container": {
                "image": "rayproject/ray:latest",
                "install_ray": True,
                "isolate_pip_installation": True
            }
        })
        
        with self.assertRaises(ValueError) as context:
            image_uri_module._modify_container_context_impl(
                runtime_env=runtime_env,
                context=self.context,
                ray_tmp_dir=self.ray_tmp_dir,
                worker_path=self.worker_path,
                logger=self.logger
            )
        
        self.assertIn("can't both be True", str(context.exception))
    
    @patch('os.path.exists')
    @patch('glob.glob')
    def test_gpu_support_detection(self, mock_glob, mock_exists):
        """Test GPU support detection and mounting."""
        # Mock GPU device detection
        mock_exists.return_value = True
        mock_glob.return_value = ["/dev/nvidia0", "/dev/nvidia1"]
        
        runtime_env = MockRuntimeEnv({
            "container": {
                "image": "rayproject/ray:latest"
            }
        })
        
        with patch.dict(os.environ, {'NVIDIA_VISIBLE_DEVICES': 'all'}):
            image_uri_module._modify_container_context_impl(
                runtime_env=runtime_env,
                context=self.context,
                ray_tmp_dir=self.ray_tmp_dir,
                worker_path=self.worker_path,
                logger=self.logger
            )
        
        command = self.context.container["container_command"]
        self.assertIn("--privileged", command)
        command_str = " ".join(command)
        self.assertIn("/dev/nvidia0:/dev/nvidia0", command_str)
        self.assertIn("/dev/nvidia1:/dev/nvidia1", command_str)
    
    def test_native_libraries_support(self):
        """Test native libraries configuration."""
        runtime_env = MockRuntimeEnv({
            "container": {
                "image": "rayproject/ray:latest",
                "native_libraries": "/usr/local/lib"
            }
        })
        
        image_uri_module._modify_container_context_impl(
            runtime_env=runtime_env,
            context=self.context,
            ray_tmp_dir=self.ray_tmp_dir,
            worker_path=self.worker_path,
            logger=self.logger
        )
        
        self.assertIn("native_libraries", self.context.__dict__)
        self.assertIn("/usr/local/lib", self.context.native_libraries["code_search_path"])
    
    def test_env_vars_propagation(self):
        """Test environment variables propagation."""
        runtime_env = MockRuntimeEnv({
            "container": {
                "image": "rayproject/ray:latest"
            }
        })
        self.context.env_vars = {"TEST_VAR": "test_value"}
        
        image_uri_module._modify_container_context_impl(
            runtime_env=runtime_env,
            context=self.context,
            ray_tmp_dir=self.ray_tmp_dir,
            worker_path=self.worker_path,
            logger=self.logger
        )
        
        self.assertIn("TEST_VAR", self.context.env_vars)
        self.assertEqual(self.context.env_vars["TEST_VAR"], "test_value")


if __name__ == '__main__':
    unittest.main()