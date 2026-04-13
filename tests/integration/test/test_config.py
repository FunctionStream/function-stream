# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""
Configuration tests: verify config overrides take effect,
workspace directories and config files are created correctly.
"""

import yaml


class TestConfigOverride:

    def test_debug_mode(self, fs_instance):
        """service.debug override is written to config.yaml and server starts."""
        fs_instance.configure(**{
            "service.debug": True,
            "logging.level": "debug",
        }).start()

        assert fs_instance.is_running

        written = yaml.safe_load(fs_instance.workspace.config_file.read_text())
        assert written["service"]["debug"] is True
        assert written["logging"]["level"] == "debug"

    def test_config_port_matches_instance(self, fs_instance):
        """The port in config.yaml must match the instance's allocated port."""
        fs_instance.workspace.setup()
        fs_instance.config.write_to_workspace()

        written = yaml.safe_load(fs_instance.workspace.config_file.read_text())
        assert written["service"]["port"] == fs_instance.port

    def test_nested_override(self, fs_instance):
        """Deep dot-separated keys create nested YAML structures."""
        fs_instance.configure(**{
            "state_storage.storage_type": "rocksdb",
            "state_storage.base_dir": "/tmp/custom",
        })
        fs_instance.workspace.setup()
        fs_instance.config.write_to_workspace()

        written = yaml.safe_load(fs_instance.workspace.config_file.read_text())
        assert written["state_storage"]["storage_type"] == "rocksdb"
        assert written["state_storage"]["base_dir"] == "/tmp/custom"


class TestWorkspaceLayout:

    def test_directories_created(self, fs_instance):
        """setup() creates conf/, data/, logs/ directories."""
        fs_instance.workspace.setup()

        assert fs_instance.workspace.conf_dir.exists()
        assert fs_instance.workspace.data_dir.exists()
        assert fs_instance.workspace.log_dir.exists()

    def test_instance_dir_name_contains_port(self, fs_instance):
        """Instance root directory is named FunctionStream-<port>."""
        expected_suffix = f"FunctionStream-{fs_instance.port}"
        assert fs_instance.workspace.root_dir.name == expected_suffix

    def test_log_files_preserved_after_stop(self, fs_instance):
        """After shutdown, log files still exist on disk."""
        fs_instance.start()
        log_dir = fs_instance.workspace.log_dir
        fs_instance.stop()

        assert log_dir.exists()
        assert fs_instance.workspace.stdout_file.exists()
        assert fs_instance.workspace.stderr_file.exists()

    def test_cleanup_data_preserves_logs(self, fs_instance):
        """cleanup_data() removes data/ but keeps logs/."""
        fs_instance.start()

        data_marker = fs_instance.workspace.data_dir / "marker.txt"
        data_marker.write_text("should be deleted")

        fs_instance.workspace.cleanup_data()

        assert not data_marker.exists()
        assert fs_instance.workspace.data_dir.exists()
        assert fs_instance.workspace.log_dir.exists()
