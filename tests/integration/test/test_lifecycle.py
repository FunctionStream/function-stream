# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""
Lifecycle tests: start, stop, kill, restart, parallel instances.
"""

from framework import FunctionStreamInstance


class TestStartAndStop:

    def test_start_and_graceful_stop(self, fs_instance):
        """Server starts on a random port and stops gracefully via SIGTERM."""
        fs_instance.start()

        assert fs_instance.is_running
        assert fs_instance.port > 0
        assert fs_instance.pid is not None

        fs_instance.stop()
        assert not fs_instance.is_running

    def test_force_kill(self, fs_instance):
        """Server can be forcefully killed via SIGKILL."""
        fs_instance.start()
        assert fs_instance.is_running

        fs_instance.kill()
        assert not fs_instance.is_running


class TestRestart:

    def test_restart_preserves_port(self, fs_instance):
        """After restart, port stays the same but PID changes."""
        fs_instance.start()
        original_pid = fs_instance.pid
        original_port = fs_instance.port

        fs_instance.restart()

        assert fs_instance.is_running
        assert fs_instance.port == original_port
        assert fs_instance.pid != original_pid

    def test_restart_server_responds(self, fs_instance):
        """After restart, the server still accepts gRPC requests."""
        fs_instance.start()
        fs_instance.restart()

        with fs_instance.get_client() as client:
            result = client.show_functions()
            assert result.functions == []


class TestParallel:

    def test_multiple_instances_run_simultaneously(self):
        """Two independent instances can run on different ports at the same time."""
        inst1 = FunctionStreamInstance(test_name="test_parallel_a")
        inst2 = FunctionStreamInstance(test_name="test_parallel_b")
        try:
            inst1.start()
            inst2.start()

            assert inst1.is_running
            assert inst2.is_running
            assert inst1.port != inst2.port
        finally:
            inst1.kill()
            inst2.kill()
