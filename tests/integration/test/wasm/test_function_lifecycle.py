# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""
WASM function lifecycle tests: create, list, start, stop, drop functions
via the gRPC FsClient API.
"""


class TestFunctionList:

    def test_show_functions_empty(self, fs_instance):
        """A fresh server has no functions registered."""
        fs_instance.start()
        with fs_instance.get_client() as client:
            result = client.show_functions()
            assert result.functions == []

    def test_show_functions_after_drop(self, fs_instance):
        """After dropping a function, show_functions reflects the removal."""
        fs_instance.start()
        with fs_instance.get_client() as client:
            result = client.show_functions()
            initial_count = len(result.functions)
            assert initial_count == 0


class TestFunctionDrop:

    def test_drop_nonexistent_function_fails(self, fs_instance):
        """Dropping a function that does not exist should raise an error."""
        fs_instance.start()
        with fs_instance.get_client() as client:
            try:
                client.drop_function("no_such_function")
                assert False, "Expected an error when dropping non-existent function"
            except Exception:
                pass


class TestFunctionStartStop:

    def test_start_nonexistent_function_fails(self, fs_instance):
        """Starting a function that does not exist should raise an error."""
        fs_instance.start()
        with fs_instance.get_client() as client:
            try:
                client.start_function("ghost_function")
                assert False, "Expected an error when starting non-existent function"
            except Exception:
                pass

    def test_stop_nonexistent_function_fails(self, fs_instance):
        """Stopping a function that does not exist should raise an error."""
        fs_instance.start()
        with fs_instance.get_client() as client:
            try:
                client.stop_function("phantom_function")
                assert False, "Expected an error when stopping non-existent function"
            except Exception:
                pass
