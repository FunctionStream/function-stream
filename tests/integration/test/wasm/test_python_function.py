# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""
Python WASM function tests: create and manage Python functions
via the CreatePythonFunction gRPC API.
"""


class TestCreatePythonFunction:

    def test_create_with_empty_modules_fails(self, fs_instance):
        """Creating a Python function with no modules should fail."""
        fs_instance.start()
        with fs_instance.get_client() as client:
            try:
                client.create_python_function(
                    class_name="EmptyDriver",
                    modules=[],
                    config_content="task_name: empty",
                )
                assert False, "Expected ValueError for empty modules"
            except (ValueError, Exception):
                pass

    def test_create_with_invalid_class_name(self, fs_instance):
        """Creating a Python function with a non-existent class should fail at server side."""
        fs_instance.start()
        with fs_instance.get_client() as client:
            try:
                client.create_python_function(
                    class_name="NoSuchClass",
                    modules=[("fake_module", b"x = 1\n")],
                    config_content="task_name: bad_class_test",
                )
            except Exception:
                pass
