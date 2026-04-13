# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""
Client access tests: verify gRPC connectivity, SQL execution,
and basic CRUD via the Python FsClient.
"""


class TestClientConnect:

    def test_client_target_matches(self, fs_instance):
        """FsClient.target points to the correct host:port."""
        fs_instance.start()
        with fs_instance.get_client() as client:
            assert client.target == f"{fs_instance.host}:{fs_instance.port}"

    def test_show_functions_empty_on_fresh_server(self, fs_instance):
        """A fresh instance has no functions registered."""
        fs_instance.start()
        with fs_instance.get_client() as client:
            result = client.show_functions()
            assert result.functions == []


class TestSqlExecution:

    def test_execute_show_tables(self, fs_instance):
        """SHOW TABLES on a fresh instance returns a successful response."""
        fs_instance.start()
        response = fs_instance.execute_sql("SHOW TABLES")
        assert response.status_code < 400

    def test_execute_invalid_sql(self, fs_instance):
        """Invalid SQL returns an error status code (>= 400)."""
        fs_instance.start()
        response = fs_instance.execute_sql("THIS IS NOT SQL")
        assert response.status_code >= 400
