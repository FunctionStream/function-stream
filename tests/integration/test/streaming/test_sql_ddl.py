# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""
Streaming SQL DDL tests: CREATE TABLE, DROP TABLE, SHOW TABLES,
SHOW CREATE TABLE, CREATE STREAMING TABLE.
"""


class TestShowTables:

    def test_show_tables_empty(self, fs_instance):
        """SHOW TABLES returns success on a fresh instance."""
        fs_instance.start()
        resp = fs_instance.execute_sql("SHOW TABLES")
        assert resp.status_code < 400

    def test_show_streaming_tables_empty(self, fs_instance):
        """SHOW STREAMING TABLES returns success on a fresh instance."""
        fs_instance.start()
        resp = fs_instance.execute_sql("SHOW STREAMING TABLES")
        assert resp.status_code < 400


class TestCreateTable:

    def test_create_source_table(self, fs_instance):
        """CREATE TABLE with connector options registers a source table."""
        fs_instance.start()

        create_sql = """
            CREATE TABLE test_source (
                id BIGINT,
                name VARCHAR,
                event_time TIMESTAMP,
                WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
            ) WITH (
                'connector' = 'kafka',
                'topic' = 'test-topic',
                'bootstrap.servers' = 'localhost:9092',
                'format' = 'json'
            )
        """
        resp = fs_instance.execute_sql(create_sql)
        assert resp.status_code < 400

        show_resp = fs_instance.execute_sql("SHOW TABLES")
        assert show_resp.status_code < 400

    def test_create_duplicate_table_fails(self, fs_instance):
        """Creating the same table twice should return an error."""
        fs_instance.start()

        create_sql = """
            CREATE TABLE dup_table (
                id BIGINT
            ) WITH (
                'connector' = 'kafka',
                'topic' = 'dup-topic',
                'bootstrap.servers' = 'localhost:9092',
                'format' = 'json'
            )
        """
        resp1 = fs_instance.execute_sql(create_sql)
        assert resp1.status_code < 400

        resp2 = fs_instance.execute_sql(create_sql)
        assert resp2.status_code >= 400


class TestDropTable:

    def test_drop_existing_table(self, fs_instance):
        """DROP TABLE removes a previously created table."""
        fs_instance.start()

        fs_instance.execute_sql("""
            CREATE TABLE to_drop (
                id BIGINT
            ) WITH (
                'connector' = 'kafka',
                'topic' = 'drop-topic',
                'bootstrap.servers' = 'localhost:9092',
                'format' = 'json'
            )
        """)

        resp = fs_instance.execute_sql("DROP TABLE to_drop")
        assert resp.status_code < 400

    def test_drop_nonexistent_table_fails(self, fs_instance):
        """DROP TABLE on a non-existent table returns an error."""
        fs_instance.start()
        resp = fs_instance.execute_sql("DROP TABLE no_such_table")
        assert resp.status_code >= 400

    def test_drop_if_exists_nonexistent_succeeds(self, fs_instance):
        """DROP TABLE IF EXISTS on a non-existent table should succeed."""
        fs_instance.start()
        resp = fs_instance.execute_sql("DROP TABLE IF EXISTS no_such_table")
        assert resp.status_code < 400


class TestShowCreateTable:

    def test_show_create_table(self, fs_instance):
        """SHOW CREATE TABLE returns DDL for an existing table."""
        fs_instance.start()

        fs_instance.execute_sql("""
            CREATE TABLE show_me (
                id BIGINT,
                value VARCHAR
            ) WITH (
                'connector' = 'kafka',
                'topic' = 'show-topic',
                'bootstrap.servers' = 'localhost:9092',
                'format' = 'json'
            )
        """)

        resp = fs_instance.execute_sql("SHOW CREATE TABLE show_me")
        assert resp.status_code < 400

    def test_show_create_nonexistent_fails(self, fs_instance):
        """SHOW CREATE TABLE on a missing table returns an error."""
        fs_instance.start()
        resp = fs_instance.execute_sql("SHOW CREATE TABLE ghost_table")
        assert resp.status_code >= 400


class TestSqlErrorHandling:

    def test_invalid_sql_syntax(self, fs_instance):
        """Malformed SQL returns an error status."""
        fs_instance.start()
        resp = fs_instance.execute_sql("NOT VALID SQL AT ALL")
        assert resp.status_code >= 400

    def test_empty_sql(self, fs_instance):
        """Empty SQL string returns an error status."""
        fs_instance.start()
        resp = fs_instance.execute_sql("")
        assert resp.status_code >= 400
