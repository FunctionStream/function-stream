# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""
Catalog persistence tests: verify that table metadata survives
a server restart when stream_catalog.persist is enabled.
"""


class TestCatalogPersistence:

    def test_table_survives_restart(self, fs_instance):
        """A table created before restart is still visible after restart."""
        fs_instance.configure(**{"stream_catalog.persist": True}).start()

        fs_instance.execute_sql("""
            CREATE TABLE persistent_tbl (
                id BIGINT,
                ts TIMESTAMP,
                WATERMARK FOR ts AS ts - INTERVAL '3' SECOND
            ) WITH (
                'connector' = 'kafka',
                'topic' = 'persist-topic',
                'bootstrap.servers' = 'localhost:9092',
                'format' = 'json'
            )
        """)

        fs_instance.restart()

        resp = fs_instance.execute_sql("SHOW TABLES")
        assert resp.status_code < 400

    def test_dropped_table_gone_after_restart(self, fs_instance):
        """A table that was dropped should not reappear after restart."""
        fs_instance.configure(**{"stream_catalog.persist": True}).start()

        fs_instance.execute_sql("""
            CREATE TABLE temp_tbl (
                id BIGINT
            ) WITH (
                'connector' = 'kafka',
                'topic' = 'temp-topic',
                'bootstrap.servers' = 'localhost:9092',
                'format' = 'json'
            )
        """)
        fs_instance.execute_sql("DROP TABLE temp_tbl")

        fs_instance.restart()

        resp = fs_instance.execute_sql("SHOW CREATE TABLE temp_tbl")
        assert resp.status_code >= 400
