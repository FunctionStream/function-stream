# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import datetime as dt
import json
import time
import uuid
from typing import Any, Dict, List

from .test_data_flow import consume_messages, produce_messages


def _uid(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def _sql_ok(fs_server: Any, sql: str) -> Any:
    resp = fs_server.execute_sql(sql)
    assert resp.status_code == 200, f"SQL failed: {sql}\nstatus={resp.status_code}\nmsg={resp.message}"
    return resp


class TestStreamingSqlKafka:
    @staticmethod
    def _create_impression_source(fs_server: Any, source_name: str, in_topic: str, bootstrap: str) -> None:
        _sql_ok(
            fs_server,
            f"""
            CREATE TABLE {source_name} (
                impression_id VARCHAR,
                ad_id BIGINT,
                campaign_id BIGINT,
                user_id VARCHAR,
                impression_time TIMESTAMP NOT NULL,
                WATERMARK FOR impression_time AS impression_time - INTERVAL '1' SECOND
            ) WITH (
                'connector' = 'kafka',
                'topic' = '{in_topic}',
                'format' = 'json',
                'scan.startup.mode' = 'earliest',
                'bootstrap.servers' = '{bootstrap}'
            );
            """,
        )

    @staticmethod
    def _create_click_source(fs_server: Any, source_name: str, in_topic: str, bootstrap: str) -> None:
        _sql_ok(
            fs_server,
            f"""
            CREATE TABLE {source_name} (
                click_id VARCHAR,
                impression_id VARCHAR,
                ad_id BIGINT,
                click_time TIMESTAMP NOT NULL,
                WATERMARK FOR click_time AS click_time - INTERVAL '1' SECOND
            ) WITH (
                'connector' = 'kafka',
                'topic' = '{in_topic}',
                'format' = 'json',
                'scan.startup.mode' = 'earliest',
                'bootstrap.servers' = '{bootstrap}'
            );
            """,
        )

    def test_tumble_window_with_kafka_produce_consume(
        self,
        fs_server: Any,
        kafka: Any,
        kafka_topics: str,
    ) -> None:
        source_name = _uid("ad_impressions_src")
        stream_name = _uid("metric_tumble_impressions")
        in_topic = _uid("topic_in")
        out_topic = _uid("topic_out")

        kafka.create_topics_if_not_exist([in_topic, out_topic])

        _sql_ok(
            fs_server,
            f"""
            CREATE TABLE {source_name} (
                impression_id VARCHAR,
                campaign_id BIGINT,
                impression_time TIMESTAMP NOT NULL,
                WATERMARK FOR impression_time AS impression_time - INTERVAL '1' SECOND
            ) WITH (
                'connector' = 'kafka',
                'topic' = '{in_topic}',
                'format' = 'json',
                'scan.startup.mode' = 'earliest',
                'bootstrap.servers' = '{kafka_topics}'
            );
            """,
        )

        _sql_ok(
            fs_server,
            f"""
            CREATE STREAMING TABLE {stream_name} WITH (
                'connector' = 'kafka',
                'topic' = '{out_topic}',
                'format' = 'json',
                'bootstrap.servers' = '{kafka_topics}'
            ) AS
            SELECT
                TUMBLE(INTERVAL '2' SECOND) AS time_window,
                campaign_id,
                COUNT(*) AS total_impressions
            FROM {source_name}
            GROUP BY 1, campaign_id;
            """,
        )

        now = dt.datetime.now(dt.timezone.utc)
        base = now.replace(microsecond=0) - dt.timedelta(seconds=8)
        old_window_msgs: List[Dict[str, Any]] = [
            {
                "impression_id": "i-1",
                "campaign_id": 1001,
                "impression_time": (base + dt.timedelta(milliseconds=100)).isoformat(),
            },
            {
                "impression_id": "i-2",
                "campaign_id": 1001,
                "impression_time": (base + dt.timedelta(milliseconds=500)).isoformat(),
            },
            {
                "impression_id": "i-3",
                "campaign_id": 1002,
                "impression_time": (base + dt.timedelta(milliseconds=900)).isoformat(),
            },
        ]
        advance_wm = {
            "impression_id": "i-4",
            "campaign_id": 9999,
            "impression_time": dt.datetime.now(dt.timezone.utc).isoformat(),
        }

        produce_messages(kafka_topics, in_topic, [json.dumps(x) for x in old_window_msgs + [advance_wm]])
        time.sleep(1.0)

        records = consume_messages(kafka_topics, out_topic, expected_count=2, timeout=15.0)
        got = {(int(r["campaign_id"]), int(r["total_impressions"])) for r in records}
        assert got == {(1001, 2), (1002, 1)}

        _sql_ok(fs_server, f"DROP STREAMING TABLE {stream_name};")

    def test_hop_window_with_where_filter(
        self,
        fs_server: Any,
        kafka: Any,
        kafka_topics: str,
    ) -> None:
        source_name = _uid("ad_impressions_src")
        stream_name = _uid("metric_hop_uv")
        in_topic = _uid("topic_in")
        out_topic = _uid("topic_out")
        kafka.create_topics_if_not_exist([in_topic, out_topic])
        self._create_impression_source(fs_server, source_name, in_topic, kafka_topics)

        _sql_ok(
            fs_server,
            f"""
            CREATE STREAMING TABLE {stream_name} WITH (
                'connector' = 'kafka',
                'topic' = '{out_topic}',
                'format' = 'json',
                'bootstrap.servers' = '{kafka_topics}'
            ) AS
            SELECT
                HOP(INTERVAL '1' SECOND, INTERVAL '4' SECOND) AS time_window,
                ad_id,
                COUNT(*) AS kept_rows
            FROM {source_name}
            WHERE campaign_id = 2001
            GROUP BY 1, ad_id;
            """,
        )

        now = dt.datetime.now(dt.timezone.utc)
        base = now.replace(microsecond=0) - dt.timedelta(seconds=8)
        msgs = [
            {"impression_id": "h1", "ad_id": 11, "campaign_id": 2001, "user_id": "u1",
             "impression_time": (base + dt.timedelta(milliseconds=100)).isoformat()},
            {"impression_id": "h2", "ad_id": 11, "campaign_id": 2002, "user_id": "u2",
             "impression_time": (base + dt.timedelta(milliseconds=300)).isoformat()},
            {"impression_id": "h3", "ad_id": 12, "campaign_id": 2001, "user_id": "u3",
             "impression_time": (base + dt.timedelta(milliseconds=600)).isoformat()},
            {"impression_id": "h4", "ad_id": 999, "campaign_id": 9999, "user_id": "wm",
             "impression_time": dt.datetime.now(dt.timezone.utc).isoformat()},
        ]
        produce_messages(kafka_topics, in_topic, [json.dumps(x) for x in msgs])
        rows = consume_messages(kafka_topics, out_topic, expected_count=2, timeout=15.0)
        got = {(int(r["ad_id"]), int(r["kept_rows"])) for r in rows}
        assert got == {(11, 1), (12, 1)}
        _sql_ok(fs_server, f"DROP STREAMING TABLE {stream_name};")

    def test_session_window_user_activity(
        self,
        fs_server: Any,
        kafka: Any,
        kafka_topics: str,
    ) -> None:
        source_name = _uid("ad_impressions_src")
        stream_name = _uid("metric_session_impr")
        in_topic = _uid("topic_in")
        out_topic = _uid("topic_out")
        kafka.create_topics_if_not_exist([in_topic, out_topic])
        self._create_impression_source(fs_server, source_name, in_topic, kafka_topics)

        _sql_ok(
            fs_server,
            f"""
            CREATE STREAMING TABLE {stream_name} WITH (
                'connector' = 'kafka',
                'topic' = '{out_topic}',
                'format' = 'json',
                'bootstrap.servers' = '{kafka_topics}'
            ) AS
            SELECT
                SESSION(INTERVAL '2' SECOND) AS time_window,
                user_id,
                COUNT(*) AS impressions_in_session
            FROM {source_name}
            GROUP BY 1, user_id;
            """,
        )

        now = dt.datetime.now(dt.timezone.utc)
        base = now.replace(microsecond=0) - dt.timedelta(seconds=10)
        msgs = [
            {"impression_id": "s1", "ad_id": 1, "campaign_id": 1, "user_id": "uA",
             "impression_time": (base + dt.timedelta(milliseconds=100)).isoformat()},
            {"impression_id": "s2", "ad_id": 1, "campaign_id": 1, "user_id": "uA",
             "impression_time": (base + dt.timedelta(milliseconds=900)).isoformat()},
            {"impression_id": "s3", "ad_id": 2, "campaign_id": 1, "user_id": "uB",
             "impression_time": (base + dt.timedelta(milliseconds=1200)).isoformat()},
            {"impression_id": "s4", "ad_id": 999, "campaign_id": 9999, "user_id": "wm",
             "impression_time": dt.datetime.now(dt.timezone.utc).isoformat()},
        ]
        produce_messages(kafka_topics, in_topic, [json.dumps(x) for x in msgs])
        rows = consume_messages(kafka_topics, out_topic, expected_count=2, timeout=15.0)
        got = {(r["user_id"], int(r["impressions_in_session"])) for r in rows}
        assert got == {("uA", 2), ("uB", 1)}
        _sql_ok(fs_server, f"DROP STREAMING TABLE {stream_name};")