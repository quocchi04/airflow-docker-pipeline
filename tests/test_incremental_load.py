import unittest
from contextlib import ExitStack
from unittest.mock import Mock, patch

import pandas as pd
from sqlalchemy import Column, DateTime, Float, MetaData, Table, create_engine, inspect, text
from sqlalchemy.dialects import postgresql

from scripts.services import load_data
from scripts.utils.db_utils import table_is_empty


class IncrementalLoadTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite://")
        self.conn = self.engine.connect()
        self.conn.execute(text("CREATE TABLE weather (time TIMESTAMP, temperature_2m FLOAT)"))

    def tearDown(self):
        self.conn.close()
        self.engine.dispose()

    def test_empty_table_and_watermark(self):
        self.assertTrue(table_is_empty(self.conn, "main", "weather"))
        self.assertIsNone(load_data.get_last_modified(self.conn, "weather", "main"))
        self.conn.execute(text("INSERT INTO weather VALUES ('2026-11-16 03:00:00', 30)"))
        self.assertFalse(table_is_empty(self.conn, "main", "weather"))
        self.assertEqual(
            load_data.get_last_modified(self.conn, "weather", "main", minutes_back=20),
            pd.Timestamp("2026-11-16T02:40:00Z"),
        )

    def test_filter_old_equal_and_new_times_and_forward_params(self):
        watermark = pd.Timestamp("2026-11-16T03:00:00Z")
        params = {"latitude": 10.8231, "current": "temperature_2m"}
        for offset, expected_count in [(-60, 0), (0, 1), (60, 1)]:
            with self.subTest(offset=offset), ExitStack() as stack:
                stack.enter_context(patch.object(load_data, "check_if_table_exists", return_value=True))
                stack.enter_context(patch.object(load_data, "table_is_empty", return_value=False))
                stack.enter_context(patch.object(load_data, "get_last_modified", return_value=watermark))
                fetch = stack.enter_context(patch.object(load_data, "fetch_md_data", return_value={
                    "current": {"time": int(watermark.timestamp()) + offset, "temperature_2m": 31}
                }))
                rows, actual_watermark = load_data.fetch_new_data(
                    self.conn, "https://example.test", "main", "weather", params=params
                )
                self.assertEqual(len(rows), expected_count)
                self.assertEqual(actual_watermark, watermark)
                fetch.assert_called_once_with("https://example.test", params=params)

    def test_initial_load_for_missing_or_empty_table(self):
        for exists in [False, True]:
            with self.subTest(exists=exists), ExitStack() as stack:
                stack.enter_context(patch.object(load_data, "check_if_table_exists", return_value=exists))
                stack.enter_context(patch.object(load_data, "table_is_empty", return_value=True))
                get_watermark = stack.enter_context(patch.object(load_data, "get_last_modified"))
                stack.enter_context(patch.object(load_data, "fetch_md_data", return_value={
                    "current": {"time": 1794798000}
                }))
                rows, watermark = load_data.fetch_new_data(self.conn, "api", "main", "weather")
                self.assertEqual(len(rows), 1)
                self.assertIsNone(watermark)
                get_watermark.assert_not_called()

    def test_upsert_uses_unique_key_and_updates_non_key_columns(self):
        table = Table("weather", MetaData(), Column("time", DateTime(timezone=True)),
                      Column("temperature_2m", Float), schema="raw")
        conn = Mock()
        with patch.object(load_data, "Table", return_value=table), patch.object(load_data.Index, "create") as create_index:
            load_data.upsert(conn, pd.DataFrame([{
                "time": pd.Timestamp("2026-11-16T03:00:00Z"), "temperature_2m": 31,
            }]), "raw", "weather", ["time"])
        create_index.assert_called_once_with(bind=conn, checkfirst=True)
        self.assertTrue(next(iter(table.indexes)).unique)
        sql = str(conn.execute.call_args.args[0].compile(dialect=postgresql.dialect()))
        self.assertIn("ON CONFLICT (time) DO UPDATE SET temperature_2m = excluded.temperature_2m", sql)
        self.assertNotIn("SET time =", sql)

    def test_incremental_bootstraps_table_and_converts_timestamp(self):
        self.conn.close()
        with ExitStack() as stack:
            stack.enter_context(patch.object(load_data, "establish_wh_connection", return_value=self.engine))
            dispose = stack.enter_context(patch.object(self.engine, "dispose"))
            stack.enter_context(patch.object(load_data, "fetch_md_data", return_value={
                "current": {"time": 1794798000, "temperature_2m": 31}
            }))
            stack.enter_context(patch.object(load_data, "check_if_table_exists", return_value=False))
            upsert = stack.enter_context(patch.object(load_data, "upsert"))
            load_data.incremental_load("api", "new_weather", "main", "time")
            self.assertTrue(inspect(self.engine).has_table("new_weather"))
            frame = upsert.call_args.args[1]
            self.assertEqual(str(frame["time"].dtype), "datetime64[ns, UTC]")
            self.assertIn("execution_time", frame.columns)
            dispose.assert_called_once()
        self.conn = self.engine.connect()

    def test_missing_destination_key_rolls_back_and_disposes(self):
        with ExitStack() as stack:
            stack.enter_context(patch.object(load_data, "establish_wh_connection", return_value=self.engine))
            dispose = stack.enter_context(patch.object(self.engine, "dispose"))
            stack.enter_context(patch.object(load_data, "fetch_new_data", return_value=([{"time": 1794798000}], None)))
            stack.enter_context(patch.object(load_data, "check_if_table_exists", return_value=True))
            stack.enter_context(patch.object(load_data, "get_existing_columns", return_value=["temperature_2m"]))
            upsert = stack.enter_context(patch.object(load_data, "upsert"))
            with self.assertRaisesRegex(ValueError, "Missing required incremental columns"):
                load_data.incremental_load("api", "weather", "main", "time")
            upsert.assert_not_called()
            dispose.assert_called_once()


if __name__ == "__main__":
    unittest.main()
