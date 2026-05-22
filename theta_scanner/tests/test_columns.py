"""Tests for the column catalog."""
from __future__ import annotations

import unittest

from theta_scanner import columns as cols


class CatalogTests(unittest.TestCase):
    def test_no_duplicate_keys(self):
        keys = [c.key for c in cols.CATALOG]
        self.assertEqual(len(keys), len(set(keys)))

    def test_default_layout_keys_all_valid(self):
        for key in cols.DEFAULT_LAYOUT:
            self.assertIsNotNone(cols.get(key), f"{key} not in catalog")

    def test_default_layout_no_duplicates(self):
        self.assertEqual(len(cols.DEFAULT_LAYOUT), len(set(cols.DEFAULT_LAYOUT)))

    def test_all_keys_covers_catalog(self):
        self.assertEqual(set(cols.all_keys()), {c.key for c in cols.CATALOG})

    def test_keys_by_category_partitions_catalog(self):
        grouped = cols.keys_by_category()
        flat = [k for keys in grouped.values() for k in keys]
        self.assertEqual(set(flat), {c.key for c in cols.CATALOG})
        self.assertEqual(len(flat), len(cols.CATALOG))

    def test_every_category_in_order_list(self):
        for c in cols.CATALOG:
            self.assertIn(c.category, cols.CATEGORY_ORDER)

    def test_label_falls_back_to_key(self):
        self.assertEqual(cols.label("not_a_real_column"), "not_a_real_column")
        self.assertEqual(cols.label("strike"), "Strike")

    def test_column_config_builds_for_visible_keys(self):
        cfg = cols.column_config(["symbol", "strike", "option_score"])
        self.assertEqual(set(cfg.keys()), {"symbol", "strike", "option_score"})


if __name__ == "__main__":
    unittest.main()
