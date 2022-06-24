import unittest
import pandas as pd
import numpy as np
from mindexer.utils.query import Query


class TestQuery(unittest.TestCase):
    def test_init_empty(self):
        query = Query()
        self.assertEqual(query.filter, {})

    def test_init_from_mql(self):
        query = Query.from_mql({"foo": {"$gt": 16}})
        self.assertEqual(query.filter, {"foo": {"$gt": 16}})
        self.assertIsNone(query.limit)
        self.assertIsNone(query.projection)
        self.assertIsNone(query.sort)

    def test_limit(self):
        query = Query()
        query.limit = 12
        self.assertEqual(query.limit, 12)

        def assign_float():
            query.limit = 12.5

        self.assertRaises(AssertionError, assign_float)

    def test_projection(self):
        query = Query()
        query.projection = ("foo", "bar")
        self.assertEqual(query.projection, ("foo", "bar"))

    def test_fields(self):
        query = Query.from_mql({"foo": {"$gt": 16}, "bar": {"$in": [1, 2, 3]}})
        query.sort = ("a", "foo")
        query.projection = ("foo", "new")

        self.assertEqual(query.fields, ["a", "bar", "foo", "new"])

    def test_add_predicate_new(self):
        query = Query.from_mql({"foo": {"$gt": 16}, "bar": {"$in": [1, 2, 3]}})
        query.add_predicate({"woo": "hoo"})

        self.assertTrue("woo" in query.fields)
        self.assertEqual(query.filter["woo"], "hoo")

    def test_add_predicate_existing(self):
        query = Query.from_mql({"foo": {"$gt": 16}, "bar": {"$in": [1, 2, 3]}})
        query.add_predicate({"foo": {"$lt": 20}})

        self.assertEqual(query.filter["foo"], {"$gt": 16, "$lt": 20})

    def test_add_predicate_conflict(self):
        query = Query.from_mql({"foo": 16, "bar": {"$in": [1, 2, 3]}})

        def fn():
            query.add_predicate({"foo": {"$lt": 20}})

        self.assertRaisesRegexp(Exception, "can't update foo", fn)

    def test_add_predicates(self):
        query = Query.from_mql({"foo": {"$gt": 5}})
        query.add_predicates({"foo": {"$lt": 10}, "bar": True})

        self.assertEqual(query.filter, {"foo": {"$gt": 5, "$lt": 10}, "bar": True})

    def test_index_intersect(self):
        pass

    def test_covered_no_projection(self):
        query = Query()
        self.assertFalse(query.is_covered(("Make", "Unladen Weight")))

    def test_covered_projection_no_predicates(self):
        query = Query()
        query.projection = ("Make",)
        self.assertFalse(query.is_covered(("City", "State")))
        self.assertTrue(query.is_covered(("City", "Make")))
        self.assertTrue(query.is_covered(("Make",)))

    def test_covered_projection_predicates(self):
        query = Query.from_mql(
            {"Unladen Weight": 2000, "Make": {"$in": ["INFIN", "HYUND"]}}
        )

        query.projection = ("City",)
        self.assertFalse(query.is_covered(("City", "State", "Make")))
        self.assertTrue(query.is_covered(("City", "Make", "Unladen Weight")))
        self.assertTrue(query.is_covered(("Make", "State", "City", "Unladen Weight")))

    def test_subset(self):
        query = Query.from_mql(
            {"Unladen Weight": 2000, "Make": {"$in": ["INFIN", "HYUND"]}}
        )

        self.assertFalse(query.is_subset(("City", "State", "Make")))
        self.assertTrue(query.is_subset(("Make", "Unladen Weight")))
        self.assertTrue(query.is_subset(("Make", "State", "City", "Unladen Weight")))

    def test_sort(self):
        query = Query()
        query.sort = ("foo", "bar")
        self.assertEqual(query.sort, ("foo", "bar"))

    def test_can_use_sort_equal(self):
        query = Query()
        query.sort = ("foo", "bar")
        self.assertTrue(query.can_use_sort(("foo", "bar")))

    def test_can_use_sort_prefix(self):
        query = Query()
        query.sort = ("foo", "bar")
        self.assertTrue(query.can_use_sort(("foo", "bar", "baz")))

    def test_can_use_sort_no_prefix(self):
        query = Query()
        query.sort = ("foo", "bar")
        self.assertFalse(query.can_use_sort(("foo", "other", "blah")))

    def test_can_use_sort_sub_seq_preceeding_eq(self):
        query = Query.from_mql(
            {"Unladen Weight": 2000, "Make": {"$in": ["INFIN", "HYUND"]}}
        )
        query.sort = ("Make", "City")
        self.assertTrue(query.can_use_sort(("Unladen Weight", "Make", "City", "State")))

    def test_can_use_sort_sub_seq_preceeding_not_eq(self):
        query = Query.from_mql(
            {"Unladen Weight": {"$gt": 2000}, "Make": {"$in": ["INFIN", "HYUND"]}}
        )
        query.sort = ("Make", "City")
        self.assertFalse(
            query.can_use_sort(("Unladen Weight", "Make", "City", "State"))
        )

    def test_fields(self):
        query = Query.from_mql(
            {"Unladen Weight": 2000, "Make": {"$in": ["INFIN", "HYUND"]}}
        )

        query.projection = ("City",)
        query.sort = ("State", "County")

        self.assertEqual(
            query.fields, ["City", "County", "Make", "State", "Unladen Weight"]
        )

    def test_to_mql_dup_keys(self):
        query = Query.from_mql(
            {
                "Unladen Weight": {"$gte": 1300, "$lt": 2000},
                "Make": {"$in": ["INFIN", "HYUND"]},
            }
        )

        mql = query.to_mql()
        self.assertEqual(
            mql,
            {
                "Unladen Weight": {"$gte": 1300, "$lt": 2000},
                "Make": {"$in": ["INFIN", "HYUND"]},
            },
        )

    def test_query_index_intersect_prefix(self):
        query = Query.from_mql({"Unladen Weight": {"$lte": 3700}, "Make": "HYUND"})

        intersected = query.index_intersect(("Unladen Weight", "Foo Field"))
        self.assertEqual(intersected.fields, ["Unladen Weight"])

    def test_query_index_intersect_empty(self):
        query = Query.from_mql({"Unladen Weight": {"$lte": 3700}, "Make": "HYUND"})

        intersected = query.index_intersect(("Foo Field", "Unladen Weight"))
        self.assertEqual(intersected.fields, [])

    def test_query_index_intersect_multi(self):
        query = Query.from_mql(
            {"Unladen Weight": {"$lte": 3700}, "Make": "HYUND", "State": "NY"}
        )

        intersected = query.index_intersect(
            ("State", "Unladen Weight", "Foo Field", "Make")
        )
        self.assertEqual(intersected.fields, ["State", "Unladen Weight"])


if __name__ == "__main__":
    unittest.main()
