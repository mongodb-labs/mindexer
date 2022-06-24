import unittest
import pandas as pd
import numpy as np
from mindexer.utils.query import Query, Predicate


class TestQuery(unittest.TestCase):
    def test_init(self):
        query = Query()
        self.assertEqual(query.predicates, [])

    def test_to_mql(self):
        query = Query()
        query.add_predicates(
            [
                Predicate("Unladen Weight", "eq", [2000]),
                Predicate("Make", "in", ["INFIN", "HYUND"]),
            ]
        )

        mql = query.to_mql()
        self.assertEqual(
            mql, {"Unladen Weight": 2000, "Make": {"$in": ["INFIN", "HYUND"]}}
        )

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
        query = Query()
        query.add_predicates(
            [
                Predicate("Unladen Weight", "eq", [2000]),
                Predicate("Make", "in", ["INFIN", "HYUND"]),
            ]
        )

        query.projection = ("City",)
        self.assertFalse(query.is_covered(("City", "State", "Make")))
        self.assertTrue(query.is_covered(("City", "Make", "Unladen Weight")))
        self.assertTrue(query.is_covered(("Make", "State", "City", "Unladen Weight")))

    def test_subset(self):
        query = Query()
        query.add_predicates(
            [
                Predicate("Unladen Weight", "eq", [2000]),
                Predicate("Make", "in", ["INFIN", "HYUND"]),
            ]
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
        query = Query()
        query.add_predicates(
            [
                Predicate("Unladen Weight", "eq", [2000]),
                Predicate("Make", "in", ["INFIN", "HYUND"]),
            ]
        )
        query.sort = ("Make", "City")
        self.assertTrue(query.can_use_sort(("Unladen Weight", "Make", "City", "State")))

    def test_can_use_sort_sub_seq_preceeding_not_eq(self):
        query = Query()
        query.add_predicates(
            [
                Predicate("Unladen Weight", "gt", [2000]),
                Predicate("Make", "in", ["INFIN", "HYUND"]),
            ]
        )
        query.sort = ("Make", "City")
        self.assertFalse(
            query.can_use_sort(("Unladen Weight", "Make", "City", "State"))
        )

    def test_fields(self):
        query = Query()
        query.add_predicates(
            [
                Predicate("Unladen Weight", "eq", [2000]),
                Predicate("Make", "in", ["INFIN", "HYUND"]),
            ]
        )

        query.projection = ("City",)
        query.sort = ("State", "County")

        self.assertEqual(
            query.fields, ["City", "County", "Make", "State", "Unladen Weight"]
        )

    def test_to_mql_dup_keys(self):
        query = Query()

        query.add_predicates(
            [
                Predicate("Unladen Weight", "lt", [2000]),
                Predicate("Unladen Weight", "gte", [1300]),
                Predicate("Make", "in", ["INFIN", "HYUND"]),
            ]
        )

        mql = query.to_mql()
        self.assertEqual(
            mql,
            {
                "Unladen Weight": {"$gte": 1300, "$lt": 2000},
                "Make": {"$in": ["INFIN", "HYUND"]},
            },
        )

    def test_to_mql_rewrite_eq(self):
        query = Query()

        query.add_predicates(
            [
                Predicate("Unladen Weight", "eq", [2000]),
                Predicate("Unladen Weight", "gte", [1800]),
                Predicate("Make", "in", ["INFIN", "HYUND"]),
            ]
        )

        mql = query.to_mql()
        self.assertEqual(
            mql,
            {
                "Unladen Weight": {"$eq": 2000, "$gte": 1800},
                "Make": {"$in": ["INFIN", "HYUND"]},
            },
        )

    def test_to_mql_exists_false(self):
        query = Query()

        query.add_predicates(
            [
                Predicate("Unladen Weight", "eq", [None]),
                Predicate("Make", "in", ["INFIN", "HYUND"]),
            ]
        )

        mql = query.to_mql()
        self.assertEqual(
            mql,
            {
                "Unladen Weight": {"$exists": False},
                "Make": {"$in": ["INFIN", "HYUND"]},
            },
        )

    def test_from_mql_eq(self):

        query = Query.from_mql(
            {
                "Unladen Weight": 4000,
                "Make": "INFIN",
            },
        )

        self.assertEqual(
            query.predicates,
            [
                Predicate("Unladen Weight", "eq", [4000]),
                Predicate("Make", "eq", ["INFIN"]),
            ],
        )

    def test_from_mql_ranges(self):

        query = Query.from_mql(
            {
                "Unladen Weight": {"$lt": 3000, "$gte": 1800},
                "Make": {"$in": ["INFIN", "HYUND"]},
            },
        )

        self.assertEqual(
            query.predicates,
            [
                Predicate("Unladen Weight", "lt", [3000]),
                Predicate("Unladen Weight", "gte", [1800]),
                Predicate("Make", "in", ["INFIN", "HYUND"]),
            ],
        )

    def test_from_mql_empty(self):
        query = Query.from_mql({})

        self.assertEqual(
            query.predicates,
            [],
        )

    def test_from_mql_not_equal(self):
        query = Query.from_mql({"Make": {"$ne": "INFIN"}})

        self.assertEqual(
            query.predicates,
            [Predicate(column="Make", op="ne", values=["INFIN"])],
        )

    def test_from_mql_in(self):
        query = Query.from_mql({"Make": {"$in": ["INFIN", "AUDI"]}})

        self.assertEqual(
            query.predicates,
            [Predicate(column="Make", op="in", values=["INFIN", "AUDI"])],
        )

    def test_from_mql_nin(self):
        query = Query.from_mql({"Make": {"$nin": ["INFIN", "AUDI"]}})

        self.assertEqual(
            query.predicates,
            [Predicate(column="Make", op="nin", values=["INFIN", "AUDI"])],
        )

    def test_from_mql_unknown_operator(self):
        def make_bad_query():
            query = Query.from_mql(
                {"Make": {"$foobar": "INFIN"}},
            )

        self.assertRaisesRegex(
            AssertionError,
            "unsupported operator",
            make_bad_query,
        )

    def test_from_mql_exists_false(self):
        query = Query.from_mql(
            {
                "Unladen Weight": {"$exists": False},
                "Make": {"$in": ["INFIN", "HYUND"]},
            },
        )

        self.assertEqual(
            query.predicates,
            [
                Predicate("Unladen Weight", "eq", [None]),
                Predicate("Make", "in", ["INFIN", "HYUND"]),
            ],
        )

    def test_query_index_intersect_prefix(self):
        query = Query()
        query.add_predicates(
            [
                Predicate("Unladen Weight", "lte", [3700]),
                Predicate("Make", "eq", ["HYUND"]),
            ]
        )

        intersected = query.index_intersect(("Unladen Weight", "Foo Field"))
        predicate_columns = [c.column for c in intersected.predicates]
        self.assertEqual(predicate_columns, ["Unladen Weight"])

    def test_query_index_intersect_empty(self):
        query = Query()
        query.add_predicates(
            [
                Predicate("Unladen Weight", "lte", [3700]),
                Predicate("Make", "eq", ["HYUND"]),
            ]
        )

        intersected = query.index_intersect(("Foo Field", "Unladen Weight"))
        predicate_columns = [c.column for c in intersected.predicates]
        self.assertEqual(predicate_columns, [])

    def test_query_index_intersect_multi(self):
        query = Query()
        query.add_predicates(
            [
                Predicate("Unladen Weight", "lte", [3700]),
                Predicate("Make", "eq", ["HYUND"]),
                Predicate("State", "eq", ["NY"]),
            ]
        )

        intersected = query.index_intersect(
            ("State", "Unladen Weight", "Foo Field", "Make")
        )
        predicate_columns = [c.column for c in intersected.predicates]
        self.assertEqual(predicate_columns, ["State", "Unladen Weight"])


if __name__ == "__main__":
    unittest.main()
