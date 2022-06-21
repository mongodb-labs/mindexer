from typing import Tuple
from .common import OPERATORS

from collections import namedtuple
import numpy as np
import pandas as pd

Predicate = namedtuple("Predicate", ["column", "op", "values"])


class Query(object):
    """The Query class represents a query object, consisting of
    a number of Predicates. Predicates are named tuples of
    the form (column, op, values).
    This class also contains some utility methods, for example
    exporting a query to MQL syntax, intersecting a query
    with an index (for Index Selection), etc.
    """

    def __init__(self):
        self.predicates = []

        self._limit = None
        self._projection = None
        self._sort = None

    @property
    def limit(self):
        """returns the limit for this query."""
        return self._limit

    @limit.setter
    def limit(self, n: int):
        """set a limit for this query. None means no limit."""
        assert type(n) == int, "limit must be an integer"
        self._limit = n

    @property
    def projection(self):
        """returns the projection tuple of this query."""
        return self._projection

    @projection.setter
    def projection(self, p: Tuple[str, ...]):
        """set the projection tuple for this query."""
        self._projection = p

    @property
    def sort(self):
        """returns the sort tuple of this query."""
        return self._sort

    @sort.setter
    def sort(self, s: Tuple[str, ...]):
        """set the sort tuple for this query."""
        self._sort = s

    @property
    def fields(self):
        """return all fields of the query, whether they are part of the predicates,
        projection or sort.
        """
        fields = [pred.column for pred in self.predicates]
        if self.sort:
            fields += list(self.sort)
        if self.projection:
            fields += list(self.projection)
        return sorted(fields)

    def add_predicate(self, predicate):
        """adds a predicate to the query."""

        assert isinstance(predicate.values, list)
        assert (
            predicate.op in OPERATORS.keys()
        ), f"unsupported operator '${predicate.op}'"

        # simplify $in
        if predicate.op in ["in", "nin"]:
            assert len(predicate.values) >= 1, "in operator requires at least 1 value"
            if len(predicate.values) == 1:
                predicate.op == "eq" if predicate.op == "in" else "neq"
        else:
            assert len(predicate.values) == 1, "operator takes exactly 1 value"

        self.predicates.append(predicate)

    def add_predicates(self, preds):
        """add multiple predicates to a query at once."""
        for pred in preds:
            self.add_predicate(pred)

    def index_intersect(self, index):
        """returns a copy of this query that only contains predicates on the
        provided index fields, left to right, up until an index field was
        not included in the query.

        Example: query is {a: true, b: {$lt: 20}, c: 5}
                 query.index_intersect(("b", "d", "c")) return the query
                 equivalent to {b: {$lt: 20}}, because d is not present
                 in the query and aborts the algorithm.

        """
        query = Query()

        for field in index:
            # reset field indicator
            query_has_field = False
            for pred in self.predicates:
                if pred.column == field:
                    query_has_field = True
                    query.add_predicate(pred)
            # if no predicates found for this field, stop.
            if not query_has_field:
                break
        return query

    def is_subset(self, index):
        """returns true if all predicate fields are included in the index.
        This is necessary, but not sufficient to be a covered by the index.
        It is used to determine if a limit caps the cost of the query or not.
        """
        predicate_fields = set(p.column for p in self.predicates)
        return predicate_fields.issubset(set(index))

    def is_covered(self, index):
        """returns true if this query is covered by the index, false otherwise.
        A query without a projection is never covered. A query with projection
        is covered if the union of all predicate fields and projected fields
        appear in the index.
        """
        if self.projection is None:
            return False

        predicate_fields = tuple(p.column for p in self.predicates)
        fields_to_cover = set(predicate_fields + self.projection)

        return fields_to_cover.issubset(set(index))

    def can_use_sort(self, index):
        """returns true if this query has a sort and the index can be used to sort,
        false otherwise.
        A query can be sorted by an index if any of the following is true:
            a) the sort fields are the same as the index fields (incl. order)
            b) the sort fields are a prefix sequence of the index fields
            c) the sort fields are a sub-sequence of the index fields and
               the query only uses equality predicates on all fields preceeding
               the sort fields.

               Example: index on ('a', 'b', 'c', 'd')
                        query {a: 5, b: {$gt: 6}} with sort ('b', 'c')
                        The predicate preceeding the sort fields is 'a' and is
                        an equality predicate. The index can be used to sort.
        """
        if self.sort is None:
            # TODO check this: if the query doesn't need a sort, the sort fields sequence
            # is and empty list, and theoretically a prefix of any index.
            return True

        # cover case a) and b)
        if self.sort == index[: len(self.sort)]:
            return True

        # cover case c)
        sub_idx = -1
        for i in range(len(index) - len(self.sort)):
            if self.sort == index[i : i + len(self.sort)]:
                # sort fields are a sub-sequence of index fields
                sub_idx = i

        if sub_idx != -1:
            # check for preceeding equality predicates
            if all(pred.op == "eq" for pred in self.predicates[:sub_idx]):
                return True

        return False

    def __repr__(self):
        s = []
        s.append(f"filter={self.to_mql()}")
        if self.sort:
            s.append(f"sort={self.sort}")
        if self.limit:
            s.append(f"limit={self.limit}")
        if self.projection:
            s.append(f"projection={self.projection}")
        return f"Query({', '.join(s)})"

    def __len__(self):
        """The length of a query, using 'len(query)', is the number of
        predicates it contains."""
        return len(self.predicates)

    def to_df_query(self, df):
        """Converts the query to be used on a Pandas DataFrame."""
        FUNC_MAP = {"lt": "lt", "lte": "le", "gt": "gt", "gte": "ge", "eq": "eq"}
        bools = pd.Series([True] * df.shape[0])
        for pred in self.predicates:
            fn = FUNC_MAP[pred.op]
            colname = pred.column.replace("_", " ")
            newbools = getattr(df[colname], fn)(pred.values[0])
            bools &= newbools
        return bools.sum()

    def to_mql(self):
        """converts the query to MQL syntax."""
        query = {}

        def clean_value(v):
            if type(v) == str:
                return v.strip()
            elif isinstance(v, np.int64):
                return v.item()
            else:
                return v

        for predicate in self.predicates:
            name = predicate.column
            # remove leading/trailing whitespace
            values = [clean_value(v) for v in predicate.values]
            if predicate.op == "eq":
                if pd.isna(values[0]):
                    # special handling for missing values
                    query[name] = {"$exists": False}
                else:
                    query[name] = values[0]
            elif predicate.op in ["in", "nin"]:
                query[name] = {f"${predicate.op}": values}
            else:
                if name not in query:
                    query[name] = {f"${predicate.op}": values[0]}
                else:
                    if isinstance(query[name], dict):
                        query[name][f"${predicate.op}"] = values[0]
                    else:
                        query[name] = {
                            "$eq": query[name],
                            f"${predicate.op}": values[0],
                        }
        return query

    @classmethod
    def from_mql(cls, query):
        """converts MQL syntax to a Query object."""
        q = cls()

        if not query:
            return q

        for col, value in query.items():
            if col.startswith("$"):
                raise AssertionError(f"unsupported operator '{col}'")
            if isinstance(value, (str, int)):
                q.add_predicate(Predicate(column=col, op="eq", values=[value]))
            elif isinstance(value, dict):
                ops = value.keys()
                for item in ops:
                    if item in ["$in", "$nin"]:
                        q.add_predicate(
                            Predicate(
                                column=col, op=item.lstrip("$"), values=value[item]
                            )
                        )
                    elif item == "$exists" and not value[item]:
                        # special handling for missing values
                        q.add_predicate(Predicate(column=col, op="eq", values=[None]))
                    else:
                        q.add_predicate(
                            Predicate(
                                column=col, op=item.lstrip("$"), values=[value[item]]
                            )
                        )

        return q


if __name__ == "__main__":

    # Example Usage of a Query objet
    query = Query()

    query.add_predicates(
        [
            Predicate("Suspension_Indicator", "eq", ["Y"]),
            Predicate("Make", "in", ["INFIN", "HYUND"]),
        ]
    )
