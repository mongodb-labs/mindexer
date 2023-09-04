from typing import Tuple
from collections import OrderedDict
from bson import Int64


def validate_recursive(obj, val_fn, depth=0):
    """runs the validation function val_fn(key, val, depth) recursively
    on keys and values, throws if the query is not supported."""

    if isinstance(obj, dict):
        for k, v in obj.items():
            val_fn(k, None, depth)
            validate_recursive(v, val_fn, depth + 1)
    elif isinstance(obj, list):
        all(validate_recursive(el, val_fn, depth) for el in obj)
    else:
        val_fn(None, obj, depth)
    return True


class Query(object):
    """The Query class represents a query object, consisting of
    a number of Predicates. Predicates are named tuples of
    the form (column, op, values).
    This class also contains some utility methods, for example
    exporting a query to MQL syntax, intersecting a query
    with an index (for Index Selection), etc.
    """

    def __init__(self):
        self._filter = OrderedDict()
        self._limit = None
        self._projection = None
        self._sort = None

    def _is_filter_supported(self, f):
        """returns whether the filter is supported in this version of mindexer."""

    @property
    def filter(self):
        """returns the filter for this query."""
        return self._filter

    @filter.setter
    def filter(self, f: int):
        """set a filter for this query. None means no filter = {}."""
        assert type(f) == dict, "limit must be a dict"

        # validation for filter object
        def val_fn(k, v, d):
            if k == "$and" and d > 0:
                raise NotImplementedError("$and is only supported at the top level")
            if k in ["$nor", "$or", "$text"]:
                raise NotImplementedError(f"queries with {k} not supported.")

        # this throws if validation fails
        validate_recursive(f, val_fn)

        # filter out $comment fields
        f = {k: v for k, v in f.items() if k != "$comment"}

        # flatten top-level $and to implicit syntax
        if "$and" in f:
            temp_q = Query()
            for el in f["$and"]:
                temp_q.add_predicate(el)
                self._filter = temp_q.filter
            return

        self._filter = f

    @property
    def limit(self):
        """returns the limit for this query."""
        return self._limit

    @limit.setter
    def limit(self, n: int):
        """set a limit for this query. None means no limit."""
        assert isinstance(n, (int, Int64)), f"limit must be an integer, is {type(n)}"
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
        fields = list(self.filter.keys())
        if self.sort:
            fields += list(self.sort)
        if self.projection:
            fields += list(self.projection)
        return sorted(set(fields))

    def add_predicate(self, predicate):
        """adds a predicate to the query."""

        assert isinstance(predicate, dict)

        # get first (and only) key
        key = next(iter(predicate))
        if not key in self.filter:
            self._filter.update(predicate)
        else:
            # get filter and predicate values
            fval = self._filter[key]
            pval = predicate[key]
            if type(fval) == dict and type(pval) == dict:
                fval.update(pval)
            else:
                raise Exception(
                    f"Error: can't update {key} field with predicate {predicate}."
                )

    def add_predicates(self, preds):
        """add multiple predicates to a query at once."""

        assert isinstance(preds, dict)

        for k, v in preds.items():
            self.add_predicate({k: v})

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
            if field in self.filter:
                query.add_predicate({field: self.filter[field]})
            else:
                break

        return query

    def is_subset(self, index):
        """returns true if all predicate fields are included in the index.
        This is necessary, but not sufficient to be a covered by the index.
        It is used to determine if a limit caps the cost of the query or not.
        """
        predicate_fields = set(self.filter.keys())
        return predicate_fields.issubset(set(index))

    def is_covered(self, index):
        """returns true if this query is covered by the index, false otherwise.
        A query without a projection is never covered. A query with projection
        is covered if the union of all predicate fields and projected fields
        appear in the index.
        """
        if self.projection is None:
            return False

        predicate_fields = tuple(self.filter.keys())
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
            return False

        # cover case a) and b)
        if self.sort == index[: len(self.sort)]:
            return True

        # cover case c)
        sub_idx = -1
        for i in range(len(index) - len(self.sort)):
            if self.sort == index[i : i + len(self.sort)]:
                # sort fields are a sub-sequence of index fields
                sub_idx = i

        def is_equality_cmp(field):
            if field not in self.filter:
                # it's not an equality comparison if the field isn't in the query
                return False
            if isinstance(self.filter[field], dict):
                # if none (= not any) of the keys start with $, then it's an equality comparison
                return not any(key.startswith("$") for key in self.filter[field].keys())
            else:
                # if value is not a dictionary, it's always an equality comp.
                return True

        if sub_idx != -1:
            # check for preceeding equality predicates
            if all(is_equality_cmp(key) for key in index[:sub_idx]):
                return True

        return False

    def __repr__(self):
        s = []
        s.append(f"filter={self.filter}")
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
        return len(self.filter.keys())

    def to_mql(self):
        """converts the query to MQL syntax."""
        return self.filter

    @classmethod
    def from_mql(cls, query):
        """converts MQL syntax to a Query object."""
        q = cls()
        if query:
            q.filter = query

        return q
