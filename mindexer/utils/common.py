import numpy as np
import pandas as pd
import operator

from bson.timestamp import Timestamp
from bson.int64 import Int64
from bson.decimal128 import Decimal128

from datetime import datetime

LOG2PI = np.log(2 * np.pi)

MISSING_TYPE = "_missing_"
TYPE_COL_NAME = "_types_"


def try_compare(x, op, y, default=False):
    try:
        return op(x, y)
    except TypeError as t:
        # if the types are not comparable, it's not a match
        # MongoDB calls this type bracketing
        return default


def eq_with_nan(a, b):
    if pd.isna(a) and pd.isna(b):
        return True
    return operator.eq(a, b)


OPERATORS = {
    "gt": lambda a, val: try_compare(a, operator.gt, val[0]),
    "lt": lambda a, val: try_compare(a, operator.lt, val[0]),
    "gte": lambda a, val: try_compare(a, operator.ge, val[0]),
    "lte": lambda a, val: try_compare(a, operator.le, val[0]),
    "eq": lambda a, val: try_compare(a, eq_with_nan, val[0]),
    "ne": lambda a, val: try_compare(a, operator.ne, val[0], True),
    "in": lambda a, val: try_compare(val, operator.contains, a),
    "nin": lambda a, val: try_compare(
        val, lambda x, y: operator.not_(operator.contains(x, y)), a, True
    ),
}


def get_type_or_missing(x):
    if pd.isna(x) or x == None:
        return MISSING_TYPE
    return type(x).__name__


def map_bson(b):
    if isinstance(b, (bool, int, float, str, datetime)):
        return b
    elif isinstance(b, Timestamp):
        return b.as_datetime()
    elif isinstance(b, Int64):
        return int(str(b))
    elif isinstance(b, Decimal128):
        return b.to_decimal()
    elif isinstance(b, list):
        raise TypeError("We don't know how to map arrays, yet")
    return str(b)


class QueryRegionEmptyException(Exception):
    pass
