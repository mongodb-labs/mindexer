import math
import pandas as pd

from bson.timestamp import Timestamp
from bson.int64 import Int64
from bson.decimal128 import Decimal128

from datetime import datetime


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


SAMPLE_DB_NAME = "samples"

# Get a cardinality estimation via sampling
class SampleEstimator:
    def __init__(
        self,
        mongo_collection,
        numrows=None,
        sample_ratio=None,
        sample_size=None,
        persist=False,
        sample_db_name=SAMPLE_DB_NAME,
        **kwargs,
    ):
        # mongoCollection is a MongoCollection class object used to access collection
        self.mongo = mongo_collection
        self.columns = None
        self.sample_size = sample_size
        self.sample_db_name = sample_db_name
        self.kwargs = kwargs

        self.limit = None
        if numrows:
            if numrows < self.mongo.count:
                self.limit = numrows

        db_size = self.limit or self.mongo.count

        self.persist = persist

        if sample_ratio is not None:
            assert 0 < sample_ratio <= 1, "sample_ratio must be between 0 and 1"

            assert (
                sample_size == None
            ), "please provide only one: sample_size or sample_ratio"

            self.sample_size = round(db_size * sample_ratio)

        if self.sample_size is not None:
            assert (
                0 < self.sample_size <= db_size
            ), "sample size must be between 0 and the size of the collection"

        if self.sample_size == db_size:
            self.sample_size = None  # Remove the sample stage if it's equal to the size of the collection and do not persist
            self.persist = False

        if self.persist:
            # create and save a sample collection in the samples database with sample_size = self.sample_size
            pipeline = [
                {"$sample": {"size": self.sample_size}},
                {
                    "$out": {
                        "db": self.sample_db_name,
                        "coll": self.mongo.collection_name,
                    }
                },
            ]
            if self.limit:
                pipeline.insert(0, {"$limit": self.limit})

            self.mongo.collection.aggregate(pipeline, allowDiskUse=True)

    def get_cardinality(self):
        return self.limit or self.mongo.count

    def make_pipeline(self, query):
        pipeline = [
            {"$match": query.to_mql()},
            {"$count": "total"},
        ]

        # take a sample from the actual database if a persistant sample was not created
        if self.sample_size and not self.persist:
            pipeline.insert(0, {"$sample": {"size": self.sample_size}})

        if self.limit and not self.persist:
            pipeline.insert(0, {"$limit": self.limit})

        return pipeline

    def estimate(self, query):
        """Returns the cardinality of a query by executing the query on the
        MongoDB collection and returning the result.
        """
        pipeline = self.make_pipeline(query)

        try:
            # if persistant sample has been created, query on that collection in db samples instead
            if self.persist:
                coll = self.mongo.collection_name
                actual = (
                    self.mongo.client[self.sample_db_name][coll]
                    .aggregate(pipeline, allowDiskUse=True)
                    .next()["total"]
                )
            else:
                # otherwise query on the full collection
                actual = self.mongo.collection.aggregate(
                    pipeline, allowDiskUse=True
                ).next()["total"]

            if self.sample_size:
                # if a sample size was provided, multiply the result to estimate the cardinality of the entire db
                total_count = self.limit or self.mongo.count
                actual = round(actual * (total_count / self.sample_size))

            return actual
        except:
            return 0

    def drop_sample(self):
        # drops any persisted samples in the samples database
        coll_name = self.mongo.collection_name
        return self.mongo.client[self.sample_db_name][coll_name].drop()

    def sample(self, query, n):
        pipeline = [
            {"$match": query.to_mql()},
            {"$sample": {"size": n}},
        ]

        try:
            if self.persist:
                coll = self.mongo.collection_name
                cur = self.mongo.client[self.sample_db_name][coll].aggregate(
                    pipeline, allowDiskUse=True
                )
            else:
                cur = self.mongo.collection.aggregate(pipeline, allowDiskUse=True)
        except StopIteration:
            raise Exception("Query region is empty")

        df = pd.json_normalize(cur)
        for col in df.columns:
            df[col] = df[col].map(map_bson)

        # if the query matches no documents in the database
        if len(df) == 0:
            raise QueryRegionEmptyException(
                "Error in SampleEstimator: Query region is empty"
            )

        # if not enough results are returned
        if len(df) < n:
            duplicate = math.ceil(n / len(df))
            # duplicate the existing data until we have n results
            df = df.append([df] * duplicate, ignore_index=True)
            df = df.head(n)

        # shuffle the result returned
        df = df.sample(frac=1).reset_index(drop=True)

        return df
