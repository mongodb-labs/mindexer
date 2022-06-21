from pymongo import MongoClient, ASCENDING
import time


class MongoCollection(object):
    """This class represents a collection in MongoDB. It contains a number
    of useful utilities, like index management and workload execution
    on that collection.
    """

    def __init__(self, uri, db, coll):
        self.collection_name = coll
        self.namespace = f"{db}.{coll}"
        self.uri = uri
        self.client = MongoClient(uri)
        self.db = self.client[db]
        self.collection = self.client[db][coll]
        self.count = self.collection.count_documents({})
        self.last_index = None

    def _get_query_options(self, query):
        """returns sort, limit and projection of a query in dictionary form."""
        if isinstance(query, dict):
            return {"sort": None, "limit": 0, "projection": None}

        sort = [(field, 1) for field in query.sort] if query.sort else None
        limit = query.limit or 0
        if query.projection:
            projection = {field: 1 for field in query.projection}
            # explicitly exclude _id
            projection["_id"] = 0
        else:
            projection = None

        return {"sort": sort, "limit": limit, "projection": projection}

    def _parse_winning_plan(self, explain):
        stages = []
        doc = explain["executionStages"]
        while "stage" in doc:
            stages.append(doc["stage"])
            doc = doc.get("inputStage", {})
        stages.reverse()
        return " -> ".join(stages)

    def execute_query(self, query):
        """Executes the query with find syntax and exhausts the cursor.
        query can be a Query instance or a MQL query as a dictionary. Note: this does
        not return the results, use the collection instance member instead if you need
        the results of the query.
        """
        options = self._get_query_options(query)
        options["allow_disk_use"] = True
        cursor = self.collection.find(query.to_mql(), **options)
        for _ in cursor:
            pass

    def explain_query(self, query):
        """Explains a single query in executionStats mode for workload measurements.
        query can be a Query instance or a MQL query as a dictionary."""
        options = self._get_query_options(query)

        command = {
            "explain": {
                "find": self.collection_name,
                "filter": query.to_mql(),
                "limit": options["limit"],
                "projection": options["projection"] or {},
                "sort": dict(options["sort"]) if options["sort"] else {},
                "allowDiskUse": True,
            },
            "verbosity": "executionStats",
        }
        result = self.db.command(command)
        return result["executionStats"]

    def execute_workload(self, workload, explain=True):
        """executes a query workload and returns the time in milliseconds it took."""
        exec_time = 0
        t = time.time()
        for i, query in enumerate(workload):
            if explain:
                result = self.explain_query(query)
                print(
                    f"{i:2}  {query}\n"
                    f"     executionTimeMillis {result['executionTimeMillis']}"
                    f"     totalKeysExamined {result['totalKeysExamined']}"
                    f"     totalDocsExamined {result['totalDocsExamined']}"
                    f"     nReturned {result['nReturned']}"
                    f"     plan {self._parse_winning_plan(result)}"
                )

                exec_time += result["executionTimeMillis"]
            else:
                self.execute_query(query)
        if explain:
            return exec_time
        return (time.time() - t) * 1000

    def list_indexes(self):
        """Lists all available indexes on the collection by name."""
        return [idx.get("name") for idx in self.collection.list_indexes()]

    def create_index(self, index):
        """create an index on the collection. index is a field tuple. e.g. ("foo", "bar")
        which would create the index {"foo": 1, "bar": 1}.
        """
        asc = [ASCENDING] * len(index)
        index_name = "_".join(index).replace(" ", "_")
        index = list(zip(index, asc))

        self.last_index = index_name
        self.collection.create_index(index, name=index_name)

    def drop_last_index(self):
        """drops the last index created with create_index(). Only one index can be
        removed this way (not a stack).
        """
        if self.last_index:
            self.collection.drop_index(self.last_index)
            self.last_index = None

    def drop_indexes(self):
        """drops all indexes of the given collection."""
        self.last_index = None
        self.collection.drop_indexes()
