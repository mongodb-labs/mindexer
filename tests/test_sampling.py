import unittest
from unittest.mock import Mock
from mindexer.utils.query import Query
from mindexer.utils.sampling import SampleEstimator


class TestSampleEstimator(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.mongoCollection = Mock()

    def test_pipeline_defaults(self):
        # test a pipeline built with default parameters
        TestSampleEstimator.mongoCollection.count = 100

        query = Query.from_mql({"County": "KINGS", "Model Year": {"$gt": 2010}})

        sample_est = SampleEstimator(TestSampleEstimator.mongoCollection)

        pipeline = sample_est.make_pipeline(query)

        self.assertEqual(
            pipeline,
            [
                {"$match": {"County": "KINGS", "Model Year": {"$gt": 2010}}},
                {"$count": "total"},
            ],
        )

    def test_pipeline_sample_size(self):
        # test building a pipeline when given a sample size
        TestSampleEstimator.mongoCollection.count = 10

        query = Query.from_mql({"County": "KINGS", "Model Year": {"$gt": 2010}})
        sample_est = SampleEstimator(TestSampleEstimator.mongoCollection, sample_size=3)

        pipeline = sample_est.make_pipeline(query)

        self.assertEqual(
            pipeline,
            [
                {"$sample": {"size": 3}},
                {"$match": {"County": "KINGS", "Model Year": {"$gt": 2010}}},
                {"$count": "total"},
            ],
        )

    def test_pipeline_sample_ratio(self):
        # test building a pipelind when given a sample ratio
        TestSampleEstimator.mongoCollection.count = 100

        query = Query.from_mql({"County": "KINGS", "Model Year": {"$gt": 2010}})

        sample_est = SampleEstimator(
            TestSampleEstimator.mongoCollection, sample_ratio=0.5
        )

        pipeline = sample_est.make_pipeline(query)

        self.assertEqual(
            pipeline,
            [
                {"$sample": {"size": 50}},
                {"$match": {"County": "KINGS", "Model Year": {"$gt": 2010}}},
                {"$count": "total"},
            ],
        )

    def test_pipeline_zero(self):
        # test that giving a size of 0 returns error
        TestSampleEstimator.mongoCollection.count = 100

        with self.assertRaises(AssertionError) as err:
            SampleEstimator(TestSampleEstimator.mongoCollection, sample_size=0)
            print(err)

    def test_pipeline_large(self):
        # test that there is an error if given a sample size larger than collection size
        TestSampleEstimator.mongoCollection.count = 100

        with self.assertRaises(AssertionError) as err:
            SampleEstimator(TestSampleEstimator.mongoCollection, sample_size=1000)
            print(err)

    def test_pipeline_round_sample_ratio(self):
        # test that giving a very small sample ratio will throw an error
        TestSampleEstimator.mongoCollection.count = 100

        with self.assertRaises(AssertionError) as err:
            SampleEstimator(TestSampleEstimator.mongoCollection, sample_ratio=0.0001)
            print(err)

    def test_pipeline_with_limit(self):
        # test building a pipeline with a limit on number of documents
        TestSampleEstimator.mongoCollection.count = 100

        query = Query.from_mql({"County": "KINGS", "Model Year": {"$gt": 2010}})

        sample_est = SampleEstimator(
            TestSampleEstimator.mongoCollection, numrows=10, sample_size=3
        )

        pipeline = sample_est.make_pipeline(query)

        self.assertEqual(
            pipeline,
            [
                {"$limit": 10},
                {"$sample": {"size": 3}},
                {"$match": {"County": "KINGS", "Model Year": {"$gt": 2010}}},
                {"$count": "total"},
            ],
        )


if __name__ == "__main__":
    unittest.main()
