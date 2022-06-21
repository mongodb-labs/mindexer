import pickle


class Workload(object):
    """Represents a workload consisting of a number of Query objects.
    In its current form, a workload is defined by a number of
    query patterns (= shapes), and a number of queries of the
    given shape. The workload is randomly generated based on these
    parameters.
    """

    def __init__(self):
        self.queries = []

    def __repr__(self):
        return f"<Workload: {len(self.queries)} queries>"

    def __iter__(self):
        return iter(self.queries)

    def __len__(self):
        return len(self.queries)

    def print(self):
        """Prints each query of the workload."""
        for i, query in enumerate(self.queries):
            print(f"{i:>4} {query}")

    def save(self, filename):
        """Save a workload to a file (without the model)"""
        # save workload
        with open(filename, "wb") as f:
            pickle.dump(self, f)

    @classmethod
    def load(cls, filename):
        """Class method to load a workload from file."""
        # read workload from file
        with open(filename, "rb") as f:
            workload = pickle.load(f)

        return workload
