"""Provides an IPython-based parallelization backend.
"""


# IPython imports
from IPython.parallel import Client

# owls-cache imports
from owls_cache.persistent import set_persistent_cache

# owls-parallel imports
from owls_parallel.backends import ParallelizationBackend


# Create a class to execute jobs on the cluster
class _Runner(object):
    def __init__(self, cache):
        self._cache = cache

    def __call__(self, job):
        # Set the persistent cache
        set_persistent_cache(self._cache)

        # Run the operations in the job
        for function, args, kwargs in job:
            function(*args, **kwargs)


class IPythonParallelizationBackend(ParallelizationBackend):
    """A parallelization backend which uses an IPython cluster to compute
    results.
    """

    def __init__(self, *args, **kwargs):
        """Initializes a new instance of the IPythonParallelizationBackend.

        Args: The same as the IPython.parallel.Client class
        """
        # Create the client
        self._client = Client(*args, **kwargs)

        # Create the cluster view
        self._cluster = self._client.load_balanced_view()

    def compute(self, cache, jobs):
        """Run jobs on the backend, blocking until their completion.

        Args:
            cache: The persistent cache which should be set on the backend
            jobs: The job specification (see
                owls_parallel.backends.ParallelizationBackend)
        """
        self._cluster.map(_Runner(cache), jobs)
