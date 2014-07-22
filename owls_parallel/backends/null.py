"""Provides a null parallelization backend that runs jobs synchronously.
"""


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


class NullParallelizationBackend(ParallelizationBackend):
    """A parallelization backend which runs jobs synchronously.
    """

    def compute(self, cache, jobs):
        """Run jobs on the backend, blocking until their completion.

        Args:
            cache: The persistent cache which should be set on the backend
            jobs: The job specification (see
                owls_parallel.backends.ParallelizationBackend)
        """
        map(_Runner(cache), jobs)
