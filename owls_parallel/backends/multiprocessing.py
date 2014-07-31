"""Provides a multiprocessing-based parallelization backend.
"""


# HACK: Use absolute_import behavior to get around module having the same name
# as the global multiprocessing module
from __future__ import absolute_import

# System imports
from multiprocessing import Pool

# owls-cache imports
from owls_cache.persistent import caching_into

# owls-parallel imports
from owls_parallel.backends import ParallelizationBackend


# Create a function to execute jobs on the cluster
def _run(cache, job):
    with caching_into(cache):
        for function, args, kwargs in job:
            function(*args, **kwargs)


class MultiprocessingParallelizationBackend(ParallelizationBackend):
    """A parallelization backend which uses a multiprocessing pool to compute
    results.
    """

    def __init__(self, *args, **kwargs):
        """Initializes a new instance of the
        MultiprocessingParallelizationBackend.

        Args: The same as the multiprocessing.Pool class
        """
        # Create the processing pool
        self._cluster = Pool(*args, **kwargs)

    def start(self, cache, jobs):
        """Run jobs on the backend, blocking until their completion.

        Args:
            cache: The persistent cache which should be set on the backend
            jobs: The job specification (see
                owls_parallel.backends.ParallelizationBackend)
        """
        return [self._cluster.apply_async(_run, (cache, j)) for j in jobs]

    def prune(self, job_ids):
        """Prunes a list of job ids by pruning those which are complete.

        The input list should not be modified.

        Args:
            job_ids: A list of job_ids to prune

        Returns:
            A new list of jobs ids whose jobs are still incomplete.
        """
        # Extract unfinished jobs, and re-raise any remote exceptions
        result = []
        for j in job_ids:
            if j.ready():
                # This will re-raise remotely-raised exceptions locally
                j.get()
            else:
                result.append(j)

        # All done
        return result
