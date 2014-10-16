"""Provides an IPython-based parallelization backend.
"""


# Six imports
from six import iteritems, itervalues

# IPython imports
from IPython.parallel import Client

# owls-cache imports
from owls_cache.persistent import caching_into

# owls-parallel imports
from owls_parallel.backends import ParallelizationBackend


# Create a function to execute jobs on the cluster
def _run(cache, job):
    with caching_into(cache):
        for batcher, calls in iteritems(job):
            for function, args_kwargs in iteritems(calls):
                batcher(function, args_kwargs)


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

    def start(self, cache, job_specs, callback):
        """Run jobs on the backend, blocking until their completion.

        Args:
            cache: The persistent cache which should be set on the backend
            job_specs: The job specification (see
                owls_parallel.backends.ParallelizationBackend)
            callback: The job notification callback, not used by this backend
        """
        return [self._cluster.apply_async(_run, cache, j)
                for j
                in itervalues(job_specs)]

    def prune(self, jobs):
        """Prunes a collection of jobs by pruning those which are complete.

        The input collection should not be modified.

        Args:
            jobs: A collection of jobs to prune

        Returns:
            A new collection of jobs which are still incomplete.
        """
        # Extract unfinished jobs, and re-raise any remote exceptions
        result = []
        for j in jobs:
            if j.ready():
                # This will re-raise remotely-raised exceptions locally
                j.get()
            else:
                result.append(j)

        # All done
        return result
