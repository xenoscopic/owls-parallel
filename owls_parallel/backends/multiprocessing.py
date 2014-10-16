"""Provides a multiprocessing-based parallelization backend.
"""


# HACK: Use absolute_import behavior to get around module having the same name
# as the global multiprocessing module
from __future__ import absolute_import

# System imports
from multiprocessing import Pool
from uuid import uuid4

# Six imports
from six import iteritems, itervalues
from six.moves import queue

# owls-cache imports
from owls_cache.persistent import caching_into

# owls-parallel imports
from owls_parallel.backends import ParallelizationBackend


# Create a function to execute jobs on the cluster
def _run(job_id, cache, job):
    with caching_into(cache):
        for batcher, calls in iteritems(job):
            for function, args_kwargs in iteritems(calls):
                batcher(function, args_kwargs)
    return job_id


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

        # Create our internal notification queue
        self._queue = queue.Queue()

    def mode(self):
        """Returns the operation mode of the backend when waiting for jobs.
        """
        return 'notify'

    def start(self, cache, job_specs, callback):
        """Run jobs on the backend, blocking until their completion.

        Args:
            cache: The persistent cache which should be set on the backend
            job_specs: The job specification (see
                owls_parallel.backends.ParallelizationBackend)
            callback: The job notification callback
        """
        # HACK: Python's multiprocessing module has a terrible design flaw.  In
        # particular, the callback for an AsyncResult is performed BEFORE that
        # result is marked as 'ready' (see the _set method of
        # ApplyResult/AsyncResult).  Thus, if we call our notification
        # callback, it can try to run `prune` before the job is 'ready' and
        # the monitoring loop will see one unfinished job and wait forever for
        # another callback announcing its completion.  So, what we do is assign
        # each job a unique id which will be returned by the job's callback.
        # For our resultant job container, we use an associative array.  We
        # also use an underlying queue to record jobs whose callbacks have been
        # activated and which we should `wait` on in `prune`.
        def notify(job_id):
            # Mark the job as needing to be waited on
            self._queue.put(job_id)

            # Notify the parallel environment that we have stuff ready
            callback()

        # Start/record the jobs
        result = {}
        for spec in itervalues(job_specs):
            # Create a unique identifier
            job_id = uuid4().hex

            # Start/store the job
            result[job_id] = self._cluster.apply_async(
                _run,
                (job_id, cache, spec),
                callback = notify
            )

        # All done
        return result

    def prune(self, jobs):
        """Prunes a collection of jobs by pruning those which are complete.

        The input collection should not be modified.

        Args:
            jobs: A collection of jobs to prune

        Returns:
            A new collection of jobs which are still incomplete.
        """
        # HACK: First things first, go through and wait() on any jobs which may
        # have fired their callback but not yet marked themselves as ready.
        # See above HACK notice.
        while True:
            try:
                # Get the next job id
                job_id = self._queue.get(block = False)

                # Get the corresponding job
                job = jobs.get(job_id, None)

                # It could be that the completion was picked up in a previous
                # prune call due to another callback, so if it's not there,
                # carry on
                if job is None:
                    continue

                # Otherwise, wait for that guy
                job.wait()
            except queue.Empty:
                break

        # Extract unfinished jobs, and re-raise any remote exceptions
        result = {}
        for job_id, job in iteritems(jobs):
            if job.ready():
                # This will re-raise remotely-raised exceptions locally
                job.get()
            else:
                result[job_id] = job

        # All done
        return result
