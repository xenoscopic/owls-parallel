"""Provides the base parallelization backend class.
"""


class ParallelizationBackend(object):
    """The base class for all parallelization backends.  This backend should be
    subclassed by concrete implementations.

    All backends should be reusable - i.e. they should be able to handle
    multiple calls to `compute`.
    """

    def mode(self):
        """Returns the operation mode of the backend when waiting for jobs.

        Backends may be either 'notify' or 'poll', and this method should
        return one of these two strings.
        """
        raise NotImplementedError('abstract method')

    def start(self, cache, job_specs, callback):
        """Starts jobs on the backend, letting them run asynchronously.

        Args:
            cache: The persistent cache which should be set on the backend
            job_specs: A map structure of the form:

                {
                    key: {
                        batcher: {
                            function: [
                                (args1, kwargs1),
                                ...
                                (argsN, kwargsN),
                            ],
                            ...
                        },
                        ...
                    },
                    ...
                }

                where a separate job should be created for each key
            callback: A callback which can be used to notify the
                parallelization environment that new results are available and
                that the job list will be pruned.  For backends which rely on
                'poll' behavior, this argument will be set to None.  For
                backends which support 'notify' behavior, this callback must be
                called at least once after the last job result becomes
                available.  This function will be non-blocking.

        Returns:
            A collection of 'job' objects.  The collection is up to the
            implementer, but it must support at least the `__len__` method.
            Used to monitor jobs via the `prune` method.
        """
        raise NotImplementedError('abstract method')

    def prune(self, jobs):
        """Prunes a collection of jobs by pruning those which are complete.

        The input collection should not be modified.

        Args:
            jobs: A collection of jobs to prune

        Returns:
            A new collection of jobs which are still incomplete.
        """
        raise NotImplementedError('abstract method')
