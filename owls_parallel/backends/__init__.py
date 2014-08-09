"""Provides the base parallelization backend class.
"""


class ParallelizationBackend(object):
    """The base class for all parallelization backends.  This backend should be
    subclassed by concrete implementations.

    All backends should be reusable - i.e. they should be able to handle
    multiple calls to `compute`.
    """

    def start(self, cache, jobs):
        """Starts jobs on the backend, letting them run asynchronously.

        Args:
            cache: The persistent cache which should be set on the backend
            jobs: A map structure of the form:

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

        Returns:
            A list of 'job id' objects, which are implementation-dependent, but
            which can be used to monitor job progress via the `prune` method.
        """
        raise NotImplementedError('abstract method')

    def prune(self, job_ids):
        """Prunes a list of job ids by pruning those which are complete.

        The input list should not be modified.

        Args:
            job_ids: A list of job_ids to prune

        Returns:
            A new list of jobs ids whose jobs are still incomplete.
        """
        raise NotImplementedError('abstract method')
