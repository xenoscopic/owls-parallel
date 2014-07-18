"""Provides the base parallelization backend class.
"""


class ParallelizationBackend(object):
    """The base class for all parallelization backends.  This backend should be
    subclassed by concrete implementations.
    """

    def compute(self, cache, jobs):
        """Run jobs on the backend, blocking until their completion.

        Args:
            cache: The persistent cache which should be set on the backend
            jobs: A tuple of tuple of tuples of the form:

                (
                    # Job 1
                    (
                        (function_1, args_1, kwargs_1),
                        ...
                        (function_M, args_M, kwargs_M)
                    ),

                    ...

                    # Job N
                    (
                        (function_1, args_1, kwargs_1),
                        ...
                        (function_L, args_L, kwargs_L)
                    )
                )
        """
        raise RuntimeError('abstract method')
