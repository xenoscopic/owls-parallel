"""Provides simple and portable parallelization facilities.
"""


# Future imports to support fancy print() on Python 2.x
from __future__ import print_function

# System imports
import threading
from collections import defaultdict
from functools import wraps
from time import sleep
from sys import stdout

# Six imports
from six import itervalues

# owls-cache imports
from owls_cache.persistent import get_persistent_cache


# Export the owls-parallel version
__version__ = '0.0.1'


# Create a thread-local variable to track whether or not the current thread is
# in capture mode
_thread_local = threading.local()


# Utility function to get the current parallelizer
_get_parallelizer = lambda: getattr(_thread_local, 'owls_parallelizer', None)


# Utility function to set the current parallelizer
def _set_parallelizer(parallelizer):
    _thread_local.owls_parallelizer = parallelizer


def parallelized(default_generator, mapper):
    """Decorator to add parallelization functionality to a callable.

    The underlying function, or some function further down the call stack, must
    be wrapped with an @owls_cache.persistent.cached directive (or some other
    persistent caching mechanism which is globally available), or the result
    of the parallel computation will be lost.

    The @parallelized decorator must also be the outermost decorator used on
    the function.

    Finally, the underlying function must also be importable by name on
    engines.

    Args:
        default_generator: A function which takes the same arguments as the
            underlying function and returns a dummy default value which will be
            suitable in place of the actual return value
        mapper: A function which takes the same arguments as the underlying
            function and returns a key by which jobs can be grouped with
            maximal performance (e.g. to take advantage of caching).

    Returns:
        A version of the function which supports parallelization using a job
        capture paradigm.
    """
    # Create the decorator
    def decorator(f):
        # Create the wrapper function
        @wraps(f)
        def wrapper(*args, **kwargs):
            # Grab the current parallelizer
            parallelizer = _get_parallelizer()

            # If we're not in capture mode, then we're done
            if parallelizer is None:
                return f(*args, **kwargs)

            # Otherwise, we are in capture mode, so we need to compute the key
            # by which to organize this job.  We wrap things in a repr/hash so
            # that the mapping function result needn't be hashable itself.
            key = hash(repr(mapper(*args, **kwargs)))

            # Register the job with the parallelizer
            # NOTE: We register the *wrapper* function, because it is what will
            # be assigned to the name of the function
            parallelizer._record(key, wrapper, args, kwargs)

            # Return a dummy value
            return default_generator(*args, **kwargs)

        # Return the wrapper
        return wrapper

    # Return the decorator
    return decorator


# Global parallelization backend
_backend = None


def set_parallelization_backend(backend):
    """Sets the global parallelization backend.

    The global cache is used by parallelization environments which are not
    explicitly provided with a parallelization backend.

    Args:
        backend: An instance of a subclass of
            owls_parallel.backends.ParallelizationBackend
    """
    # Switch to the global variable
    global _backend

    # Set the backend
    _backend = backend


def get_parallelization_backend():
    """Gets the global parallelization backend.

    Returns:
        The global parallelization backend, or None if the global backend is
        not set.
    """
    return _backend


class ParallelizedEnvironment(object):
    """An environment in which functions wrapped with the @parallelized
    directive are captured when called and then executed on a given backend.
    """

    def __init__(self, backend = None, monitor_interval = 5):
        """Creates a new instance of the ParallelizedEnvironment class.

        Args:
            backend: The backend to use for parallelization.  If None is
                passed (the default), then the global parallelization backend
                will be used, or if the global parallelization backend is not
                set, no parallelization will be performed.
            monitor_interval: How often to query/print progress
        """
        # Create variables to track run state
        self._captured = False
        self._computed = False

        # Create the list of register jobs
        self._jobs = defaultdict(list)

        # Grab current persistent cache and validate it
        self._cache = get_persistent_cache()
        if self._cache is None:
            raise RuntimeError('global persistent cache not set')

        # If no backend has been provided, then try to grab the global
        # parallelization backend
        if backend is None:
            backend = get_parallelization_backend()

        # Store the backend and progress interval
        self._backend = backend
        self._monitor_interval = monitor_interval

    def _record(self, key, function, args, kwargs):
        """Adds a new job to be computed on the parallel backend.

        Args:
            key: The key by which to group the job for optimal performance.
                The key must be hashable.
            function: The function to execute
            args: The arguments to the function
            kwargs: The keyword arguments to the function
        """
        self._jobs[key].append((function, args, kwargs))

    def _compute(self, progress = True):
        """Runs computation and blocks until completion, optionally printing
        progress.

        Args:
            progress: Whether or not to print progress information
        """
        # Start jobs
        all_jobs = self._backend.start(self._cache, tuple(
            (tuple(j) for j in itervalues(self._jobs))
        ))

        # Monitor jobs
        remaining_jobs = all_jobs
        while True:
            # Grab the unfinished jobs
            remaining_jobs = self._backend.prune(remaining_jobs)

            # Compute progress
            total = len(all_jobs)
            completed = total - len(remaining_jobs)

            # Print the percentage if necessary
            if progress:
                fraction_completed = 1.0 * completed / total
                filled_blocks = int(fraction_completed / 0.1)
                empty_blocks = 10 - filled_blocks
                print(
                    '\r[{0}{1}] {2:.0f}% ({3}/{4})'.format(
                        '#' * filled_blocks,
                        ' ' * empty_blocks,
                        fraction_completed * 100,
                        completed,
                        total),
                    end = ''
                )
                stdout.flush()

            # If we're done, leave this loop
            if completed == total:
                if progress:
                    print('')
                break

            # Wait for a few seconds for results to complete
            sleep(self._monitor_interval)

    def capturing(self):
        """Returns True if the environment is in capture mode, False otherwise.
        """
        return self._captured and not self._computed

    def run(self, progress = True):
        """Manages execution in a parallelized environment, optionally printing
        progress.

        This method is designed to wrap code which should be parallelized in a
        while loop, e.g.:

            # Compute results in a parallel manner
            parallelizer = ParallelizedEnvironment(...)
            while parallelizer.run():
                # Peform computation which should be parallelized
                value_1 = function_1()
                value_2 = function_2()

            # Use results
            ...

        On the first call, this method sets the global state (within the
        thread) to a mode where all @parallelized calls are captured for later
        computation, and the method returns True.

        On the second call, the captured computations are computed in parallel,
        and this method returns True.

        On the third (or later) call, this method is a no-op and returns False.

        Args:
            progress: Whether or not to print progress information

        Returns:
            True or False depending on run state.
        """
        # Handle based on state
        if self._backend is None and not (self._captured or self._computed):
            # If we have no backend, then mark ourselves as completed the first
            # time through, since we won't be doing any parallelization
            if progress:
                print('Parallelization unavailable, capturing...')
            self._captured = True
            self._computed = True
            return True
        elif not self._captured:
            # If we haven't captured yet, then set the parallelizer for
            # capturing and allow the loop to run
            if progress:
                print('Capturing for parallelization...')
            self._captured = True
            _set_parallelizer(self)
            return True
        elif not self._computed:
            # If we have already captured but haven't computed, then unset the
            # parallelizer and run the computations in a blocking manner, then
            # allow the loop to run to pull values out of the persistent cache
            if progress:
                print('Computing in parallel...')
            self._computed = True
            _set_parallelizer(None)
            self._compute(progress)
            return True
        return False
