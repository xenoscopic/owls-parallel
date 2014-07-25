"""Non-public API - provides a test function for parallelization.

This module is necessary because parallelized functions must be importable by
name on the backend.
"""


# owls-cache imports
from owls_cache.persistent import cached as persistently_cached
from owls_cache.persistent.caches.fs import FileSystemPersistentCache

# owls-parallel imports
from owls_parallel import parallelized


# Global counter which records the number of times the computation is called
class CallCount(object):
    def __init__(self):
        self.value = 0
counter = CallCount()


@parallelized(lambda a, b: 0, lambda a, b: a)
@persistently_cached
def computation(a, b):
    """Test computation which is persistently-cached and parallelized.  It adds
    two numbers.

    Args:
        a: The first number
        b: The second number

    Returns:
        The sum of a and b.
    """
    # Increment the counter
    counter.value += 1

    # Return the result
    return a + b
