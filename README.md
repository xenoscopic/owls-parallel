# owls-parallel

[![Build Status](https://travis-ci.org/havoc-io/owls-parallel.png?branch=master)](https://travis-ci.org/havoc-io/owls-parallel)

This is the parallelization module for the OWLS analysis framework.  The goal of
this module is two-fold.  First, this module is designed to coallesce function
calls that might be arbitrarily scattered around a program into batches that
might be more efficiently evaluated simultaneously.  For example, calls to a
histogram function that use the same large dataset stored on disk might be more
efficiently evaluated if they can be grouped based on the dataset argument and
evaluated by loading the dataset from disk only once and performing all
histogramming at once.  This is useful for being able to structure analysis code
in a more natural, serial manner, rather than trying to manually organize and
batch computations.  Second, this module is designed to evaluate these batches
in parallel on a variety of different parallelization backends, including a
process pool, an IPython cluster, a Portable Batch System, or a custom
parallelization backend.  See the "Usage" section below for more information.


## Requirements

The OWLS analysis framework supports Python 2.7, 3.3, 3.4, and 3.5.

All functions flagged for batching and parallelization must also be persistently
memoized in a common persistent store using the owls-cache module, which must
also be installed.


## Installation

Installation is most easily performed via pip:

    pip install git+https://github.com/havoc-io/owls-parallel.git

Alternatively, you may execute `setup.py` manually, but this is not supported.


## Usage

Because this module aims to satisfy both batching and parallelization goals, the
approach it takes is somewhat unique.  The basic idea is that it does two "runs"
of code within a Python context.  The first run is a "capture" run - it runs
through arbitrarily structured code, returning "dummy" values for functions
flagged for batching and parallelization, recording the arguments passed to
these functions, and creating optimized batches of function calls for
parallelization.  By returning dummy values that have the same interface and
algebraic behavior as the real values, code can be naturally structured and
still run in a manner that allows function calls to be captured.  After the
capture run, the batches are evaluated in parallel.  Instead of retrieving
results from the parallelization backend, functions flagged for batching and
parallelization must also be flagged for persistent memoization into a common
persistent store using the
[owls-cache module](https://github.com/havoc-io/owls-cache).  Once parallel
computation is complete, the code inside the parallelization context enters the
second run, where all function calls simply return their persistently memoized
values.

Parallelization of functions is provided by the `owls_parallel.parallelized`
decorator.  For this decorator to have any beneficial effect, it must be
combined with some sort of persistent memoization, usually provided by the
owls-cache module:

    # owls-cache imports
    from owls_cache.persistent import cached as persistently_cached

    # owls-parallel imports
    from owls_parallel import parallelized

    # Create a function that is persistently memoized, has its calls batched
    # based on its first argument, and parallelized
    @parallelized(lambda a, b: 0, lambda a, b: (a,))
    @persistently_cached('example.computation', lambda a, b: (a, b))
    def computation(a, b):
        # You'd usually do something more expensive here to justify the effort
        # of batching and parallelizing
        return a + b

In this example, a simple function called `computation` is persistently memoized
using the owls-cache module's `persistently_cached` decorator (see the
owls_cache documentation for details).  The function is then flagged for
batching and parallelization using the `owls_parallel.parallelized` decorator.
This decorator has two required arguments.  The first argument is a function
that takes the same arguments as the underlying function and returns a dummy
value that can be propagated during the capture run.  The second argument is a
function that takes the same arguments as the underlying function and returns a
key by which calls to the underlying function should be batched (in this case,
calls will be grouped based on their first argument, but in general this would
be something like a dataset).  The decorator also takes an optional third
argument, which is a function that can be called with a list of `(args, kwargs)`
tuples representing calls to the underlying function which might be more
efficiently performed simultaneously (e.g. by loading a datset only once).  The
default value of this third argument is a function which simply loops over all
`(args, kwargs)` tuples and evaluates the underlying function, which might work
well if there are other data loading caching mechanisms at play, but generally
speaking you'll want to create some custom batching function (in the trivial
example above, it might be something like SIMD addition).

This function can then be used inside memoization/parallelization contexts:

    # owls-cache imports
    from owls_cache.persistent import caching_into
    from owls_cache.persistent.caches.fs import FileSystemPersistentCache

    # owls-parallel imports
    from owls_parallel import parallelized, ParallelizedEnvironment
    from owls_parallel.backends.multiprocessing import \
        MultiprocessingParallelizationBackend

    # Create a persistent store for memoization
    cache = FileSystemPersistentCache('/tmp')

    # Create a parallelization environment with a process pool of size 5
    parallel = ParallelizedEnvironment(MultiprocessingParallelizationBackend(5))

    # Use the specified persistent store
    with caching_into(cache):
        # Execute in a parallelized context
        while parallel.run():
            # Run a variety of computations
            x = computation(1, 2)

            # This computation will be batched with the one above
            y = computation(1, 4)

            # This computation will be its own batch since it has a different
            # first argument
            z = computation(2, 6)

    # x, y, and z can now be used...

One very important caveat: functions flagged for batching and parallelization
MUST be globally importable by name on systems where they are computed in
parallel, just like with the multiprocessing module and IPython.  This is
necessary so that these functions can be pickled.  This is an unfortunate
restriction of Python (why can't it just pickle freakin' bytecode?).  In most
cases though, this is not a problem for OWLS users, because the owls-hep module
provides most of the parallelization functionality required, and only owls-hep
functions need to be importable on worker nodes, not the end-user's code.

Three parallelization backends are provided by the owls-parallel module: a
process pool based on Python's multiprocessing module, an IPython cluster, or a
Portable Batch system.  Their functionality is detailed below.

Additionally, custom backends can be easily implemented by subclassing
`owls_parallel.backends.ParallelizationBackend`.  Required methods are
documented in the docstrings of the corresponding module, and examples can be
found in the existing backends.


### Multiprocessing

The multiprocessing backend is implemented by the
`owls_parallel.backends.multiprocessing.MultiprocessingParallelizationBackend`
class.  Its constructor takes the same arguments as the `multiprocessing.Pool`
constructor.


### IPython

The IPython cluster backend is implemented by the
`owls_parallel.backends.ipython.IPythonParallelizationBackend` class.  Its
constructor takes the same arguments as the `IPython.parallel.Client`
constructor.  Note that this class does not create an IPython cluster, it merely
connects to one.


### Portable Batch System

The Portable Batch System backend is implemented by the
`owls_parallel.backends.batch.BatchParallelizationBackend`.  Its constructor
takes three arguments: a folder in which to store batch output, a submit
function, and a monitor function.  More information on the requirements for
each, as well as examples of such functions, can be found in the docstrings of
the corresponding module.  This backend is fragile and inefficient because it
relies on such ancient technology, but it does work.
