# System imports
import unittest
from subprocess import check_output
from tempfile import mkdtemp
from sys import version_info

# owls-cache imports
from owls_cache.persistent import set_persistent_cache
from owls_cache.persistent.caches.fs import FileSystemPersistentCache

# owls-parallel imports
from owls_parallel import parallelized, ParallelizedEnvironment
from owls_parallel.backends.multiprocessing import \
    MultiprocessingParallelizationBackend

from owls_parallel.testing import counter, computation


# Set up the multiprocessing backend
multiprocessing_backend = MultiprocessingParallelizationBackend(2)


# If we're using Python 2.7 and IPython is available, create the backend
ipython_backend = None
if version_info[:2] == (2, 7):
    try:
        from owls_parallel.backends.ipython import IPythonParallelizationBackend
        ipython_backend = IPythonParallelizationBackend()
    except:
        pass


# Try to set up the batch backend, but only if the qsub command is available
batch_backend = None
try:
    check_output(['qstat'])
    from owls_parallel.backends.batch import BatchParallelizationBackend, \
        qsub_submit, qsub_monitor
    batch_backend = BatchParallelizationBackend(mkdtemp(),
                                                qsub_submit,
                                                qsub_monitor,
                                                5)
except:
    pass


class TestParallelizationBase(unittest.TestCase):
    def execute(self):
        # Create and set the global persistent cache
        set_persistent_cache(FileSystemPersistentCache(mkdtemp()))

        # Reset the counter
        global counter
        counter.value = 0

        # Create a parallelization environment with the current backend
        parallel = ParallelizedEnvironment(self._backend)

        # Run the computation a few times in the parallelized environment
        while parallel.run():
            x = computation(1, 2)
            y = computation(3, 4)
            z = computation(5, 6)

        # Make sure the computation was never invoked locally
        self.assertEqual(counter.value, 0)

        # Validate the results
        self.assertEqual(x, 3)
        self.assertEqual(y, 7)
        self.assertEqual(z, 11)

        # Run outside of the parallelization environment
        self.assertEqual(computation(7, 8), 15)
        self.assertEqual(counter.value, 1)


class TestMultiprocessingParallelization(TestParallelizationBase):
    def setUp(self):
        self._backend = multiprocessing_backend

    def test(self):
        self.execute()


@unittest.skipIf(ipython_backend is None, 'IPython cluster not available')
class TestIPythonParallelization(TestParallelizationBase):
    def setUp(self):
        self._backend = ipython_backend

    def test(self):
        self.execute()


@unittest.skipIf(batch_backend is None, 'Batch cluster not available')
class TestBatchParallelization(TestParallelizationBase):
    def setUp(self):
        self._backend = batch_backend

    def test(self):
        self.execute()


# Run the tests if this is the main module
if __name__ == '__main__':
    unittest.main()
