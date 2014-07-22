# System imports
import unittest
from subprocess import check_output
from tempfile import mkdtemp

# owls-cache imports
from owls_cache.persistent import set_persistent_cache
from owls_cache.persistent.caches.fs import FileSystemPersistentCache

# owls-parallel imports
from owls_parallel import parallelized, ParallelizedEnvironment
from owls_parallel.backends.null import NullParallelizationBackend
from owls_parallel.backends.multiprocessing import \
    MultiprocessingParallelizationBackend
from owls_parallel.testing import counter, computation


# Set up the null parallelization backend
null_backend = NullParallelizationBackend()


# Set up the multiprocessing backend
multiprocessing_backend = MultiprocessingParallelizationBackend(2)


# Try to set up the IPython backend, but only if a cluster is available
ipython_backend = None
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
    def execute(self, is_null = False):
        # Create and set the global persistent cache
        set_persistent_cache(FileSystemPersistentCache(mkdtemp()))

        # Reset the counter
        global counter
        counter.value = 0

        # Create a parallelization environment with the current backend
        parallel = ParallelizedEnvironment(self._backend)

        # Run the computation a few times in the parallelized environment
        loop_count = 0
        while parallel.run():
            # Run some computations
            x = computation(1, 2)
            y = computation(3, 4)
            z = computation(5, 6)

            # Check that we can monitor if we're capturing
            if loop_count == 0:
                self.assertTrue(parallel.capturing())
            else:
                self.assertFalse(parallel.capturing())
            loop_count += 1

        # Make sure the computation was never invoked locally
        self.assertEqual(counter.value, 3 if is_null else 0)

        # Validate the results
        self.assertEqual(x, 3)
        self.assertEqual(y, 7)
        self.assertEqual(z, 11)

        # Run outside of the parallelization environment
        self.assertEqual(computation(7, 8), 15)
        self.assertEqual(counter.value, 4 if is_null else 1)


class TestNullParallelization(TestParallelizationBase):
    def setUp(self):
        self._backend = null_backend

    def test(self):
        self.execute(True)


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
