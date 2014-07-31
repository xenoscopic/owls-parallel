# System imports
import unittest
from subprocess import check_output
from os.path import abspath
from uuid import uuid4
from os import makedirs
from shutil import rmtree

# owls-cache imports
from owls_cache.persistent import caching_into
from owls_cache.persistent.caches.fs import FileSystemPersistentCache

# owls-parallel imports
from owls_parallel import parallelized, ParallelizedEnvironment
from owls_parallel.backends.multiprocessing import \
    MultiprocessingParallelizationBackend
from owls_parallel.backends.batch import BatchParallelizationBackend, \
    qsub_submit, qsub_monitor
from owls_parallel.testing import counter, computation


# Check if IPython support is available, and try to setup a backend for it,
# which will fail if a cluster is not available
ipython_backend = None
try:
    from owls_parallel.backends.ipython import IPythonParallelizationBackend
    ipython_backend = IPythonParallelizationBackend()
except:
    pass


# Check if batch parallelization is available (the backend will be created
# later once the working directory is known)
batch_available = False
try:
    check_output(['qstat'])
    batch_available = True
except:
    pass


class TestParallelizationBase(unittest.TestCase):
    def setUp(self):
        # Create a temporary working directory for the tests
        self.working_directory = abspath('.testing-{0}'.format(uuid4().hex))
        makedirs(self.working_directory)

    def tearDown(self):
        # Clean up the working directory
        rmtree(self.working_directory)

    def execute(self, is_null = False):
        # Reset the counter
        counter.value = 0

        # Create a parallelization environment with the current backend
        parallel = ParallelizedEnvironment(self._backend, 5)

        # Run the computation a few times in the parallelized environment
        loop_count = 0
        with caching_into(FileSystemPersistentCache(self.working_directory)):
            while parallel.run(False):
                # Run some computations
                x = computation(1, 2)
                y = computation(3, 4)
                z = computation(5, 6)

                # Check that we can monitor if we're capturing
                if loop_count == 0 and not is_null:
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
        # Call superclass setup
        super(TestNullParallelization, self).setUp()

        # Set the backend
        self._backend = None

    def test(self):
        self.execute(True)


class TestMultiprocessingParallelization(TestParallelizationBase):
    def setUp(self):
        # Call superclass setup
        super(TestMultiprocessingParallelization, self).setUp()

        # Set the backend
        self._backend = MultiprocessingParallelizationBackend(2)

    def test(self):
        self.execute()


@unittest.skipIf(ipython_backend is None, 'IPython cluster not available')
class TestIPythonParallelization(TestParallelizationBase):
    def setUp(self):
        # Call superclass setup
        super(TestIPythonParallelization, self).setUp()

        # Set the backend
        self._backend = ipython_backend

    def test(self):
        self.execute()


@unittest.skipIf(not batch_available, 'Batch cluster not available')
class TestBatchParallelization(TestParallelizationBase):
    def setUp(self):
        # Call superclass setup
        super(TestBatchParallelization, self).setUp()

        # Set the backend
        self._backend = BatchParallelizationBackend(self.working_directory,
                                                    qsub_submit,
                                                    qsub_monitor)

    def test(self):
        self.execute()


# Run the tests if this is the main module
if __name__ == '__main__':
    unittest.main()
