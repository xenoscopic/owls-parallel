"""Provides a portable batch system-based parallelization backend.
"""


# System imports
from os.path import exists, isdir, join
from os import makedirs
from subprocess import check_output, CalledProcessError
from uuid import uuid4
from time import sleep

# Six imports
from six.moves.cPickle import dumps

# owls-parallel imports
from owls_parallel.backends import ParallelizationBackend


# Template script for submission to the batch system
_BATCH_TEMPLATE = """#!/usr/bin/env python

# Six imports
from six.moves.cPickle import loads

# Initialize the persistent cache
from owls_cache.persistent import set_persistent_cache
set_persistent_cache(loads('{cache}'))

# Run the operations
operations = loads('{operations}')
for function, args, kwargs in operations:
    function(*args, **kwargs)
"""


class BatchParallelizationBackend(ParallelizationBackend):
    """A parallelization backend which uses a batch system to compute results.
    """

    def __init__(self, path, submit, monitor, interval = 5):
        """Initializes a new instance of the BatchParallelizationBackend.

        Args:
            path: The path in which to store batch system metadata
            submit: A function to submit jobs to the batch system, of the form:

                    submit(working_directory, script_name)

                which should return a string representing the job id
            monitor: A function to monitor the status of a job on the batch
                system, which should accept the job id as an argument and
                return True if the job is complete, False otherwise
            interval: The polling interval at which to check batch system
                completion
        """
        # Make sure the path exists and store it
        if exists(path):
            if not isdir(path):
                raise OSError('cache path exists and is not a directory')
        else:
            # Just pass on exceptions
            makedirs(path)
        self._path = path

        # Store submission/monitoring commands and polling interval
        self._submit = submit
        self._monitor = monitor
        self._interval = interval

    def compute(self, cache, jobs):
        """Run jobs on the backend, blocking until their completion.

        Args:
            cache: The persistent cache which should be set on the backend
            jobs: The job specification (see
                owls_parallel.backends.ParallelizationBackend)
        """
        # Create a list to track unfinished ids
        unfinished = []

        # Go through each job and create a batch job for it
        for job in jobs:
            # Create the job content
            batch_script = _BATCH_TEMPLATE.format(**{
                "cache": dumps(cache),
                "operations": dumps(job)
            })

            # Create an on-disk handle
            script_name = '{0}.py'.format(uuid4().hex)
            script_path = join(self._path, script_name)

            # Write it to file
            with open(script_path, 'w') as f:
                f.write(batch_script)

            # Submit the batch job and record the job id
            unfinished.append(self._submit(self._path, script_name))

        # Monitor completion of batch jobs
        while True:
            # Sleep
            sleep(self._interval)

            # Check remaining jobs
            unfinished = [j for j in unfinished if not self._monitor(j)]

            # If everything is done, return
            if len(unfinished) == 0:
                break


def qsub_submit(working_directory, script_name):
    """Provides batch submission capabilities for portable batch systems.

    Args:
        working_directory: The working directory for the batch job
        script_name: The name of the script to submit

    Returns:
        The job id.
    """
    return check_output(['qsub', script_name],
                        cwd = working_directory).strip('\n')


def qsub_monitor(job_id):
    """Provides batch monitoring capabilities for portable batch systems.

    Args:
        job_id: The job id to check

    Returns:
        True if the job has completed, False otherwise.
    """
    try:
        # If the qstat command returns a 0 exit code, it means the jobs was
        # found, which means it is running
        check_output(['qstat', job_id])
        return False
    except CalledProcessError:
        # If the qstat command returns a non-0 exit code, it means the job was
        # not found, which means it is not running, and *hopefully* means that
        # it finished successfully
        return True
