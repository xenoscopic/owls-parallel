"""Provides a portable batch system-based parallelization backend.
"""


# System imports
from os.path import exists, isdir, join
from os import makedirs
from subprocess import check_output, CalledProcessError, \
    STDOUT as MERGE_WITH_STDOUT
from uuid import uuid4
from base64 import b64encode

# Six imports
from six import itervalues
from six.moves.cPickle import dumps

# owls-parallel imports
from owls_parallel.backends import ParallelizationBackend


# Template script for submission to the batch system
_BATCH_TEMPLATE = """#!/usr/bin/env python

# System imports
from base64 import b64decode

# Six imports
from six.moves.cPickle import loads
from six import iteritems

# owls-cache imports
from owls_cache.persistent import caching_into

# Run the job
job = loads(b64decode('{job}'))
with caching_into(loads(b64decode('{cache}'))):
    for batcher, calls in iteritems(job):
        for function, args_kwargs in iteritems(calls):
            batcher(function, args_kwargs)
"""


class BatchParallelizationBackend(ParallelizationBackend):
    """A parallelization backend which uses a batch system to compute results.
    """

    def __init__(self, path, submit, monitor):
        """Initializes a new instance of the BatchParallelizationBackend.

        Args:
            path: The path in which to store batch system metadata
            submit: A function to submit jobs to the batch system, of the form:

                    submit(working_directory, script_name)

                which should return a string representing the job id
            monitor: A function to monitor the status of a job on the batch
                system, which should accept the job id as an argument and
                return True if the job is complete, False otherwise
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

    def mode(self):
        """Returns the operation mode of the backend when waiting for jobs.
        """
        return 'poll'

    def start(self, cache, job_specs, callback):
        """Run jobs on the backend, blocking until their completion.

        Args:
            cache: The persistent cache which should be set on the backend
            job_specs: The job specification (see
                owls_parallel.backends.ParallelizationBackend)
            callback: The job notification callback, not used by this backend
        """
        # Create the result list
        results = []

        # Go through each job and create a batch job for it
        for spec in itervalues(job_specs):
            # Create the job content
            batch_script = _BATCH_TEMPLATE.format(**{
                "cache": b64encode(dumps(cache)),
                "job": b64encode(dumps(spec)),
            })

            # Create an on-disk handle
            script_name = '{0}.py'.format(uuid4().hex)
            script_path = join(self._path, script_name)

            # Write it to file
            with open(script_path, 'w') as f:
                f.write(batch_script)

            # Submit the batch job and record the job id
            results.append(self._submit(self._path, script_name))

        # All done
        return results

    def prune(self, jobs):
        """Prunes a collection of jobs by pruning those which are complete.

        The input collection should not be modified.

        Args:
            jobs: A collection of jobs to prune

        Returns:
            A new collection of jobs which are still incomplete.
        """
        return [j for j in jobs if not self._monitor(j)]


def qsub_submit(working_directory, script_name):
    """Provides batch submission capabilities for portable batch systems.

    Args:
        working_directory: The working directory for the batch job
        script_name: The name of the script to submit

    Returns:
        The job id.
    """
    return check_output(['qsub', '-l', 'cput=1:00:00,walltime=1:00:00',
                         script_name],
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
        check_output(['qstat', job_id], stderr = MERGE_WITH_STDOUT)
        return False
    except CalledProcessError:
        # If the qstat command returns a non-0 exit code, it means the job was
        # not found, which means it is not running, and *hopefully* means that
        # it finished successfully
        return True
