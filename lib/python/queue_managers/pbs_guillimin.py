import subprocess
import os
import os.path
import time

import PBSQuery

import debug
import queue_managers
import config.basic
import config.email

class PBSManager(queue_managers.generic_interface.PipelineQueueManager):
    def __init__(self, job_basename, property, walltime_per_gb=50, \
                    rapID=None):
        """Constructor for the PBS queue manager interface.

            Inputs:
                job_basename: Name of jobs in PBS
                property: Submit jobs to nodes with this name/propery
                walltime_per_gb: The amount of walltime to assign to
                                    a job per gb of input data for that job.
                rapID: identification number of Compute Canada allocation.
                                    (needed on guillimin)

            Output:
                pbsmanager: The PBSManager instance.
        """
        self.job_basename = job_basename
        self.property = property
        self.rapID = rapID
        self.walltime_per_gb = walltime_per_gb
        self.pbs_conn = PBSQuery.PBSQuery()

        # initiate last update to 10 minutes ago, so that it will be updated immediately on next _showq
        self.showq_last_update = time.time() - 2 * 300

    def submit(self, datafiles, outdir, job_id, \
                script=os.path.join(config.basic.pipelinedir, 'bin', 'search.py')):
        """Submits a job to the queue to be processed.
            Returns a unique identifier for the job.

            Inputs:
                datafiles: A list of the datafiles being processed.
                outdir: The directory where results will be copied to.
                job_id: The unique job identifer from the jobtracker database.
                script: The script to submit to the queue. (Default:
                        '{config.basic.pipelinedir}/bin/search.py')

            Output:
                jobid: A unique job identifier.
        
            *** NOTE: A queue_manager.QueueManagerJobFatalError should be
                        raised if the queue submission fails.
            *** NOTE: A queue_manager.QueueManagerNonFatalError should be
                        raised if the queue submission could not be performed.
        """
        # compute walltime needed
        filesize = 0 
        for file in datafiles:
            filesize += os.stat(file).st_size   

        filesize /= 1024.0**3
        
        walltime_hrs = int( self.walltime_per_gb * filesize)
        if walltime_hrs < 16:
            walltime = '16:00:00'
        else:
            walltime = str( walltime_hrs ) + ':00:00'
        print 'Filesize:',filesize,'GB Walltime:', walltime
	
        errorlog = os.path.join(config.basic.qsublog_dir, "'$PBS_JOBID'.ER")
        if debug.PROCESSING:
            stdoutlog = os.path.join(config.basic.qsublog_dir, "'$PBS_JOBID'.OU")
        else:
            stdoutlog = os.devnull

        # bit of hack to get jobs that are short enough to go to Sandy Bridge nodes
        if filesize < 2.1:
            resources = 'nodes=1:ppn=1:sandybridge,walltime=36:00:00'
        else:
            resources = 'nodes=1:ppn=1,walltime=%s' % walltime

        # temporarily use -V option and define variables before, since -v not working
        os.putenv('DATAFILES',';'.join(datafiles))
        os.putenv('OUTDIR',outdir)

        cmd = 'qsub -V -A %s -l %s -N %s -e %s -o %s %s' % \
                        (self.rapID, resources, \
                            self.job_basename + str(job_id), errorlog, stdoutlog, script)
        #cmd = 'qsub -V -v DATAFILES="%s",OUTDIR="%s" -A %s -l nodes=1:ppn=1,walltime=%s -N %s -e %s -o %s %s' % \
        #                (';'.join(datafiles), outdir, self.rapID, walltime, \
        #                    self.job_basename + str(job_id), errorlog, stdoutlog, script)
        if debug.QMANAGER:
            print "Job submit command: %s" % cmd
        pipe = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, \
                                stdin=subprocess.PIPE)
        queue_id = pipe.communicate()[0].strip()
        pipe.stdin.close()
        if not queue_id:
            errormsg  = "No job identifier returned by qsub!\n"
            errormsg += "\tCommand executed: %s\n" % cmd
            raise queue_manager.QueueManagerNonFatalError(errormsg)
        else:
            # There is occasionally a short delay between submission and 
            # the job appearing on the queue, so sleep for 1 second. 
            time.sleep(1)
            
            # Update the queue cache so that the new submission registers
            queue = self._get_PBSQueue(update_time=0) 
        return queue_id

    def can_submit(self):
        """Check if we can submit a job
            (i.e. limits imposed in config file aren't met)

            Inputs:
                None

            Output:
                Boolean value. True if submission is allowed.
        """
        running, queued = self.status()
        if ((running + queued) < config.jobpooler.max_jobs_running) and \
            (queued < config.jobpooler.max_jobs_queued):
            return True
        else:
            return False

    def is_running(self, queue_id):
        """Must return True/False whether the job is in the queue or not
            respectively.

        Input:
            queue_id: Unique identifier for a job.
        
        Output:
            in_queue: Boolean value. True if the job identified by 'queue_id'
                        is still running.
        """
        jobs = self._get_PBSQueue()
        return (queue_id in jobs)

    def delete(self, queue_id):
        """Remove the job identified by 'queue_id' from the queue.

        Input:
            queue_id: Unique identifier for a job.
        
        Output:
            None
            
            *** NOTE: A queue_managers.QueueManagerNonFatalError is raised if
                        the job removal fails.
        """
        cmd = "qdel %s" % queue_id
        pipe = subprocess.Popen(cmd, shell=True)
        
        # Wait a few seconds a see if the job is still being tracked by
        # the queue manager, or if it marked as exiting.
        time.sleep(5)
        jobs = self._get_PBSQueue(update_time=0)
        if (queue_id in jobs) and ('E' not in jobs[queue_id]['job_state']):
            errormsg  = "The job (%s) is still in the queue " % queue_id
            errormsg += "and is not marked as exiting (status = 'E')!\n"
            #raise queue_managers.QueueManagerNonFatalError(errormsg)
            raise pipeline_utils.PipelineError(errormsg) # for debugging

    def status(self):
        """Return a tuple of number of jobs running and queued for the pipeline

        Inputs:
            None

        Outputs:
            running: The number of pipeline jobs currently marked as running 
                        by the queue manager.
            queued: The number of pipeline jobs currently marked as queued 
                        by the queue manager.
        """
        numrunning = 0
        numqueued = 0
        jobs = self._get_PBSQueue()
        for j in jobs.keys():
            if jobs[j]['Job_Name'][0].startswith(self.job_basename):
                if 'R' in jobs[j]['job_state']:
                    numrunning += 1
                elif 'Q' in jobs[j]['job_state']:
                    numqueued += 1
        return (numrunning, numqueued)

    def _get_stderr_path(self, jobid_str):
        """A private method not required by the PipelineQueueManager interface.
            Return the path to the error log of the given job, 
            defined by its queue ID.

            Input:
                queue_id: Unique identifier for a job.

            Output:
                stderr_path: Path to the error log file provided by queue 
                        manger for this job.
        
            NOTE: A ValueError is raised if the error log cannot be found.
        """
        stderr_path = os.path.join(config.basic.qsublog_dir, "%s.ER" % jobid_str)
        if not os.path.exists(stderr_path):
            raise ValueError("Cannot find error log for job (%s): %s" % \
                        (jobid_str, stderr_path))
        return stderr_path

    def had_errors(self, queue_id):
        """Given the unique identifier for a job, return if the job 
            terminated with an error or not.

        Input:
            queue_id: Unique identifier for a job.
        
        Output:
            errors: A boolean value. True if this job terminated with an error.
                    False otherwise.
        """

        try:
            errorlog = self._get_stderr_path(queue_id)
        except ValueError:
            errors = True
        else:
            if os.path.getsize(errorlog) > 0:
                errors = True
            else:
                errors = False
        return errors

    def get_errors(self, queue_id):
        """Return content of error log file for a given queue ID.
        
            Input:
                queue_id: Queue's unique identifier for the job.

            Output:
                errors: The content of the error log for this job (a string).
        """
        try:
            errorlog = self._get_stderr_path(queue_id)
        except ValueError, e:
            errors = str(e)
        else:
            if os.path.exists(errorlog):
                err_f = open(errorlog, 'r')
                errors = err_f.read()
                err_f.close()
        return errors

    def _get_PBSQueue(self, update_time=300):
        """A private method not required by the PipelineQueueManager interface.
            Query the PBS queue if time since last update is < update_time.
            Otherwise return the already cached queue.

            Optional Input:
                update_time: Will update cache if time since last update is
                                 less than update_time

            Output:
                queue: Output of PBSQuery.PBSQuery().getjobs()
        """

        if time.time() >= self.showq_last_update + update_time:
            print "Updating queue cache ..."

            queue = self.pbs_conn.getjobs()
            
            self.queue = queue
            self.showq_last_update = time.time()
        else:
            queue = self.queue
                      
        return queue
