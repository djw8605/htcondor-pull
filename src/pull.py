from os.path import expanduser
import os


import htcondor
import classad

htcondor.set_subsystem("TOOL")
htcondor.param['TOOL_DEBUG'] = 'D_FULLDEBUG'
#htcondor.param['TOOL_LOG'] = '/tmp/log'
# htcondor.enable_log()    # Send logs to the log file (/tmp/foo)
htcondor.enable_debug()


sandbox_dir = os.path.join(expanduser("~"), ".sandbox")

class RoutedJob(object):
    def __init__(self, src_ad):

        self.src_job_id = "{}.{}".format(src_ad["ClusterId"], src_ad["ProcId"])
        
        self.dest_job_id = None
        if 'RoutedToJobId' in src_ad:
            self.dest_job_id = src_ad["RoutedToJobId"]
        self.src_ad = src_ad
        
        # For now, we don't care much about the dest ad
        self.dest_ad = None
    

class Router:
    def __init__(self):
        self.schedd = None
        self.jobs = []
        
        self.config()
        
        self.RebuildJobList()
        
    def config(self):
        # Get the source schedd
        self.src_schedd = htcondor.Schedd()
        
        # Get the destination schedd
        self.dest_schedd = self.src_schedd
        
    def Poll(self):
        """
        Periodically run
        """
        self.GetCandidateJobs()
        
        for job in self.jobs:
            self.TakeOverJob(job)
            self.SubmitJob(job)
            self.CheckSubmittedJobStatus(job)
            self.FinalizeJob(job)
            self.CleanupJob(job)
            self.CleanupRetiredJob(job)
            
    
    def RebuildJobList(self):
        # Query the source schedd for jobs we should know about
        jobs = self.src_schedd.xquery('Managed == "External" && RoutedBy == "HTCondor Pull"')
        
        for job in jobs:
            print("Adding job {} in rebuild".format(job['ClusterId']))
            self.jobs.append(RoutedJob(job))
        
    def GetCandidateJobs(self):
        # Get all the jobs not currently managed by this router
        jobs = self.src_schedd.xquery('Managed =!= "External" && isUndefined(RoutedBy)')
        
        for job in jobs:
            self.jobs.append(RoutedJob(job))
        
        
    def TakeOverJob(self, job):
        if 'Managed' not in job.src_ad:
            self._editJob(self.src_schedd, job.src_ad, "Managed", '"External"')
            self._editJob(self.src_schedd, job.src_ad, "RoutedBy",
                    '"{}"'.format("HTCondor Pull"))
    
    def SubmitJob(self, job):
        if 'RoutedToJobId' not in job.src_ad:
            
            # Transfer the input data
            self.src_schedd.transferInputSandbox("ClusterId == {} && ProcId == {}".format(job.src_ad['ClusterId'], job.src_ad['ProcId']), sandbox_dir)
            
            # Make a deep copy of the src_ad
            new_job = classad.ClassAd(str(job.src_ad))
            
            # Transform the job ad to get ready for submission
            old_job_id = '{}.{}'.format(job.src_ad['ClusterId'], job.src_ad['ProcId'])
            new_job["RoutedFromJobId"] = old_job_id
            new_job["RoutedBy"] = "HTCondor Pull"
            new_iwd = os.path.join(sandbox_dir, str(
                job.src_ad['ClusterId']), str(job.src_ad['ProcId']))
            new_job['IWD'] = new_iwd
            new_job['Requirements'] = classad.ExprTree(
                '(TARGET.Arch == "X86_64") && (TARGET.OpSys == "LINUX") && (TARGET.Disk >= RequestDisk) && (TARGET.Memory >= RequestMemory) && (TARGET.HasFileTransfer)')
            del new_job["GlobalJobId"]
            del new_job["ClusterId"]
            del new_job["ProcId"]

            submitted_jobs = []
            new_job_id = self.dest_schedd.submit(new_job, ad_results=submitted_jobs)
            print new_job_id
            print submitted_jobs
            job.dest_ad = submitted_jobs[0]
            job.dest_job_id = '{}.{}'.format(job.dest_ad['ClusterId'], job.dest_ad['ProcId'])

            self._editJob(self.src_schedd, job.src_ad, "RoutedToJobId", '"{}"'.format(job.dest_job_id))
        

    def CheckSubmittedJobStatus(self, job):
        if 'RoutedToJobId' in job.src_ad:
            
            # Get the job ad from the schedd, update our in-memory version
            print("Getting job status of: {}".format(job.dest_job_id))
            jobs = self.dest_schedd.query("ClusterId == {} && ProcId == {}".format(job.dest_job_id.split(".")[0], job.dest_job_id.split(".")[1]))
            
            # We should only get 1 job back
            if len(jobs) != 1:
                print("Got {} job back when querying for a specific job".format(len(jobs)))
                return
            updated_job = jobs[0]
            
            # Update the in-memory ad
            job.dest_ad = updated_job
            
            # Propagate some of the information back to the original ad
            self._editJob(self.src_schedd, job.src_ad, "JobStatus", str(job.dest_ad["JobStatus"]))
    
    def FinalizeJob(self, job):
        if job.dest_ad is not None:
            if 'JobStatus' in job.dest_ad and job.dest_ad['JobStatus'] == 4:
                self._editJob(self.src_schedd, job.src_ad, "JobStatus", str(4))
                print("Removing job: {}".format(job.dest_job_id))
                self.dest_schedd.act(htcondor.JobAction.Remove, [job.dest_job_id])
            if 'JobStatus' in job.src_ad and job.src_ad['JobStatus'] == 3:
                # Remove the dest job
                print("Removing job: {}".format(job.dest_job_id))
                self.dest_schedd.act(htcondor.JobAction.Remove, [job.dest_job_id])
                
                
    def CleanupJob(self, job):
        pass
    
    def CleanupRetiredJob(self, job):
        pass
        
    def _editJob(self, schedd, ad, attribute, value):
        jobid = '{}.{}'.format(ad['ClusterId'], ad['ProcId'])
        ad[attribute] = value
        return schedd.edit([jobid], attribute, value)




def main():
    # Make sure sandbox directory exists
    if not os.path.isdir(sandbox_dir):
        os.mkdir(sandbox_dir)
    
    router = Router()
    router.Poll()

    


if __name__ == "__main__":
    main()
