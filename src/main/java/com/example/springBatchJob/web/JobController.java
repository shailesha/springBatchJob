package com.example.springBatchJob.web;

import com.example.springBatchJob.PersonItemProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.*;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Set;

@RestController
public class JobController {


    private static final Logger log = LoggerFactory.getLogger(JobController.class);

    @Autowired
    @Qualifier("asyncJobLauncher")
    JobLauncher asyncJobLauncher;

    //default sync job launcher loaded by spring batch
    @Autowired
    JobLauncher jobLauncher;

    @Autowired
    JobExplorer jobExplorer;

    @Autowired
    JobOperator jobOperator;

    @Autowired
    @Qualifier("importUserJob")
    Job importUserJob;

    @Autowired
    @Qualifier("printlnJob")
    Job printlnJob;

    @Autowired
   // @Qualifier("taskletJob")
    Job taskletJob;


    @PostMapping("/invokejob")
    @ResponseBody
    public String invokeJob(@RequestParam(name = "name") String jobName) throws Exception {

        JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addLong("time", System.currentTimeMillis());

        JobParameters jobParameters = null;
        JobExecution jobExecution = null;
        Long instanceId1 = null;

                log.info ("jobName ="  + jobName);
        if (jobName.equals("printlnJob")) {
            jobParameters =  jobParametersBuilder.addString("endOfDayPosting", "Y")
                    .toJobParameters();

            if(jobExplorer.findRunningJobExecutions("printlnJob").size() == 0 ) {
                log.info("No existing job running ");
                jobExecution = asyncJobLauncher.run(printlnJob, jobParameters);
            } else {
                Set<JobExecution> jobExecutions = jobExplorer.findRunningJobExecutions("printlnJob");
                for(JobExecution exec : jobExecutions) {
                    if(exec.getExitStatus().equals(ExitStatus.EXECUTING)) {
                        log.info("Batch job is already running ");
                        return "Batch job is already running ";
                    } else if ( exec.getExitStatus().equals(ExitStatus.FAILED)
                            || exec.getExitStatus().equals(ExitStatus.STOPPED)
                            || exec.getExitStatus().equals(ExitStatus.UNKNOWN)
                    ){
                        Long instanceIdToResume = exec.getJobInstance().getInstanceId();
                        Long restartId = jobOperator.restart(instanceIdToResume);
                        log.info("Batch job id " + instanceIdToResume + " has been restarted with restart id as  " + restartId);
                        return "Batch job id " + instanceIdToResume + " has been restarted with restart id as  " + restartId;
                    }  else {
                        log.info("Batch job status " + exec.getExitStatus());
                    }
                }
                log.info("no other jobs running. a new job is being submitted");
                jobExecution = asyncJobLauncher.run(printlnJob, jobParameters);


            }
        }else if (jobName.equals("taskletJob")) {
            jobParameters =  jobParametersBuilder.addString("endOfDayPosting", "Y")
                    .toJobParameters();

                Set<JobExecution> jobExecutionSet = jobExplorer.findRunningJobExecutions("printlnJob");
                if(jobExecutionSet.size() > 0) {
                    for(JobExecution exec : jobExecutionSet) {
                        log.info("exit status printlnjob : job instance id " + exec.getJobInstance() + ":" + exec.getExitStatus());
                        log.info("exit status printlnjob : job instance id " + exec.getJobInstance() + ":" + ExitStatus.UNKNOWN);
                        log.info( "compare the two exitstatus" + exec.getExitStatus().equals(ExitStatus.UNKNOWN));
                        if(exec.getStatus().equals(BatchStatus.STARTED)) {
                            jobOperator.stop(exec.getId());
                            jobOperator.abandon(exec.getId());
                        }
                    }
                }



              if(jobExplorer.findRunningJobExecutions("taskletJob").size() == 0 ) {
                log.info("No existing job running ");

                jobExecution = asyncJobLauncher.run(taskletJob, jobParameters);
              } else {
                  Set<JobExecution> jobExecutions = jobExplorer.findRunningJobExecutions("taskletJob");
                  for(JobExecution exec : jobExecutions) {
                      log.info("exit status : job instance id " + exec.getJobInstance() + ":" + exec.getExitStatus()+ ":" + exec.getStatus());
                      if(exec.getStatus().equals(BatchStatus.STARTED)) {
                          jobOperator.stop(exec.getId());
                          jobOperator.abandon(exec.getId());
                      }
                  }
                  log.info("Batch job is already running ");
                  return "Batch job is already running ";
                /*Set<JobExecution> jobExecutions = jobExplorer.findRunningJobExecutions("taskletJob");
                for(JobExecution exec : jobExecutions) {
                    log.info("exit status : job instance id " + exec.getJobInstance() + ":" +  exec.getExitStatus());
                    if(exec.getExitStatus().equals(ExitStatus.EXECUTING)) {
                        log.info("Batch job is already running ");
                        return "Batch job is already running ";
                    } else if ( exec.getExitStatus().equals(ExitStatus.FAILED)
                            || exec.getExitStatus().equals(ExitStatus.STOPPED)
                            || exec.getExitStatus().equals(ExitStatus.UNKNOWN)
                    ){
                        Long instanceIdToResume = exec.getJobInstance().getInstanceId();
                        Long restartId = jobOperator.restart(instanceIdToResume);
                        log.info("Batch job id " + instanceIdToResume + " has been restarted with restart id as  " + restartId);
                        return "Batch job id " + instanceIdToResume + " has been restarted with restart id as  " + restartId;
                    }  else {
                        log.info("Batch job status " + exec.getExitStatus());
                    }
                }
                log.info("no other jobs running. a new job is being submitted");
                jobExecution = jobLauncher.run(taskletJob, jobParameters);*/


            }
        } else if(jobName.equals("importUserJob")){
            jobParameters =  jobParametersBuilder.addString("endOfDayPosting", "N")
                    .toJobParameters();
            if(jobExplorer.findRunningJobExecutions("importUserJob").size() == 0 ){
                jobExecution = asyncJobLauncher.run(importUserJob, jobParameters);
            } else {
                log.info("Batch job is already running ");
                return "Batch job is already running ";
            }
        } else if(jobName.equals("all")){
            jobParameters =  jobParametersBuilder.addString("endOfDayPosting", "N")
                    .toJobParameters();
            jobExecution = asyncJobLauncher.run(importUserJob, jobParameters);
            instanceId1 = jobExecution.getJobInstance().getInstanceId();
            jobExecution = asyncJobLauncher.run(importUserJob, jobParameters);

        }
        Long instanceId = jobExecution.getJobInstance().getInstanceId();

                log.info ("Batch job has been invoked " + instanceId1 + "," + instanceId);

        return "Batch job has been invoked " + instanceId1 + "," + instanceId;
    }
}
