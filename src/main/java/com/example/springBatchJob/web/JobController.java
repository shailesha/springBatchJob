package com.example.springBatchJob.web;

import com.example.springBatchJob.JobStopSignalRepository;
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

import java.util.Collection;
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

    @PostMapping("/killjob")
    @ResponseBody
    public String killJob(@RequestParam(name = "name") String jobName) throws Exception {
        if (jobName.equals("taskletJob")) {
            Set<JobExecution> jobExecutions = jobExplorer.findRunningJobExecutions("taskletJob");
            for(JobExecution exec : jobExecutions) {
                log.info("exit status : job instance id " + exec.getJobInstance() + ":" + exec.getExitStatus()+ ":" + exec.getStatus());
                if(exec.getStatus().equals(BatchStatus.STARTED)) {


                    Collection<StepExecution> stepExecutions = exec.getStepExecutions();
                    for(StepExecution stepExec : stepExecutions) {
                        log.info("step execution id : " + stepExec.getId() + " is being marked as terminated");
                        stepExec.setTerminateOnly();
                    }
                    //log.info("exec.id in controller = " + exec.getId());

                    //if the job instance is singleton, this hack could work
                    JobStopSignalRepository.signalStopToJobExecution(exec);
                    //Thread.sleep(2000);
                   // if(jobOperator.stop(exec.getId())) {
                     //   Thread.sleep(1000);
                    //}

                   /* if(jobOperator.stop(exec.getId())) {
                        Thread.sleep(2000);
                    } else {
                        log.info("job not stopping");
                    }*/
                    //jobOperator.abandon(exec.getId());
                    //log.info("job instance id = " + exec.getJobInstance().getId() + " killed");
                }
            }
        }
        return "done";
    }
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

            // no two println jobs should run together
            if(jobExplorer.findRunningJobExecutions("printlnJob").size() == 0 ) {
                log.info("No existing println job running ");

                //if tasklet job is running, then also println job should not be running
                if(jobExplorer.findRunningJobExecutions("taskletJob").size() == 0 ) {
                    jobExecution = asyncJobLauncher.run(printlnJob, jobParameters);
                } else {
                    log.info("tasklet job running ");
                    return "tasklet job running";
                }
            } else {
                Set<JobExecution> jobExecutions = jobExplorer.findRunningJobExecutions("taskletJob");
                for(JobExecution exec : jobExecutions) {
                    log.info("exit status : job instance id " + exec.getJobInstance() + ":" + exec.getExitStatus()+ ":" + exec.getStatus());
                }
                log.info("println  job is already running ");
                return "println Batch job is already running ";


            }
        }else if (jobName.equals("taskletJob")) {
            jobParameters =  jobParametersBuilder.addString("endOfDayPosting", "Y")
                    .toJobParameters();





              if(jobExplorer.findRunningJobExecutions("taskletJob").size() == 0 ) {
                log.info("No existing job running ");

                  if(jobExplorer.findRunningJobExecutions("printlnJob").size() == 0 ) {
                      jobExecution = asyncJobLauncher.run(taskletJob, jobParameters);
                  } else {
                      log.info("println job running ");
                      return "println job running";
                  }

              } else {
                  Set<JobExecution> jobExecutions = jobExplorer.findRunningJobExecutions("taskletJob");
                  for(JobExecution exec : jobExecutions) {
                      log.info("exit status : job instance id " + exec.getJobInstance() + ":" + exec.getExitStatus()+ ":" + exec.getStatus());

                  }
                  log.info("tasklet Batch job is already running ");
                  return "tasklet Batch job is already running ";



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
