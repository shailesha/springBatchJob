package com.example.springBatchJob;

import com.example.springBatchJob.web.JobController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.*;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Set;

@SpringBootApplication
public class SpringBatchJobApplication implements CommandLineRunner{


	private static final Logger log = LoggerFactory.getLogger(SpringBatchJobApplication.class);
	@Autowired
	JobExplorer jobExplorer;

	@Autowired
	JobOperator jobOperator;

	//@Autowired
	//Job job;

	public static void main(String[] args) {

		SpringApplication.run(SpringBatchJobApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception
	{
		Set<JobExecution> jobExecutionSet = jobExplorer.findRunningJobExecutions("printlnJob");
		jobExecutionSet.addAll(jobExplorer.findRunningJobExecutions("taskletJob"));
		if(jobExecutionSet.size() > 0) {
			for(JobExecution exec : jobExecutionSet) {
				log.info("exit status  : job instance id " + exec.getJobInstance() + ":" + exec.getExitStatus());
				log.info("exit status  : job instance id " + exec.getJobInstance() + ":" + ExitStatus.UNKNOWN);
				log.info( "compare the two exitstatus" + exec.getExitStatus().equals(ExitStatus.UNKNOWN));
				if(exec.getStatus().equals(BatchStatus.STARTED)) {
					jobOperator.stop(exec.getId());
					jobOperator.abandon(exec.getId());
					// we can automatically restart it also afresh if there is no job dependency problem
					// we can also pass in job parameters indicating that it was started due to pod crash


					jobOperator.start(exec.getJobInstance().getJobName(), "trigger=appStartAfterpodCrashed,time="+System.currentTimeMillis());
				}
			}
			// we could send out a mail to operations on all jobs which were abandoned here.


		}

	}
}
