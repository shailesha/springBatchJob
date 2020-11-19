package com.example.springBatchJob;

import com.example.springBatchJob.web.JobController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.*;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

import java.util.Set;

@SpringBootApplication
public class SpringBatchJobApplication implements CommandLineRunner{


	private static final Logger log = LoggerFactory.getLogger(SpringBatchJobApplication.class);


	@Autowired
	private ApplicationContext applicationContext;

	@Autowired
	JobExplorer jobExplorer;

	@Autowired
	JobOperator jobOperator;

	@Autowired
	@Qualifier("asyncJobLauncher")
	JobLauncher asyncJobLauncher;

	//@Autowired
	//Job job;

	public static void main(String[] args) {

		SpringApplication.run(SpringBatchJobApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception
	{
//		//if the job instance is singleton, this  could work to close orphan jobs and restart
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
					// we can automatically restart it also afresh if there is no job dependency problem - same dependencies as in job controller would apply
					// we can also pass in job parameters indicating that it was started due to pod crash

					JobParametersBuilder jobParametersBuilder = new JobParametersBuilder(exec.getJobParameters());
					jobParametersBuilder.addString("trigger", "app Start After pod Crash. abandoned execution id:" + exec.getId());
					Job job = null;
					if(exec.getJobInstance().getJobName().equals("taskletJob")) {
						job = (Job)applicationContext.getBean("taskletJob");
					} else {
						job = (Job)applicationContext.getBean("printlnJob");
					}
					asyncJobLauncher.run(job,jobParametersBuilder.toJobParameters());


				} else if(exec.getStatus().equals(BatchStatus.STOPPING) ){
					jobOperator.abandon(exec.getId());
				}
			}
			// we could send out a mail to operations on all jobs which were abandoned here.


		}

	}
}
