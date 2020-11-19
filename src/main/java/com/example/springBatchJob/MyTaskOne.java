package com.example.springBatchJob;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;

import java.util.Map;

public class MyTaskOne implements Tasklet {



    private static final Logger log = LoggerFactory.getLogger(MyTaskOne.class);

    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception
    {
        try {
            log.info("MyTaskOne start..");


            //getting passed in job parameters and job instance id
            Map<String, Object> jobParameters = chunkContext.getStepContext().getJobParameters();
            Long jobInstanceId = chunkContext.getStepContext().getJobInstanceId();

            FlatFileItemReader<Person> flatFileItemReader = new FlatFileItemReaderBuilder<Person>()
                    .name("personItemReader")
                    .resource(new ClassPathResource("sampledata.csv"))
                    .delimited()
                    .names(new String[]{"firstName", "lastName"})
                    .fieldSetMapper(new BeanWrapperFieldSetMapper() {{
                        setTargetType(Person.class);
                    }})
                    .build();
            flatFileItemReader.open(new ExecutionContext(chunkContext.getStepContext().getJobExecutionContext()));

            Person person = null;

            while ((person = flatFileItemReader.read()) != null) {
           /* log.info("terminate flag set for id = " + contribution.getStepExecution().getId() + ":" + contribution.getStepExecution().isTerminateOnly());
            if(chunkContext.getStepContext().getStepExecution().isTerminateOnly()) {
                throw new StepInterruptException();
            }
            log.info("Job execution status = " + chunkContext.getStepContext().getStepExecution().getJobExecution().getStatus());
                //throw new StepInterruptException();
           // }
            if(chunkContext.getStepContext().getStepExecution().getJobExecution().getStatus().equals(BatchStatus.STOPPING)) {
                throw new StepInterruptException();
            }*/
                log.info("exec.id in task = " + chunkContext.getStepContext().getStepExecution().getJobExecution().getId());
                BatchStatus jobStopSignal = JobStopSignalRepository.getStopSignalForJobEXecution(
                        chunkContext.getStepContext().getStepExecution().getJobExecution());
                log.info("Job stop signal " + jobStopSignal);
                if (jobStopSignal != null && jobStopSignal.equals(BatchStatus.STOPPING)) {
                    throw new StepInterruptException();
                }


                Thread.sleep(2000);
                String firstName = person.getFirstName().toUpperCase();
                String lastName = person.getLastName().toUpperCase();

                Person transformedPerson = new Person(firstName, lastName);

                log.info("Converting (" + person + ") into (" + transformedPerson + ")");

                log.info("writing " + person.toString());
            }
            // ... your code
            flatFileItemReader.close();

            log.info("MyTaskOne done..");
            return RepeatStatus.FINISHED;
        } catch(StepInterruptException ex) {
            contribution.getStepExecution().addFailureException(ex);
            log.info("exception added : " + ex + " : stepexecution id = " + contribution.getStepExecution().getId());
            return RepeatStatus.FINISHED;

        }
    }
}
