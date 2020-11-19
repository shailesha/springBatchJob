package com.example.springBatchJob;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.listener.StepExecutionListenerSupport;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class TaskOneStepExecutionListener extends StepExecutionListenerSupport {

    private static final Logger log = LoggerFactory.getLogger(TaskOneStepExecutionListener.class);

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
      //  log.info("afterstep of TaskOneStepExecutionListener : " + " : stepexecution id = " + stepExecution.getId());
        List<Throwable> exceptions = stepExecution.getFailureExceptions();
        //log.info("exception size " + exceptions.size());
        for(Throwable ex : exceptions) {
          //  log.info("exception found " + ex.toString());
            if(ex instanceof StepInterruptException) {
            //    log.info("exception instance of StepInterruptException ");
                return new ExitStatus("FORCE STOPPED");
            }
        }
        return super.afterStep(stepExecution);


    }
}
