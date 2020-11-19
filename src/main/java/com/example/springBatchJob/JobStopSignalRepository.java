package com.example.springBatchJob;

import com.example.springBatchJob.web.JobController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;

import java.util.HashMap;
import java.util.Map;

public class JobStopSignalRepository {

    private static final Logger log = LoggerFactory.getLogger(JobStopSignalRepository.class);

    private static Map<Long, BatchStatus> map = new HashMap<Long, BatchStatus>();

    public static void signalStopToJobExecution(JobExecution exec) {

        map.put(exec.getId(),BatchStatus.STOPPING);

    }
    public static void resetSignalToJobExecution(JobExecution exec) {
        map.remove(exec.getId());
    }

    public static BatchStatus getStopSignalForJobEXecution(JobExecution exec) {

        return map.get(exec.getId());
    }
}
