package com.example.springBatchJob;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemWriter;

import java.util.List;

public class PrintlnWriter implements ItemWriter<Person> {
    private static final Logger log = LoggerFactory.getLogger(PrintlnWriter.class);

    @Override
    public void write(List<? extends Person> list) throws Exception {
        log.info("Performing printing job writing. chunk size = " + list.size());
        Thread.sleep(3000);
        for (Person p : list) {
            log.info(p.toString());
        }
    }
}
