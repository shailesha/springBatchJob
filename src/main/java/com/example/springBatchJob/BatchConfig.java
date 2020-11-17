package com.example.springBatchJob;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.support.JobRegistryBeanPostProcessor;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobOperator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

import javax.sql.DataSource;

@Configuration
@EnableBatchProcessing
public class BatchConfig {

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Autowired
    public JobRepository jobRepository;

    @Autowired
    public JobExplorer jobExplorer;

    @Autowired
    StepBuilderFactory steps;


    @Bean
    public Step stepOne(){
        return steps.get("stepOne")
                .tasklet(new MyTaskOne())
                .startLimit(3)
                .build();
    }

    /*@Bean
    public Step stepTwo(){
        return steps.get("stepTwo")
                .tasklet(new MyTaskTwo())
                .build();
    }*/

    @Bean
    public Job taskletJob(){
        return jobBuilderFactory.get("taskletJob")
                .incrementer(new RunIdIncrementer())
                .start(stepOne())
                //.next(stepTwo())
                .build();
    }

    @Bean
    public FlatFileItemReader reader() {
        return new FlatFileItemReaderBuilder<Person>()
                .name("personItemReader")
                .resource(new ClassPathResource("sampledata.csv"))
                .delimited()
                .names(new String[]{"firstName", "lastName"})
                .fieldSetMapper(new BeanWrapperFieldSetMapper() {{
                    setTargetType(Person.class);
                }})
                .build();
    }


    @Bean
    public PersonItemProcessor processor() {
        return new PersonItemProcessor();
    }

    @Bean
    public JdbcBatchItemWriter<Person> writer(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Person>()
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .sql("INSERT INTO people (first_name, last_name) VALUES (:firstName, :lastName)")
                .dataSource(dataSource)
                .build();
    }

    @Bean
    @Qualifier("importUserJob")
    public Job importUserJob(JobCompletionNotificationListener listener,  @Qualifier("importUserJobStep1") Step importUserJobStep1) {
        return jobBuilderFactory.get("importUserJob")
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .flow(importUserJobStep1)
                .end()
                .build();
    }

    @Bean
    @Qualifier("importUserJobStep1")
    public Step importUserJobStep1(JdbcBatchItemWriter<Person> writer) {
        return stepBuilderFactory.get("importUserJobStep1")
                .<Person, Person> chunk(3)
                .reader(reader())
                .processor(processor())
                .writer(writer)
                .build();
    }


    @Bean
    @Qualifier("printlnJob")
    public Job printlnJob(JobCompletionNotificationListener listener, @Qualifier("printlnJobStep1") Step printlnJobStep1) {
        return jobBuilderFactory.get("printlnJob")
                .incrementer(new RunIdIncrementer())
               // .listener(listener)
                .flow(printlnJobStep1)
                .end()
                .build();
    }

    @Bean
    @Qualifier("printlnJobStep1")
    public Step printlnJobStep1() {
        return stepBuilderFactory.get("printlnJobStep1")
                .<Person, Person> chunk(2)
               .reader(reader())
                .processor(processor())
               .writer(printlnWriter())
                .build();
    }

    @Bean
    public PrintlnWriter printlnWriter() {
        return new PrintlnWriter();
    }

    @Bean
    @Qualifier("asyncJobLauncher")
    public JobLauncher simpleJobLauncher() throws Exception {
        SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
        jobLauncher.setJobRepository(jobRepository);
        jobLauncher.setTaskExecutor(new SimpleAsyncTaskExecutor());
        jobLauncher.afterPropertiesSet();
        return jobLauncher;
    }
    @Bean
    public JobOperator jobOperator(JobRegistry jobRegistry) throws Exception{
        final SimpleJobOperator jobOperator = new SimpleJobOperator();
        jobOperator.setJobLauncher(simpleJobLauncher());
        jobOperator.setJobRepository(jobRepository);
        jobOperator.setJobRegistry(jobRegistry);
        jobOperator.setJobExplorer(jobExplorer);
        return jobOperator;
    }
    @Bean
    public JobRegistryBeanPostProcessor jobRegistryBeanPostProcessor(JobRegistry jobRegistry) {
        JobRegistryBeanPostProcessor jobRegistryBeanPostProcessor = new JobRegistryBeanPostProcessor();
        jobRegistryBeanPostProcessor.setJobRegistry(jobRegistry);
        return jobRegistryBeanPostProcessor;
    }

}
