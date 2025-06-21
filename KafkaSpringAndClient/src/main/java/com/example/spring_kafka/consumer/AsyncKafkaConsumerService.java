package com.example.spring_kafka.consumer;

import com.example.spring_kafka.processor.AsyncRecordProcessor;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

// @todo: create AsyncKafkaClientConsumerService for kafka-client library(maven) consumers
@Service
@EnableScheduling
@Slf4j
public class AsyncKafkaConsumerService implements DisposableBean {

    private ExecutorService executorService;
    @Value("${numberOfThreads}")
    private String numberOfThreads;
    @Autowired
    AsyncRecordProcessor recordProcessor;
    @PostConstruct
    public void init() {
        executorService = Executors.newFixedThreadPool(Integer.parseInt(numberOfThreads));
    }

    @KafkaListener(
            topics = "${spring.kafka.consumer.source-topic}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    @KafkaHandler
    public void handleMessage(List<ConsumerRecord<String, String>> records, Acknowledgment acknowledgment) {

            executorService.submit(() -> {
                acknowledgment.acknowledge();
                // Simulate processing
                log.info("Received {} records", records.size());
                recordProcessor.processRecord(records);
            });

    }

    // to do add error handling and retry logic using @Retryable annotation
    // also another listener for DLQ topic should be implemented to process dlq records

    @Scheduled(cron = "* /1 * * * *")
    public void logMetrices() {
        MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
        MemoryUsage heapMemoryUsage = memoryMXBean.getHeapMemoryUsage();

        long maxMemory = heapMemoryUsage.getMax();
        long usedMemory = heapMemoryUsage.getUsed();
        long remainingMemory = maxMemory - usedMemory;

        log.debug("Max memory: " + maxMemory + " bytes");
        log.debug("Heap memory used: " + usedMemory + " bytes");
        log.debug("Remaining memory: " + remainingMemory + " bytes");
        log.debug("Records processed: " + AsyncRecordProcessor.recordsProcessed.getAndSet(0));

        /**
         * recordsProcessed.getAndSet(0) atomically retrieves the current value of recordsProcessed and then resets it to 0.
         * This is useful for periodic metrics logging: it shows how many records were processed since the last log,
         * and resets the counter for the next interval. This way, each log reflects only the records processed in
         * that specific period, not a running total.
         */
    }

    @Override
    public void destroy() {
        if (executorService != null) {
            executorService.shutdown();
        }
    }
}
