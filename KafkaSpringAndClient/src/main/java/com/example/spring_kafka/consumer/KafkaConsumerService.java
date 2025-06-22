package com.example.spring_kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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

@Service
@EnableScheduling
@Slf4j
public class KafkaConsumerService {

    @KafkaListener(
            topics = "${spring.kafka.consumer.source-topic}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    @KafkaHandler
    public void handleMessage(List<ConsumerRecord<String, String>> records, Acknowledgment acknowledgment) {
        for (ConsumerRecord<String, String> record : records) {
            log.info("Received message: key={}, value={}", record.key(), record.value());
        }
        acknowledgment.acknowledge();
    }
    /**
     * Call acknowledgement.acknowledge() to commit offsets.
     * This is necessary to ensure that the messages are not reprocessed.
     * This setup enables batch processing and manual acknowledgment.
     * You can also use auto-acknowledgment by setting
     * spring.kafka.consumer.enable-auto-commit=true
     * but it is not recommended for production use
     * as it can lead to message loss or duplication.
     * You can also use the @SendTo annotation to send the processed messages to another topic.
     * This is useful for processing messages in a pipeline.
     * You can also use the @Retryable annotation to retry processing messages in case of failures.
     * This is useful for handling transient errors.
     * You can also use the @Transactional annotation to ensure that the processing of messages is atomic.
     * This is useful for ensuring that the processing of messages is consistent.
     * You can also use the @KafkaListener annotation to specify the topics to listen to.
     * You can also use the @KafkaHandler annotation to specify the method to handle the messages.
     * You can also use executor services to process messages asynchronously.
     * You can also use the @Scheduled annotation to schedule the processing of messages.
     *
     */

    @Scheduled(cron = "* /1 * * * *")
    public void logMetrices() {
        MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
        MemoryUsage heapMemoryUsage = memoryMXBean.getHeapMemoryUsage();

        long maxMemory = heapMemoryUsage.getMax();
        long usedMemory = heapMemoryUsage.getUsed();
        long remainingMemory = maxMemory - usedMemory;

        log.info("Max memory: {} bytes", maxMemory);
        log.info("Heap memory used: {} bytes", usedMemory);
        log.info("Remaining memory: {} bytes", remainingMemory);
    }



}
