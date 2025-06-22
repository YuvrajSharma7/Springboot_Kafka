package com.example.spring_kafka.processor;

import com.example.spring_kafka.model.DlqMetaData;
import com.example.spring_kafka.model.Order;
import com.example.spring_kafka.producer.KafkaProducerService;
import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Here we have implemented retry logic for processing records using single DLQ topic but if
 * we have multiple microservices then we can have multiple DLQ topics for each service and
 * a common DLQ topic for all services.
 * Then we will have separate application called DLQ processor which will read from the common DLQ topic
 * and send the records to respective DLQ topics based on the service name(AppName) or any other identifier.
 */
@Slf4j
public class AsyncRecordProcessor {

    public static AtomicInteger recordsProcessed = new AtomicInteger(0);
    @Value("${kafka.topic.dlq}")
    private String dlqTopic;
    @Value("${spring.kafka.producer.topic}")
    private String outputTopic;
    @Autowired
    Gson gson;
    @Autowired
    KafkaProducerService kafkaProducerService;

    public void processRecord(List<ConsumerRecord<String, String>> records) {
        // if required we can transform the records to a different DTO using gson or any other library
        // then null check the records

        records.forEach(record -> {
            Order order = gson.fromJson(record.value(), Order.class);
            try {
                processSingleRecord(outputTopic,record.key(),order);
                // Acknowledge the records if using manual acknowledgment
                // acknowledgment.acknowledge();
                log.info("Processed {} records successfully", records.size());
                // Increment the counter for processed records
                recordsProcessed.incrementAndGet();
            } catch (Exception e) {
                log.error("Error processing records: ", e);
                // Handle the exception as needed, e.g., log it, send to a dead-letter queue, etc.
                DlqMetaData dlqMetaData = new DlqMetaData();
                dlqMetaData.setRetryCount((byte) 0);
                dlqMetaData.setAppName("appName");
                dlqMetaData.setInstanceId("instanceId");
                dlqMetaData.setOrderData(order);

                kafkaProducerService.sendMessage(dlqTopic,record.key(),gson.toJson(dlqMetaData));

            }
        });
    }

    public void processDlqRecord(List<ConsumerRecord<String, String>> records) {
        // if required we can transform the records to a different DTO using gson or any other library
        // then null check the records

        records.forEach(record -> {
            DlqMetaData dlqMetaData = gson.fromJson(record.value(), DlqMetaData.class);
            try {
                processSingleRecord(outputTopic,record.key(),dlqMetaData.getOrderData());
                // Acknowledge the records if using manual acknowledgment
                // acknowledgment.acknowledge();
                log.info("Processed {} dlq records successfully", records.size());
                // Increment the counter for processed records
                recordsProcessed.incrementAndGet();
            } catch (Exception e) {
                log.error("Error processing records: ", e);
                // Handle the exception as needed, e.g., log it, send to a dead-letter queue, etc.
                // Increment the retry count and send back to DLQ
                dlqMetaData.setRetryCount((byte) (dlqMetaData.getRetryCount()+1));
                kafkaProducerService.sendMessage(dlqTopic,record.key(),gson.toJson(dlqMetaData));

            }
        });
    }

    public void processSingleRecord(String topic,String key,Order orderData) {

        log.info("Processing record: key={}, value={}", key, orderData);
        kafkaProducerService.sendMessage(topic, key, gson.toJson(orderData));
    }
}
