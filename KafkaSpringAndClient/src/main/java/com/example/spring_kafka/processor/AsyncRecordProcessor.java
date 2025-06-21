package com.example.spring_kafka.processor;

import com.example.spring_kafka.model.DlqMetaData;
import com.example.spring_kafka.model.Order;
import com.example.spring_kafka.producer.KafkaClientProducerService;
import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class AsyncRecordProcessor {

    public static AtomicInteger recordsProcessed = new AtomicInteger(0);
    @Value("${kafka.topic.dlq}")
    private String dlqTopic;
    @Autowired
    Gson gson;
    @Autowired
    KafkaClientProducerService kafkaClientProducerService;

    public void processRecord(List<ConsumerRecord<String, String>> records) {
        // if required we can transform the records to a different DTO using gson or any other library
        // then null check the records

        records.forEach(record -> {
            Order order = gson.fromJson(record.value(), Order.class);
            try {
                processSingleRecord(record);
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

                kafkaClientProducerService.publishToTopic(dlqTopic,record.key(),gson.toJson(dlqMetaData));

            }
        });
    }

    private void processSingleRecord(ConsumerRecord<String, String> record) {

        AsyncRecordProcessor.log.info("Processing record: key=" + record.key() + ", value=" + record.value());
    }
}
