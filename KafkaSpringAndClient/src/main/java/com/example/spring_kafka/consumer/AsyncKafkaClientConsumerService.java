package com.example.spring_kafka.consumer;

import com.example.spring_kafka.model.Order;
import com.example.spring_kafka.processor.AsyncRecordProcessor;
import com.example.spring_kafka.utility.PropertyReader;
import com.google.gson.Gson;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class AsyncKafkaClientConsumerService implements AutoCloseable {

    private final KafkaConsumer<String, String> consumer;
    private final ExecutorService executorService;
    private final ExecutorService pollerService;
    private final AsyncRecordProcessor recordProcessor;
    private volatile boolean running = true;
    private final PropertyReader propertyReader = new PropertyReader();
    private final String sourceTopic;
    private final String outputTopic;
    private final String bootstrapServers;
    private final String consumerGroup;
    private final Gson gson;

    public AsyncKafkaClientConsumerService() {
        Properties props = new Properties();
        this.sourceTopic = propertyReader.get("topic.source");
        this.outputTopic = propertyReader.get("topic.destination");
        this.bootstrapServers = propertyReader.get("bootstrap-servers");
        this.consumerGroup = propertyReader.get("consumer.group");
        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(Arrays.asList(sourceTopic));
        int numberOfThreads = Integer.parseInt(propertyReader.get("numberOfThreads"));
        this.executorService = Executors.newFixedThreadPool(numberOfThreads);
        this.pollerService = Executors.newSingleThreadExecutor();
        this.recordProcessor = new AsyncRecordProcessor();
        this.gson = new Gson();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", consumerGroup);
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("auto.offset.reset", "earliest");
        startPolling();
    }

    private void startPolling() {
        pollerService.submit(() -> {
            while (running) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                if (!records.isEmpty()) {
                    executorService.submit(() -> {
                        for (ConsumerRecord<String, String> record : records) {
                            Order order = gson.fromJson(record.value(), Order.class);
                            recordProcessor.processSingleRecord(outputTopic, record.key(), order);
                        }
                        consumer.commitSync();
                    });
                }
            }
        });
    }

    @Override
    public void close() {
        running = false;
        pollerService.shutdown();
        executorService.shutdown();
        consumer.close();
    }
}
