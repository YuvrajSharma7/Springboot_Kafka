package com.example.spring_kafka.producer;

import com.example.spring_kafka.config.KafkaProducerConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.apache.kafka.clients.producer.ProducerRecord;

@Service
public class KafkaProducerService {

    private final KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    public KafkaProducerService() {
        KafkaProducerConfig kafkaProducerConfig = new KafkaProducerConfig();
        this.kafkaTemplate = kafkaProducerConfig.kafkaTemplate();
    }

    public void sendMessage(String topicName, String key, String message) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, message);
        kafkaTemplate.send(record);
    }

}