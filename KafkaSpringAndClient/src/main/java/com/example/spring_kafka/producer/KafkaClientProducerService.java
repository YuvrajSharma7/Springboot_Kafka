package com.example.spring_kafka.producer;

import com.example.spring_kafka.config.KafkaClientProducerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.logging.Logger;

@Component
public class KafkaClientProducerService {

    private static final Logger logger = Logger.getLogger(KafkaClientProducerService.class.getName());

    @Autowired
    private KafkaClientProducerConfig kafkaClientProducerConfig;

    public boolean publishToTopic(String topicName, String key, String message) {

        KafkaProducer kafkaProducer=kafkaClientProducerConfig.getProducerInstance();
        ProducerRecord producerRecord=new ProducerRecord<>(topicName,key,message);

        try {
            kafkaProducer.send(producerRecord, (recordMetadata, exception) -> {
                if (null != exception ) {
                    logger.severe("Error sending message: " + exception.getMessage());
                } else {
                    logger.info("Message sent successfully to topic: " + recordMetadata.topic() + " partition: " + recordMetadata.partition() + " offset: " + recordMetadata.offset());
                }
            });
            return true;
        } catch (Exception e) {
            logger.severe("Failed to send message: " + e.getMessage());
            return false;
        }
    }
}
