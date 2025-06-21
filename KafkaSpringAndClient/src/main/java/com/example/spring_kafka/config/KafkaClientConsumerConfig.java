package com.example.spring_kafka.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

public class KafkaClientConsumerConfig {

    public static KafkaConsumer<String, String> createConsumer(
            String bootstrapServers,
            String groupId,
            String autoOffsetReset,
            String maxPollRecords,
            String securityProtocol,
            String truststoreLocation,
            String truststorePassword,
            String keystoreLocation,
            String keystorePassword,
            String keyPassword
    ) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);

        if (securityProtocol != null && !securityProtocol.isEmpty()) {
            props.put("security.protocol", securityProtocol);
        }
        if (truststoreLocation != null && !truststoreLocation.isEmpty()) {
            props.put("ssl.truststore.location", truststoreLocation);
            props.put("ssl.truststore.password", truststorePassword);
        }
        if (keystoreLocation != null && !keystoreLocation.isEmpty()) {
            props.put("ssl.keystore.location", keystoreLocation);
            props.put("ssl.keystore.password", keystorePassword);
            props.put("ssl.key.password", keyPassword);
        }

        return new KafkaConsumer<>(props);
    }
}