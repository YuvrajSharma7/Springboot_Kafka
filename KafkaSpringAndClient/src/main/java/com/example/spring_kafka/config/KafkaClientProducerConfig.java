package com.example.spring_kafka.config;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Value;

import java.util.HashMap;
import java.util.Map;

/**
 * This config is based on <artifactId>kafka-clients</artifactId> dependency is
 */
public class KafkaClientProducerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.producer.key-serializer}")
    private String keySerializer;

    @Value("${spring.kafka.producer.value-serializer}")
    private String valueSerializer;

    @Value("${spring.kafka.properties.security.protocol:}")
    private String securityProtocol;

    @Value("${spring.kafka.properties.ssl.truststore.location:}")
    private String truststoreLocation;

    @Value("${spring.kafka.properties.ssl.truststore.password:}")
    private String truststorePassword;

    @Value("${spring.kafka.properties.ssl.keystore.location:}")
    private String keystoreLocation;

    @Value("${spring.kafka.properties.ssl.keystore.password:}")
    private String keystorePassword;

    @Value("${spring.kafka.properties.ssl.key.password:}")
    private String keyPassword;
    
    private KafkaProducer kafkaProducer=null;
    
    public KafkaProducer getProducerInstance() {
        if (null ==  kafkaProducer ) {
            return getKafkaProducer();
        }
        return kafkaProducer;
    }

    private KafkaProducer getKafkaProducer() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);
        configProps.put("use.latest.version",true);
        configProps.put("security.protocol", securityProtocol);
        configProps.put("ssl.truststore.location", truststoreLocation);
        configProps.put("ssl.truststore.password", truststorePassword);
        configProps.put("ssl.keystore.location", keystoreLocation);
        configProps.put("ssl.keystore.password", keystorePassword);
        configProps.put("ssl.key.password", keyPassword);

        kafkaProducer=new KafkaProducer<>(configProps);
        return kafkaProducer;
    }

}
