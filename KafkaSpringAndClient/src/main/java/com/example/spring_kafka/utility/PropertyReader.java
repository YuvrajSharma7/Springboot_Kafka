package com.example.spring_kafka.utility;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Utility class to read properties from application.properties file.
 * To read application properties without using Spring's @Value,
 * load the properties file manually using Properties and InputStream.
 */
public class PropertyReader {
    private final Properties properties = new Properties();

    public PropertyReader() {
        try (InputStream input = getClass().getClassLoader().getResourceAsStream("application.properties")) {
            if (input != null) {
                properties.load(input);
            } else {
                throw new RuntimeException("application.properties not found");
            }
        } catch (IOException ex) {
            throw new RuntimeException("Failed to load properties", ex);
        }
    }

    public String get(String key) {
        return properties.getProperty(key);
    }
}
