package com.example.spring_kafka.model;

import lombok.Data;

@Data
public class Order {
    private String orderId;
    private String customerName;
    private double amount;
}
