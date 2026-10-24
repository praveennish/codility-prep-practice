package com.example.demo;

/*
* This serves as the data transfer object (DTO) representing our message body (the Kafka Value).
* Using a Java record automatically generates immutability (getters, equals, hashCode, and toString). 
* In streaming data architecture, records are ideal because once an event is emitted into an immutable log like Kafka, its state should never change inside the application layers.
*/
public record TransactionEvent(String transactionId, String accountId, double amount, String currency){}

