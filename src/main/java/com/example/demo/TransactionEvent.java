package com.example.demo;

public record TransactionEvent(String transactionId, String accountId, double amount, String currency){}