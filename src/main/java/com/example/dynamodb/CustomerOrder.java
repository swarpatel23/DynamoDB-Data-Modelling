package com.example.dynamodb;

import java.time.Instant;

public record CustomerOrder(
        String customerId,
        String orderId,
        Instant createdAt,
        OrderStatus status,
        long totalCents) {
}
