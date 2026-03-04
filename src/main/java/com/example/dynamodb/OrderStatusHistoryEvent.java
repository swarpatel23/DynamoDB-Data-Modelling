package com.example.dynamodb;

import java.time.Instant;

public record OrderStatusHistoryEvent(
        String customerId,
        String orderId,
        OrderStatus oldStatus,
        OrderStatus newStatus,
        Instant changedAt,
        String changedBy,
        String reason,
        long previousVersion,
        long newVersion) {
}
