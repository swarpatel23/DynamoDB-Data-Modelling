package com.example.dynamodb;

public record OrderLineItem(
        String orderId,
        String itemId,
        String sku,
        int quantity,
        long unitPriceCents,
        OrderItemStatus itemStatus) {
}
