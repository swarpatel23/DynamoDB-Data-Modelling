package com.example.dynamodb;

public enum OrderStatus {
    CREATED,              // order placed
    AWAITING_PAYMENT,     // optional for delayed payment flows
    PAID,                 // payment confirmed
    IN_FULFILLMENT,       // warehouse processing
    PARTIALLY_FULFILLED,  // some items fulfilled, some still pending
    FULFILLED,            // all shippable items fulfilled
    CANCELLED,
    REFUNDED              // optional if refunds are tracked at order level
}
