package com.example.dynamodb;

import java.util.List;
import java.util.Map;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public record CustomerOrderPage(
        List<CustomerOrder> orders,
        Map<String, AttributeValue> lastEvaluatedKey) {
}
