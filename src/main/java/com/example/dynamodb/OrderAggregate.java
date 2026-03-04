package com.example.dynamodb;

import java.util.List;

public record OrderAggregate(CustomerOrder order, List<OrderLineItem> items) {
}
