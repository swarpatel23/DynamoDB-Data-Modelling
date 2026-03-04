package com.example.dynamodb;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.Put;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.Update;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

public class CommerceSingleTableRepository {
    // Base table primary key attributes.
    private static final String PK = "PK";
    private static final String SK = "SK";

    // Sparse GSI #1: order timeline for one customer.
    private static final String GSI1_PK = "GSI1PK";
    private static final String GSI1_SK = "GSI1SK";

    // Sparse GSI #2: orders by status across all customers.
    private static final String GSI2_PK = "GSI2PK";
    private static final String GSI2_SK = "GSI2SK";

    // Sparse GSI #3: orders by status for one customer.
    private static final String GSI3_PK = "GSI3PK";
    private static final String GSI3_SK = "GSI3SK";

    private static final String ENTITY_TYPE = "entityType";
    private static final String ENTITY_CUSTOMER = "CUSTOMER";
    private static final String ENTITY_ORDER = "ORDER";
    private static final String ENTITY_ORDER_ITEM = "ORDER_ITEM";
    private static final String ENTITY_ORDER_STATUS_EVENT = "ORDER_STATUS_EVENT";
    private static final String VERSION = "version";

    private static final String GSI_CUSTOMER_ORDERS = "gsi_customer_orders";
    private static final String GSI_STATUS_ORDERS = "gsi_status_orders";
    private static final String GSI_CUSTOMER_STATUS_ORDERS = "gsi_customer_status_orders";
    private static final String ORDER_STATUS_EVENT_PREFIX = "ORDER_STATUS_EVT#";

    private final DynamoDbClient dynamoDbClient;
    private final String tableName;

    public CommerceSingleTableRepository(DynamoDbClient dynamoDbClient, String tableName) {
        this.dynamoDbClient = dynamoDbClient;
        this.tableName = tableName;
    }

    public void createTableIfMissing() {
        try {
            dynamoDbClient.describeTable(DescribeTableRequest.builder().tableName(tableName).build());
            return;
        } catch (ResourceNotFoundException ignored) {
        }

        dynamoDbClient.createTable(CreateTableRequest.builder()
                .tableName(tableName)
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .attributeDefinitions(
                        AttributeDefinition.builder().attributeName(PK).attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName(SK).attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName(GSI1_PK).attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName(GSI1_SK).attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName(GSI2_PK).attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName(GSI2_SK).attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName(GSI3_PK).attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName(GSI3_SK).attributeType(ScalarAttributeType.S).build())
                .keySchema(
                        KeySchemaElement.builder().attributeName(PK).keyType(KeyType.HASH).build(),
                        KeySchemaElement.builder().attributeName(SK).keyType(KeyType.RANGE).build())
                .globalSecondaryIndexes(
                        GlobalSecondaryIndex.builder()
                                .indexName(GSI_CUSTOMER_ORDERS)
                                .keySchema(
                                        KeySchemaElement.builder().attributeName(GSI1_PK).keyType(KeyType.HASH).build(),
                                        KeySchemaElement.builder().attributeName(GSI1_SK).keyType(KeyType.RANGE).build())
                                .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
                                .build(),
                        GlobalSecondaryIndex.builder()
                                .indexName(GSI_STATUS_ORDERS)
                                .keySchema(
                                        KeySchemaElement.builder().attributeName(GSI2_PK).keyType(KeyType.HASH).build(),
                                        KeySchemaElement.builder().attributeName(GSI2_SK).keyType(KeyType.RANGE).build())
                                .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
                                .build(),
                        GlobalSecondaryIndex.builder()
                                .indexName(GSI_CUSTOMER_STATUS_ORDERS)
                                .keySchema(
                                        KeySchemaElement.builder().attributeName(GSI3_PK).keyType(KeyType.HASH).build(),
                                        KeySchemaElement.builder().attributeName(GSI3_SK).keyType(KeyType.RANGE).build())
                                .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
                                .build())
                .build());
    }

    public void putCustomer(CustomerProfile customer) {
        putItem(customerItem(customer));
    }

    // Idempotent create helper: returns false if the customer row already exists.
    public boolean putCustomerIfAbsent(CustomerProfile customer) {
        try {
            dynamoDbClient.putItem(PutItemRequest.builder()
                    .tableName(tableName)
                    .item(customerItem(customer))
                    .conditionExpression("attribute_not_exists(PK) AND attribute_not_exists(SK)")
                    .build());
            return true;
        } catch (ConditionalCheckFailedException ignored) {
            return false;
        }
    }

    public void putOrder(CustomerOrder order) {
        putItem(orderRootItem(order));
    }

    public void putOrderItem(String customerId, OrderLineItem lineItem) {
        putItem(orderLineItem(customerId, lineItem));
    }

    // Atomic write for one order root + N order items.
    // Returns false if any row already exists and the transaction is canceled.
    public boolean createOrderWithItemsAtomic(CustomerOrder order, List<OrderLineItem> lineItems) {
        List<TransactWriteItem> writes = new ArrayList<>();
        writes.add(TransactWriteItem.builder()
                .put(Put.builder()
                        .tableName(tableName)
                        .item(orderRootItem(order))
                        .conditionExpression("attribute_not_exists(PK) AND attribute_not_exists(SK)")
                        .build())
                .build());

        for (OrderLineItem lineItem : lineItems) {
            writes.add(TransactWriteItem.builder()
                    .put(Put.builder()
                            .tableName(tableName)
                            .item(orderLineItem(order.customerId(), lineItem))
                            .conditionExpression("attribute_not_exists(PK) AND attribute_not_exists(SK)")
                            .build())
                    .build());
        }

        try {
            dynamoDbClient.transactWriteItems(TransactWriteItemsRequest.builder()
                    .transactItems(writes)
                    .build());
            return true;
        } catch (TransactionCanceledException ignored) {
            return false;
        }
    }

    public Optional<CustomerProfile> getCustomerProfile(String customerId) {
        Map<String, AttributeValue> item = getItem(customerPk(customerId), profileSk(customerId));
        if (item == null || item.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(toCustomerProfile(item));
    }

    public Optional<CustomerOrder> getOrderHeader(String customerId, String orderId) {
        Map<String, AttributeValue> item = getItem(customerPk(customerId), orderSk(orderId));
        if (item == null || item.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(toOrder(item));
    }

    public Optional<OrderLineItem> getOrderItem(String customerId, String orderId, String itemId) {
        Map<String, AttributeValue> item = getItem(customerPk(customerId), orderItemSk(orderId, itemId));
        if (item == null || item.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(toOrderLineItem(item));
    }

    public void deleteOrderItem(String customerId, String orderId, String itemId) {
        Map<String, AttributeValue> key = Map.of(
                PK, s(customerPk(customerId)),
                SK, s(orderItemSk(orderId, itemId)));

        dynamoDbClient.deleteItem(DeleteItemRequest.builder()
                .tableName(tableName)
                .key(key)
                .build());
    }

    public List<OrderLineItem> listOrderItems(String customerId, String orderId) {
        QueryRequest query = QueryRequest.builder()
                .tableName(tableName)
                .keyConditionExpression("PK = :pk AND begins_with(SK, :skPrefix)")
                .expressionAttributeValues(Map.of(
                        ":pk", s(customerPk(customerId)),
                        ":skPrefix", s(orderItemPrefix(orderId))))
                .build();

        List<OrderLineItem> items = new ArrayList<>();
        for (Map<String, AttributeValue> item : dynamoDbClient.query(query).items()) {
            items.add(toOrderLineItem(item));
        }
        return items;
    }

    // One query returns ORDER root + ORDER_ITEM children for this customer+order.
    public Optional<OrderAggregate> getOrderWithItems(String customerId, String orderId) {
        QueryRequest query = QueryRequest.builder()
                .tableName(tableName)
                .keyConditionExpression("PK = :pk AND begins_with(SK, :skPrefix)")
                .expressionAttributeValues(Map.of(
                        ":pk", s(customerPk(customerId)),
                        ":skPrefix", s(orderPrefix(orderId))))
                .build();

        List<Map<String, AttributeValue>> items = dynamoDbClient.query(query).items();
        if (items == null || items.isEmpty()) {
            return Optional.empty();
        }

        CustomerOrder order = null;
        List<OrderLineItem> orderItems = new ArrayList<>();

        for (Map<String, AttributeValue> item : items) {
            String entityType = item.get(ENTITY_TYPE).s();
            if (ENTITY_ORDER.equals(entityType)) {
                order = toOrder(item);
            } else if (ENTITY_ORDER_ITEM.equals(entityType)) {
                orderItems.add(toOrderLineItem(item));
            }
        }

        if (order == null) {
            return Optional.empty();
        }

        return Optional.of(new OrderAggregate(order, orderItems));
    }

    // Efficient customer timeline query: uses sparse GSI1 (ORDER entities only).
    public List<CustomerOrder> listOrdersForCustomerNewestFirst(String customerId, int limit) {
        return listOrdersForCustomerNewestFirstPage(customerId, limit, Map.of()).orders();
    }

    // Same customer timeline query, but exposes LastEvaluatedKey for pagination.
    public CustomerOrderPage listOrdersForCustomerNewestFirstPage(
            String customerId,
            int limit,
            Map<String, AttributeValue> exclusiveStartKey) {
        QueryRequest.Builder queryBuilder = QueryRequest.builder()
                .tableName(tableName)
                .indexName(GSI_CUSTOMER_ORDERS)
                .keyConditionExpression("GSI1PK = :pk")
                .expressionAttributeValues(Map.of(":pk", s(customerPk(customerId))))
                .scanIndexForward(false)
                .limit(limit);

        if (exclusiveStartKey != null && !exclusiveStartKey.isEmpty()) {
            queryBuilder.exclusiveStartKey(exclusiveStartKey);
        }

        QueryResponse response = dynamoDbClient.query(queryBuilder.build());
        List<CustomerOrder> orders = new ArrayList<>();
        for (Map<String, AttributeValue> item : response.items()) {
            orders.add(toOrder(item));
        }
        Map<String, AttributeValue> lek = response.lastEvaluatedKey();
        return new CustomerOrderPage(orders, lek == null ? Map.of() : lek);
    }

    // Efficient cross-customer status query: uses sparse GSI2 (ORDER entities only).
    public List<CustomerOrder> listOrdersByStatusNewestFirst(OrderStatus status, int limit) {
        QueryRequest query = QueryRequest.builder()
                .tableName(tableName)
                .indexName(GSI_STATUS_ORDERS)
                .keyConditionExpression("GSI2PK = :pk")
                .expressionAttributeValues(Map.of(":pk", s(statusPk(status))))
                .scanIndexForward(false)
                .limit(limit)
                .build();

        List<CustomerOrder> orders = new ArrayList<>();
        for (Map<String, AttributeValue> item : dynamoDbClient.query(query).items()) {
            orders.add(toOrder(item));
        }
        return orders;
    }

    // Efficient query for one customer's orders with a specific status.
    public List<CustomerOrder> listOrdersForCustomerByStatusNewestFirst(String customerId, OrderStatus status, int limit) {
        QueryRequest query = QueryRequest.builder()
                .tableName(tableName)
                .indexName(GSI_CUSTOMER_STATUS_ORDERS)
                .keyConditionExpression("GSI3PK = :pk")
                .expressionAttributeValues(Map.of(":pk", s(customerStatusPk(customerId, status))))
                .scanIndexForward(false)
                .limit(limit)
                .build();

        List<CustomerOrder> orders = new ArrayList<>();
        for (Map<String, AttributeValue> item : dynamoDbClient.query(query).items()) {
            orders.add(toOrder(item));
        }
        return orders;
    }

    // Updates order status and rewrites GSI keys so index queries move to the new status bucket.
    public void updateOrderStatus(String customerId, String orderId, OrderStatus newStatus) {
        long expectedVersion = getOrderVersion(customerId, orderId);
        boolean updated = updateOrderStatusWithHistory(
                customerId,
                orderId,
                newStatus,
                expectedVersion,
                "system",
                "default-status-update");
        if (!updated) {
            throw new IllegalStateException("Optimistic lock conflict while updating order status for " + customerId + "/" + orderId);
        }
    }

    // Atomic status change with optimistic lock + immutable status-history event append.
    public boolean updateOrderStatusWithHistory(
            String customerId,
            String orderId,
            OrderStatus newStatus,
            long expectedVersion,
            String changedBy,
            String reason) {
        String customerPk = customerPk(customerId);
        String orderSk = orderSk(orderId);
        Map<String, AttributeValue> currentOrder = loadOrderItem(customerPk, orderSk);
        String createdAt = currentOrder.get("createdAt").s();
        OrderStatus oldStatus = OrderStatus.valueOf(currentOrder.get("status").s());
        long nextVersion = expectedVersion + 1;
        String changedAt = Instant.now().toString();

        Map<String, AttributeValue> key = Map.of(
                PK, s(customerPk),
                SK, s(orderSk));

        Map<String, AttributeValue> updateValues = Map.of(
                ":status", s(newStatus.name()),
                ":gsi2pk", s(statusPk(newStatus)),
                ":gsi2sk", s("ORDER#" + createdAt + "#" + customerPk + "#" + orderId),
                ":gsi3pk", s(customerStatusPk(customerId, newStatus)),
                ":gsi3sk", s("ORDER#" + createdAt + "#" + orderId),
                ":expectedVersion", n(expectedVersion),
                ":nextVersion", n(nextVersion));

        Update updateOrder = Update.builder()
                .tableName(tableName)
                .key(key)
                .updateExpression(
                        "SET #status = :status, GSI2PK = :gsi2pk, GSI2SK = :gsi2sk, GSI3PK = :gsi3pk, GSI3SK = :gsi3sk, #version = :nextVersion")
                .conditionExpression("#version = :expectedVersion")
                .expressionAttributeNames(Map.of(
                        "#status", "status",
                        "#version", VERSION))
                .expressionAttributeValues(updateValues)
                .build();

        Put putHistoryEvent = Put.builder()
                .tableName(tableName)
                .item(orderStatusEventItem(
                        customerId,
                        orderId,
                        oldStatus,
                        newStatus,
                        changedAt,
                        changedBy,
                        reason,
                        expectedVersion,
                        nextVersion))
                .conditionExpression("attribute_not_exists(PK) AND attribute_not_exists(SK)")
                .build();

        try {
            dynamoDbClient.transactWriteItems(TransactWriteItemsRequest.builder()
                    .transactItems(
                            TransactWriteItem.builder().update(updateOrder).build(),
                            TransactWriteItem.builder().put(putHistoryEvent).build())
                    .build());
            return true;
        } catch (TransactionCanceledException ignored) {
            return false;
        }
    }

    public long getOrderVersion(String customerId, String orderId) {
        Map<String, AttributeValue> item = loadOrderItem(customerPk(customerId), orderSk(orderId));
        return parseVersion(item);
    }

    public List<OrderStatusHistoryEvent> listOrderStatusHistoryNewestFirst(String customerId, String orderId, int limit) {
        QueryRequest query = QueryRequest.builder()
                .tableName(tableName)
                .keyConditionExpression("PK = :pk AND begins_with(SK, :skPrefix)")
                .expressionAttributeValues(Map.of(
                        ":pk", s(customerPk(customerId)),
                        ":skPrefix", s(orderStatusEventPrefix(orderId))))
                .scanIndexForward(false)
                .limit(limit)
                .build();

        List<OrderStatusHistoryEvent> events = new ArrayList<>();
        for (Map<String, AttributeValue> item : dynamoDbClient.query(query).items()) {
            events.add(toOrderStatusHistoryEvent(item));
        }
        return events;
    }

    public void updateOrderItemStatus(String customerId, String orderId, String itemId, OrderItemStatus newStatus) {
        Map<String, AttributeValue> key = Map.of(
                PK, s(customerPk(customerId)),
                SK, s(orderItemSk(orderId, itemId)));

        dynamoDbClient.updateItem(UpdateItemRequest.builder()
                .tableName(tableName)
                .key(key)
                .updateExpression("SET itemStatus = :status")
                .expressionAttributeValues(Map.of(":status", s(newStatus.name())))
                .build());
    }

    private void putItem(Map<String, AttributeValue> item) {
        dynamoDbClient.putItem(PutItemRequest.builder()
                .tableName(tableName)
                .item(item)
                .build());
    }

    private static CustomerOrder toOrder(Map<String, AttributeValue> item) {
        return new CustomerOrder(
                item.get("customerId").s(),
                item.get("orderId").s(),
                Instant.parse(item.get("createdAt").s()),
                OrderStatus.valueOf(item.get("status").s()),
                Long.parseLong(item.get("totalCents").n()));
    }

    private static CustomerProfile toCustomerProfile(Map<String, AttributeValue> item) {
        return new CustomerProfile(
                item.get("customerId").s(),
                item.get("name").s(),
                item.get("email").s());
    }

    private static OrderLineItem toOrderLineItem(Map<String, AttributeValue> item) {
        return new OrderLineItem(
                item.get("orderId").s(),
                item.get("itemId").s(),
                item.get("sku").s(),
                Integer.parseInt(item.get("quantity").n()),
                Long.parseLong(item.get("unitPriceCents").n()),
                item.containsKey("itemStatus")
                        ? OrderItemStatus.valueOf(item.get("itemStatus").s())
                        : OrderItemStatus.PENDING);
    }

    private static OrderStatusHistoryEvent toOrderStatusHistoryEvent(Map<String, AttributeValue> item) {
        return new OrderStatusHistoryEvent(
                item.get("customerId").s(),
                item.get("orderId").s(),
                OrderStatus.valueOf(item.get("oldStatus").s()),
                OrderStatus.valueOf(item.get("newStatus").s()),
                Instant.parse(item.get("changedAt").s()),
                item.get("changedBy").s(),
                item.get("reason").s(),
                Long.parseLong(item.get("previousVersion").n()),
                Long.parseLong(item.get("newVersion").n()));
    }

    private static String customerPk(String customerId) {
        return "CUST#" + customerId;
    }

    private static String profileSk(String customerId) {
        return "PROFILE#" + customerId;
    }

    private static String orderSk(String orderId) {
        return "ORDER#" + orderId;
    }

    private static String orderPrefix(String orderId) {
        return "ORDER#" + orderId;
    }

    private static String orderItemPrefix(String orderId) {
        return "ORDER#" + orderId + "#ITEM#";
    }

    private static String orderStatusEventPrefix(String orderId) {
        return ORDER_STATUS_EVENT_PREFIX + orderId + "#";
    }

    private static String orderItemSk(String orderId, String itemId) {
        return "ORDER#" + orderId + "#ITEM#" + itemId;
    }

    private static String orderStatusEventSk(String orderId, long newVersion) {
        return ORDER_STATUS_EVENT_PREFIX + orderId + "#V#" + formatVersionForSk(newVersion);
    }

    private static String formatVersionForSk(long version) {
        // Zero-pad to preserve numeric order in lexicographic sort-key ordering.
        return String.format("%019d", version);
    }

    private static String statusPk(OrderStatus status) {
        return "STATUS#" + status.name();
    }

    private static String customerStatusPk(String customerId, OrderStatus status) {
        return "CUST#" + customerId + "#STATUS#" + status.name();
    }

    private Map<String, AttributeValue> loadOrderItem(String customerPk, String orderSk) {
        Map<String, AttributeValue> item = getItem(customerPk, orderSk);
        if (item == null || item.isEmpty() || !item.containsKey("createdAt")) {
            throw new IllegalStateException("Order not found for status update: " + customerPk + "/" + orderSk);
        }
        return item;
    }

    private static long parseVersion(Map<String, AttributeValue> item) {
        if (item.containsKey(VERSION)) {
            return Long.parseLong(item.get(VERSION).n());
        }
        // Backward compatibility for legacy rows written before versioning.
        return 1L;
    }

    private Map<String, AttributeValue> getItem(String pk, String sk) {
        Map<String, AttributeValue> key = Map.of(
                PK, s(pk),
                SK, s(sk));
        return dynamoDbClient.getItem(GetItemRequest.builder()
                .tableName(tableName)
                .key(key)
                .build()).item();
    }

    private static Map<String, AttributeValue> customerItem(CustomerProfile customer) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put(PK, s(customerPk(customer.customerId())));
        item.put(SK, s(profileSk(customer.customerId())));
        item.put(ENTITY_TYPE, s(ENTITY_CUSTOMER));
        item.put("customerId", s(customer.customerId()));
        item.put("name", s(customer.name()));
        item.put("email", s(customer.email()));
        return item;
    }

    private static Map<String, AttributeValue> orderRootItem(CustomerOrder order) {
        String createdAt = order.createdAt().toString();
        String customerPk = customerPk(order.customerId());

        Map<String, AttributeValue> item = new HashMap<>();
        item.put(PK, s(customerPk));
        item.put(SK, s(orderSk(order.orderId())));
        item.put(ENTITY_TYPE, s(ENTITY_ORDER));
        item.put("customerId", s(order.customerId()));
        item.put("orderId", s(order.orderId()));
        item.put("createdAt", s(createdAt));
        item.put("status", s(order.status().name()));
        item.put(VERSION, n(1));
        item.put("totalCents", n(order.totalCents()));

        // GSI #1 stores only ORDER items; this creates a customer-specific timeline.
        item.put(GSI1_PK, s(customerPk));
        item.put(GSI1_SK, s("ORDER#" + createdAt + "#" + order.orderId()));

        // GSI #2 stores only ORDER items; this enables "all fulfilled/all paid orders" queries.
        item.put(GSI2_PK, s(statusPk(order.status())));
        item.put(GSI2_SK, s("ORDER#" + createdAt + "#" + customerPk + "#" + order.orderId()));

        // GSI #3 stores only ORDER items; this enables "orders by status for one customer".
        item.put(GSI3_PK, s(customerStatusPk(order.customerId(), order.status())));
        item.put(GSI3_SK, s("ORDER#" + createdAt + "#" + order.orderId()));
        return item;
    }

    private static Map<String, AttributeValue> orderLineItem(String customerId, OrderLineItem lineItem) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put(PK, s(customerPk(customerId)));
        item.put(SK, s(orderItemSk(lineItem.orderId(), lineItem.itemId())));
        item.put(ENTITY_TYPE, s(ENTITY_ORDER_ITEM));
        item.put("orderId", s(lineItem.orderId()));
        item.put("itemId", s(lineItem.itemId()));
        item.put("sku", s(lineItem.sku()));
        item.put("quantity", n(lineItem.quantity()));
        item.put("unitPriceCents", n(lineItem.unitPriceCents()));
        item.put("itemStatus", s(lineItem.itemStatus().name()));
        return item;
    }

    private static Map<String, AttributeValue> orderStatusEventItem(
            String customerId,
            String orderId,
            OrderStatus oldStatus,
            OrderStatus newStatus,
            String changedAt,
            String changedBy,
            String reason,
            long previousVersion,
            long newVersion) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put(PK, s(customerPk(customerId)));
        item.put(SK, s(orderStatusEventSk(orderId, newVersion)));
        item.put(ENTITY_TYPE, s(ENTITY_ORDER_STATUS_EVENT));
        item.put("customerId", s(customerId));
        item.put("orderId", s(orderId));
        item.put("oldStatus", s(oldStatus.name()));
        item.put("newStatus", s(newStatus.name()));
        item.put("changedAt", s(changedAt));
        item.put("changedBy", s(changedBy == null ? "unknown" : changedBy));
        item.put("reason", s(reason == null ? "" : reason));
        item.put("previousVersion", n(previousVersion));
        item.put("newVersion", n(newVersion));
        return item;
    }

    private static AttributeValue s(String value) {
        return AttributeValue.builder().s(value).build();
    }

    private static AttributeValue n(long value) {
        return AttributeValue.builder().n(Long.toString(value)).build();
    }
}
